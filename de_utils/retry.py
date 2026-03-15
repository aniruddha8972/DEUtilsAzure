"""
de_utils.retry
--------------
Retry / resilience utilities for ETL operations that interact with
external systems (ADLS, Hive metastore, JDBC sources) which can
experience transient failures.

Features
--------
  @retry           — decorator with exponential back-off + jitter
  @circuit_breaker — stops hammering a failing service after N failures
  RetryPolicy      — reusable policy object you can share across jobs
  with_retry()     — functional wrapper (no decorator needed)

Usage
-----
>>> from de_utils.retry import retry, RetryPolicy, CircuitBreaker

>>> # Simple decorator
>>> @retry(max_attempts=3, wait_seconds=2.0, backoff=2.0)
... def read_from_adls(path):
...     return conn.read_parquet(path)

>>> # Reusable policy
>>> policy = RetryPolicy(max_attempts=5, wait_seconds=1.0, backoff=2.0,
...                      max_wait_seconds=30.0, jitter=True)
>>> policy.execute(lambda: spark.table("silver.orders").count())

>>> # Circuit breaker — open after 3 failures, reset after 60s
>>> cb = CircuitBreaker(failure_threshold=3, recovery_timeout=60)
>>> @cb.protect
... def write_to_delta(df, path):
...     df.write.format("delta").save(path)
"""

from __future__ import annotations

import functools
import logging
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Callable, Optional, Tuple, Type, Union

from .utils import get_logger, DataEngineeringError

log = get_logger(__name__)


# ── Retryable exception groups ────────────────────────────────────────────────

# Exceptions we always retry (transient infrastructure issues)
_DEFAULT_RETRYABLE: Tuple[Type[Exception], ...] = (
    ConnectionError,
    TimeoutError,
    OSError,
)


def _is_retryable(exc: Exception) -> bool:
    """Return True for known-transient errors."""
    msg = str(exc).lower()
    transient_keywords = [
        "connection reset", "broken pipe", "timeout", "temporarily unavailable",
        "service unavailable", "too many requests", "throttl", "retriable",
        "metastore connection", "thrift connection", "network",
    ]
    if isinstance(exc, _DEFAULT_RETRYABLE):
        return True
    return any(kw in msg for kw in transient_keywords)


# ── RetryPolicy ───────────────────────────────────────────────────────────────

@dataclass
class RetryPolicy:
    """
    Reusable retry policy with exponential back-off and optional jitter.

    Parameters
    ----------
    max_attempts      : int   Total attempts (including the first try).
    wait_seconds      : float Initial wait between attempts.
    backoff           : float Multiplier applied to wait on each failure (>= 1).
    max_wait_seconds  : float Cap on wait time.
    jitter            : bool  Add ±25 % random jitter to avoid thundering herd.
    retryable_on      : tuple Extra exception types to retry on.
    """

    max_attempts:     int   = 3
    wait_seconds:     float = 1.0
    backoff:          float = 2.0
    max_wait_seconds: float = 60.0
    jitter:           bool  = True
    retryable_on:     Tuple[Type[Exception], ...] = field(default_factory=tuple)

    def _should_retry(self, exc: Exception) -> bool:
        if self.retryable_on and isinstance(exc, self.retryable_on):
            return True
        return _is_retryable(exc)

    def _wait(self, attempt: int) -> float:
        delay = self.wait_seconds * (self.backoff ** attempt)
        delay = min(delay, self.max_wait_seconds)
        if self.jitter:
            delay *= random.uniform(0.75, 1.25)
        return delay

    def execute(self, fn: Callable, *args, **kwargs):
        """Execute *fn* with the retry policy applied."""
        last_exc: Optional[Exception] = None
        for attempt in range(self.max_attempts):
            try:
                return fn(*args, **kwargs)
            except Exception as exc:
                last_exc = exc
                if attempt + 1 >= self.max_attempts or not self._should_retry(exc):
                    raise
                wait = self._wait(attempt)
                log.warning(
                    "Attempt %d/%d failed: %s — retrying in %.1fs",
                    attempt + 1, self.max_attempts, exc, wait,
                )
                time.sleep(wait)
        raise last_exc  # type: ignore[misc]


# ── @retry decorator ──────────────────────────────────────────────────────────

def retry(
    max_attempts: int = 3,
    wait_seconds: float = 1.0,
    backoff: float = 2.0,
    max_wait_seconds: float = 60.0,
    jitter: bool = True,
    retryable_on: Tuple[Type[Exception], ...] = (),
):
    """
    Decorator that retries a function with exponential back-off.

    Example
    -------
    >>> @retry(max_attempts=4, wait_seconds=2, backoff=2)
    ... def unstable_read():
    ...     return conn.read_parquet("s3://...")
    """
    policy = RetryPolicy(
        max_attempts=max_attempts,
        wait_seconds=wait_seconds,
        backoff=backoff,
        max_wait_seconds=max_wait_seconds,
        jitter=jitter,
        retryable_on=retryable_on,
    )

    def decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            return policy.execute(fn, *args, **kwargs)
        return wrapper
    return decorator


def with_retry(fn: Callable, policy: Optional[RetryPolicy] = None, *args, **kwargs):
    """
    Functional (non-decorator) retry wrapper.

    Example
    -------
    >>> result = with_retry(lambda: spark.table("silver.orders").count(),
    ...                     RetryPolicy(max_attempts=3))
    """
    p = policy or RetryPolicy()
    return p.execute(fn, *args, **kwargs)


# ── Circuit Breaker ───────────────────────────────────────────────────────────

class CircuitState(str, Enum):
    CLOSED   = "CLOSED"    # Normal — requests pass through
    OPEN     = "OPEN"      # Tripped — requests fail immediately
    HALF_OPEN= "HALF_OPEN" # Testing — one probe request allowed


class CircuitBreakerOpenError(DataEngineeringError):
    """Raised when a call is blocked by an open circuit breaker."""


class CircuitBreaker:
    """
    Circuit breaker that stops calling a failing service after
    *failure_threshold* consecutive failures.

    After *recovery_timeout* seconds, one probe request is allowed
    (HALF_OPEN state). If it succeeds, the circuit resets to CLOSED.
    If it fails, the circuit stays OPEN.

    Example
    -------
    >>> cb = CircuitBreaker(failure_threshold=3, recovery_timeout=60)
    >>>
    >>> @cb.protect
    ... def write_to_adls(df, path):
    ...     conn.write_delta(df, path)
    >>>
    >>> # Check state
    >>> cb.state    # CircuitState.CLOSED / OPEN / HALF_OPEN
    >>> cb.reset()  # manually reset to CLOSED
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout:  int = 60,
        name:              str = "default",
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout  = recovery_timeout
        self.name              = name
        self._state            = CircuitState.CLOSED
        self._failure_count    = 0
        self._last_failure_at: Optional[datetime] = None

    @property
    def state(self) -> CircuitState:
        if self._state == CircuitState.OPEN:
            if (
                self._last_failure_at
                and datetime.utcnow() - self._last_failure_at
                    >= timedelta(seconds=self.recovery_timeout)
            ):
                log.info("Circuit '%s' → HALF_OPEN (probe allowed)", self.name)
                self._state = CircuitState.HALF_OPEN
        return self._state

    def _on_success(self) -> None:
        self._failure_count = 0
        if self._state != CircuitState.CLOSED:
            log.info("Circuit '%s' → CLOSED (recovered)", self.name)
        self._state = CircuitState.CLOSED

    def _on_failure(self, exc: Exception) -> None:
        self._failure_count += 1
        self._last_failure_at = datetime.utcnow()
        if self._failure_count >= self.failure_threshold or self._state == CircuitState.HALF_OPEN:
            self._state = CircuitState.OPEN
            log.error(
                "Circuit '%s' → OPEN after %d failures (last: %s)",
                self.name, self._failure_count, exc,
            )

    def call(self, fn: Callable, *args, **kwargs):
        """Call *fn* guarded by this circuit breaker."""
        if self.state == CircuitState.OPEN:
            raise CircuitBreakerOpenError(
                f"Circuit '{self.name}' is OPEN — service is unavailable. "
                f"Will retry after {self.recovery_timeout}s."
            )
        try:
            result = fn(*args, **kwargs)
            self._on_success()
            return result
        except Exception as exc:
            self._on_failure(exc)
            raise

    def protect(self, fn: Callable) -> Callable:
        """Decorator form of the circuit breaker."""
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            return self.call(fn, *args, **kwargs)
        return wrapper

    def reset(self) -> None:
        """Manually reset the circuit to CLOSED."""
        self._state         = CircuitState.CLOSED
        self._failure_count = 0
        self._last_failure_at = None
        log.info("Circuit '%s' manually reset to CLOSED", self.name)

    def __repr__(self) -> str:
        return (
            f"CircuitBreaker(name={self.name!r}, state={self.state.value}, "
            f"failures={self._failure_count}/{self.failure_threshold})"
        )
