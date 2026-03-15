"""tests/test_retry.py — Tests for retry, RetryPolicy, CircuitBreaker."""
import time
import pytest
from de_utils.retry import (
    RetryPolicy, retry, with_retry,
    CircuitBreaker, CircuitState, CircuitBreakerOpenError,
)


class TestRetryPolicy:

    def test_succeeds_first_try(self):
        policy  = RetryPolicy(max_attempts=3)
        counter = {"n": 0}
        def fn():
            counter["n"] += 1
            return "ok"
        assert policy.execute(fn) == "ok"
        assert counter["n"] == 1

    def test_retries_on_transient_error(self):
        policy  = RetryPolicy(max_attempts=3, wait_seconds=0)
        counter = {"n": 0}
        def fn():
            counter["n"] += 1
            if counter["n"] < 3:
                raise ConnectionError("transient")
            return "ok"
        assert policy.execute(fn) == "ok"
        assert counter["n"] == 3

    def test_raises_after_max_attempts(self):
        policy = RetryPolicy(max_attempts=2, wait_seconds=0)
        def fn():
            raise ConnectionError("always fails")
        with pytest.raises(ConnectionError):
            policy.execute(fn)

    def test_does_not_retry_non_transient(self):
        policy  = RetryPolicy(max_attempts=3, wait_seconds=0)
        counter = {"n": 0}
        def fn():
            counter["n"] += 1
            raise ValueError("not transient")
        with pytest.raises(ValueError):
            policy.execute(fn)
        assert counter["n"] == 1

    def test_retries_custom_exception(self):
        policy  = RetryPolicy(max_attempts=3, wait_seconds=0, retryable_on=(ValueError,))
        counter = {"n": 0}
        def fn():
            counter["n"] += 1
            if counter["n"] < 2:
                raise ValueError("retry this")
            return "done"
        assert policy.execute(fn) == "done"
        assert counter["n"] == 2

    def test_backoff_increases_wait(self):
        waits = []
        policy = RetryPolicy(max_attempts=3, wait_seconds=1.0, backoff=2.0, jitter=False)
        # Just test the _wait calculation directly
        assert policy._wait(0) == 1.0
        assert policy._wait(1) == 2.0
        assert policy._wait(2) == 4.0

    def test_max_wait_capped(self):
        policy = RetryPolicy(max_attempts=3, wait_seconds=10.0, backoff=10.0,
                             max_wait_seconds=15.0, jitter=False)
        assert policy._wait(3) == 15.0

    def test_jitter_within_range(self):
        policy = RetryPolicy(wait_seconds=1.0, backoff=1.0, jitter=True)
        for _ in range(20):
            w = policy._wait(0)
            assert 0.75 <= w <= 1.25


class TestRetryDecorator:

    def test_decorator_retries(self):
        counter = {"n": 0}

        @retry(max_attempts=3, wait_seconds=0)
        def flaky():
            counter["n"] += 1
            if counter["n"] < 3:
                raise ConnectionError("oops")
            return "ok"

        assert flaky() == "ok"
        assert counter["n"] == 3

    def test_decorator_preserves_function_name(self):
        @retry(max_attempts=1)
        def my_function():
            pass
        assert my_function.__name__ == "my_function"

    def test_with_retry_functional(self):
        counter = {"n": 0}
        def fn():
            counter["n"] += 1
            if counter["n"] < 2:
                raise ConnectionError("retry")
            return "done"
        result = with_retry(fn, RetryPolicy(max_attempts=3, wait_seconds=0))
        assert result == "done"


class TestCircuitBreaker:

    def test_closed_by_default(self):
        cb = CircuitBreaker(failure_threshold=3)
        assert cb.state == CircuitState.CLOSED

    def test_opens_after_threshold(self):
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=999)
        for _ in range(3):
            try:
                cb.call(lambda: (_ for _ in ()).throw(ConnectionError("fail")))
            except:
                pass
        assert cb.state == CircuitState.OPEN

    def test_open_circuit_raises_immediately(self):
        cb = CircuitBreaker(failure_threshold=1, recovery_timeout=999)
        try:
            cb.call(lambda: (_ for _ in ()).throw(ConnectionError("fail")))
        except:
            pass
        with pytest.raises(CircuitBreakerOpenError):
            cb.call(lambda: "should not run")

    def test_success_resets_failure_count(self):
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=999)
        try:
            cb.call(lambda: (_ for _ in ()).throw(ConnectionError("fail")))
        except:
            pass
        cb.call(lambda: "ok")
        assert cb._failure_count == 0
        assert cb.state == CircuitState.CLOSED

    def test_protect_decorator(self):
        cb      = CircuitBreaker(failure_threshold=3)
        counter = {"n": 0}

        @cb.protect
        def fn():
            counter["n"] += 1
            return "ok"

        fn()
        assert counter["n"] == 1

    def test_manual_reset(self):
        cb = CircuitBreaker(failure_threshold=1, recovery_timeout=999)
        try:
            cb.call(lambda: (_ for _ in ()).throw(ConnectionError("fail")))
        except:
            pass
        cb.reset()
        assert cb.state == CircuitState.CLOSED
        assert cb._failure_count == 0

    def test_repr_contains_state(self):
        cb = CircuitBreaker(name="test_cb")
        assert "test_cb" in repr(cb)
        assert "CLOSED" in repr(cb)
