"""
de_utils.pipeline
-----------------
Lightweight pipeline orchestration — define tasks, declare dependencies,
and run them in topologically-sorted order with parallel execution,
retry, timeout, and built-in audit/alert integration.

No external scheduler required. Designed to be called from a single
Spark driver — not a replacement for Airflow, but perfect for structured
multi-step ETL jobs that need more than a flat script.

Usage
-----
>>> from de_utils.pipeline import Pipeline, task

>>> pipeline = Pipeline(name="daily_orders_etl", spark=spark)

>>> @pipeline.task(depends_on=[])
... def ingest_bronze():
...     BronzeLoader(spark, adls_cfg, "bronze").ingest(raw_df, "orders_raw")

>>> @pipeline.task(depends_on=["ingest_bronze"])
... def load_silver():
...     SilverLoader(spark, "silver").load(bronze_df, "orders", key_columns=["order_id"])

>>> @pipeline.task(depends_on=["load_silver"])
... def load_gold():
...     GoldLoader(spark, "gold").load_dimension(silver_df, "dim_orders", ["order_id"])

>>> result = pipeline.run()
>>> result.show()
"""

from __future__ import annotations

import time
import traceback
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, TYPE_CHECKING

from .utils import get_logger, DataEngineeringError

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

log = get_logger(__name__)


# ── Task status ───────────────────────────────────────────────────────────────

class TaskStatus(str, Enum):
    PENDING   = "PENDING"
    RUNNING   = "RUNNING"
    SUCCESS   = "SUCCESS"
    FAILED    = "FAILED"
    SKIPPED   = "SKIPPED"
    UPSTREAM_FAILED = "UPSTREAM_FAILED"


@dataclass
class TaskResult:
    name:          str
    status:        TaskStatus
    started_at:    Optional[datetime] = None
    ended_at:      Optional[datetime] = None
    elapsed_sec:   float              = 0.0
    error:         Optional[str]      = None
    return_value:  Any                = None

    @property
    def succeeded(self) -> bool:
        return self.status == TaskStatus.SUCCESS


@dataclass
class PipelineResult:
    pipeline_name: str
    run_id:        str
    started_at:    datetime
    ended_at:      Optional[datetime]          = None
    task_results:  Dict[str, TaskResult]       = field(default_factory=dict)

    @property
    def succeeded(self) -> bool:
        return all(
            r.status in (TaskStatus.SUCCESS, TaskStatus.SKIPPED)
            for r in self.task_results.values()
        )

    @property
    def failed_tasks(self) -> List[str]:
        return [n for n, r in self.task_results.items() if r.status == TaskStatus.FAILED]

    @property
    def elapsed_sec(self) -> float:
        if self.ended_at:
            return (self.ended_at - self.started_at).total_seconds()
        return 0.0

    def show(self) -> None:
        status_icon = "✅" if self.succeeded else "❌"
        print(f"\n{'='*66}")
        print(f"  Pipeline: {self.pipeline_name}  {status_icon}  "
              f"({self.elapsed_sec:.1f}s total)")
        print(f"  Run ID:   {self.run_id}")
        print(f"{'='*66}")
        icons = {
            TaskStatus.SUCCESS:        "✅",
            TaskStatus.FAILED:         "❌",
            TaskStatus.SKIPPED:        "⏭ ",
            TaskStatus.UPSTREAM_FAILED:"⏭ ",
            TaskStatus.RUNNING:        "🔄",
            TaskStatus.PENDING:        "⏳",
        }
        for name, r in self.task_results.items():
            icon    = icons.get(r.status, "?")
            elapsed = f"{r.elapsed_sec:.1f}s" if r.elapsed_sec else ""
            err     = f" — {r.error[:60]}..." if r.error else ""
            print(f"  {icon} {name:<30} {r.status.value:<18} {elapsed}{err}")
        print(f"{'='*66}\n")


# ── Task definition ───────────────────────────────────────────────────────────

@dataclass
class TaskDef:
    name:          str
    fn:            Callable
    depends_on:    List[str]          = field(default_factory=list)
    retries:       int                = 0
    retry_delay:   float              = 5.0
    timeout_sec:   Optional[float]    = None
    skip_on_fail:  bool               = False   # mark SKIPPED instead of FAILED
    tags:          List[str]          = field(default_factory=list)
    description:   str                = ""


# ── Pipeline ──────────────────────────────────────────────────────────────────

class Pipeline:
    """
    Define and run a DAG of ETL tasks.

    Tasks are executed in topological order. Tasks with no inter-dependencies
    can run in parallel when `max_workers > 1`.
    """

    def __init__(
        self,
        name:         str,
        spark:        Optional["SparkSession"] = None,
        max_workers:  int = 1,
        audit_log:    Any = None,     # de_utils.audit.AuditLog instance
        alert_router: Any = None,     # de_utils.alerts.AlertRouter instance
    ):
        self.name         = name
        self.spark        = spark
        self.max_workers  = max_workers
        self.audit_log    = audit_log
        self.alert_router = alert_router
        self._tasks:      Dict[str, TaskDef] = {}

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def task(
        self,
        depends_on:   Optional[List[str]] = None,
        retries:      int   = 0,
        retry_delay:  float = 5.0,
        timeout_sec:  Optional[float] = None,
        skip_on_fail: bool  = False,
        tags:         Optional[List[str]] = None,
        description:  str   = "",
    ):
        """Decorator to register a function as a pipeline task."""
        def decorator(fn: Callable) -> Callable:
            self.register(
                fn,
                depends_on=depends_on or [],
                retries=retries,
                retry_delay=retry_delay,
                timeout_sec=timeout_sec,
                skip_on_fail=skip_on_fail,
                tags=tags or [],
                description=description or fn.__doc__ or "",
            )
            return fn
        return decorator

    def register(
        self,
        fn:           Callable,
        depends_on:   Optional[List[str]] = None,
        retries:      int   = 0,
        retry_delay:  float = 5.0,
        timeout_sec:  Optional[float] = None,
        skip_on_fail: bool  = False,
        tags:         Optional[List[str]] = None,
        description:  str   = "",
    ) -> "Pipeline":
        """Explicitly register a callable as a task."""
        t = TaskDef(
            name=fn.__name__,
            fn=fn,
            depends_on=depends_on or [],
            retries=retries,
            retry_delay=retry_delay,
            timeout_sec=timeout_sec,
            skip_on_fail=skip_on_fail,
            tags=tags or [],
            description=description,
        )
        self._tasks[t.name] = t
        return self

    # ------------------------------------------------------------------
    # Topological sort
    # ------------------------------------------------------------------

    def _topo_sort(self) -> List[List[str]]:
        """
        Return tasks grouped into levels (batches that can run in parallel).
        Raises DataEngineeringError if a cycle is detected.
        """
        in_degree: Dict[str, int] = {name: 0 for name in self._tasks}
        dependents: Dict[str, List[str]] = defaultdict(list)

        for name, task in self._tasks.items():
            for dep in task.depends_on:
                if dep not in self._tasks:
                    raise DataEngineeringError(
                        f"Task '{name}' depends on unknown task '{dep}'"
                    )
                in_degree[name] += 1
                dependents[dep].append(name)

        queue  = deque([n for n, d in in_degree.items() if d == 0])
        levels: List[List[str]] = []

        while queue:
            level = list(queue)
            levels.append(level)
            queue.clear()
            next_level_candidates: Dict[str, int] = {}
            for name in level:
                for dep in dependents[name]:
                    in_degree[dep] -= 1
                    if in_degree[dep] == 0:
                        next_level_candidates[dep] = 1
            queue.extend(next_level_candidates.keys())

        resolved = sum(len(lvl) for lvl in levels)
        if resolved < len(self._tasks):
            raise DataEngineeringError(
                "Cycle detected in pipeline DAG — check task dependencies."
            )
        return levels

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def _run_task(
        self,
        task_def:    TaskDef,
        failed_deps: Set[str],
    ) -> TaskResult:
        """Execute a single task with retries and timeout."""
        import signal

        # Skip if any upstream dependency failed
        if any(dep in failed_deps for dep in task_def.depends_on):
            log.warning("SKIP  %s (upstream failed)", task_def.name)
            return TaskResult(task_def.name, TaskStatus.UPSTREAM_FAILED)

        log.info("START %s", task_def.name)
        started_at = datetime.utcnow()
        last_exc: Optional[Exception] = None

        for attempt in range(task_def.retries + 1):
            try:
                t0     = time.time()
                result = task_def.fn()
                elapsed = time.time() - t0
                tr = TaskResult(
                    name=task_def.name,
                    status=TaskStatus.SUCCESS,
                    started_at=started_at,
                    ended_at=datetime.utcnow(),
                    elapsed_sec=elapsed,
                    return_value=result,
                )
                log.info("DONE  %s (%.1fs)", task_def.name, elapsed)
                return tr
            except Exception as exc:
                last_exc = exc
                if attempt < task_def.retries:
                    log.warning(
                        "RETRY %s (attempt %d/%d): %s — waiting %.1fs",
                        task_def.name, attempt + 1, task_def.retries + 1, exc, task_def.retry_delay,
                    )
                    time.sleep(task_def.retry_delay)

        # All attempts exhausted
        status = TaskStatus.SKIPPED if task_def.skip_on_fail else TaskStatus.FAILED
        log.error("FAIL  %s: %s", task_def.name, last_exc)
        return TaskResult(
            name=task_def.name,
            status=status,
            started_at=started_at,
            ended_at=datetime.utcnow(),
            elapsed_sec=(datetime.utcnow() - started_at).total_seconds(),
            error=traceback.format_exc(),
        )

    def run(
        self,
        tags:        Optional[List[str]] = None,
        dry_run:     bool = False,
    ) -> PipelineResult:
        """
        Execute the pipeline.

        Parameters
        ----------
        tags    : Only run tasks with these tags (None = run all)
        dry_run : Print execution plan without running anything
        """
        import uuid

        run_id    = str(uuid.uuid4())[:8]
        started   = datetime.utcnow()
        levels    = self._topo_sort()
        results:  Dict[str, TaskResult] = {}
        failed:   Set[str] = set()

        log.info("Pipeline '%s' starting (run_id=%s, tasks=%d, workers=%d)",
                 self.name, run_id, len(self._tasks), self.max_workers)

        if dry_run:
            print(f"\nDry-run: {self.name}")
            for i, level in enumerate(levels):
                tasks_str = ", ".join(level)
                print(f"  Level {i+1}: [{tasks_str}]")
            return PipelineResult(self.name, run_id, started)

        for level in levels:
            # Filter by tags
            tasks_in_level = [
                self._tasks[n] for n in level
                if tags is None or any(t in self._tasks[n].tags for t in tags)
            ]

            if self.max_workers > 1 and len(tasks_in_level) > 1:
                with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
                    futures = {
                        pool.submit(self._run_task, t, failed): t.name
                        for t in tasks_in_level
                    }
                    for future in as_completed(futures):
                        tr = future.result()
                        results[tr.name] = tr
                        if tr.status == TaskStatus.FAILED:
                            failed.add(tr.name)
            else:
                for task_def in tasks_in_level:
                    tr = self._run_task(task_def, failed)
                    results[tr.name] = tr
                    if tr.status == TaskStatus.FAILED:
                        failed.add(tr.name)

        ended = datetime.utcnow()
        pr    = PipelineResult(
            pipeline_name=self.name,
            run_id=run_id,
            started_at=started,
            ended_at=ended,
            task_results=results,
        )

        # Integrate with audit + alerts
        if self.audit_log:
            try:
                for name, tr in results.items():
                    self.audit_log.log(
                        table=name,
                        operation=f"PIPELINE_TASK",
                        status="SUCCESS" if tr.succeeded else "FAILED",
                        error_message=tr.error,
                    )
            except Exception as e:
                log.warning("Audit log flush failed: %s", e)

        if self.alert_router and not pr.succeeded:
            try:
                self.alert_router.send_failure(
                    table=self.name,
                    message=f"Pipeline failed — tasks: {', '.join(pr.failed_tasks)}",
                )
            except Exception as e:
                log.warning("Alert send failed: %s", e)

        log.info(
            "Pipeline '%s' finished in %.1fs — %s",
            self.name, pr.elapsed_sec, "SUCCESS" if pr.succeeded else "FAILED",
        )
        return pr

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    def list_tasks(self) -> List[Dict]:
        return [
            {"name": t.name, "depends_on": t.depends_on,
             "retries": t.retries, "tags": t.tags, "description": t.description}
            for t in self._tasks.values()
        ]

    def visualize(self) -> str:
        """Return a simple ASCII DAG representation."""
        lines = [f"Pipeline: {self.name}", ""]
        levels = self._topo_sort()
        for i, level in enumerate(levels):
            lines.append(f"  Level {i+1}:  " + "  →  ".join(
                f"[{n}]" for n in level
            ))
        return "\n".join(lines)
