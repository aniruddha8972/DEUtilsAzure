"""
de_utils.audit
--------------
Central audit log — every load/merge/SCD/DQ operation writes a structured
row capturing job context, table, operation type, row counts, and status.

The audit log is the single source of truth for:
    - What ran, when, and for how long
    - How many rows were inserted / updated / deleted
    - Whether the operation succeeded or failed
    - Which job and run_id triggered the operation

Usage
-----
>>> from de_utils.audit import AuditLog
>>> audit = AuditLog(spark, audit_table="gold._audit_log", job_name="daily_etl")

>>> # Manual entry
>>> audit.log(
...     table="silver.orders",
...     operation="FULL_LOAD",
...     rows_inserted=50000,
...     rows_updated=0,
...     rows_deleted=0,
...     status="SUCCESS",
... )

>>> # Context manager — auto-captures timing, status, and errors
>>> with audit.capture("silver.orders", "MERGE") as entry:
...     ops.merge("silver.orders", updates_df, match_keys=["order_id"])
...     entry.rows_updated = 1200
...     entry.rows_inserted = 300

>>> # Persist all buffered entries
>>> audit.flush()

>>> # Query helpers
>>> audit.get_table_history("silver.orders").show()
>>> audit.get_failed_jobs(since="2024-01-01").show()
"""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, TYPE_CHECKING

from .utils import get_logger

if TYPE_CHECKING:
    from pyspark.sql import SparkSession, DataFrame

log = get_logger(__name__)


@dataclass
class AuditEntry:
    """Single audit log entry — mutable so callers can fill in counts."""
    audit_id:      str
    job_name:      str
    run_id:        str
    table_name:    str
    operation:     str        # FULL_LOAD | INCREMENTAL | PARTITION | MERGE | SCD1..6 | DQ | etc.
    rows_read:     int = 0
    rows_inserted: int = 0
    rows_updated:  int = 0
    rows_deleted:  int = 0
    rows_rejected: int = 0
    status:        str = "RUNNING"   # RUNNING | SUCCESS | FAILED | SKIPPED
    error_message: str = ""
    extra:         str = ""          # JSON-serialised extra metadata
    started_at:    datetime = field(default_factory=datetime.utcnow)
    completed_at:  Optional[datetime] = None

    def complete(self, status: str = "SUCCESS") -> None:
        self.status = status
        self.completed_at = datetime.utcnow()

    def fail(self, error: str) -> None:
        self.status = "FAILED"
        self.error_message = str(error)[:1000]
        self.completed_at = datetime.utcnow()

    @property
    def duration_seconds(self) -> Optional[float]:
        if self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None

    def to_row(self) -> tuple:
        return (
            self.audit_id,
            self.job_name,
            self.run_id,
            self.table_name,
            self.operation,
            self.rows_read,
            self.rows_inserted,
            self.rows_updated,
            self.rows_deleted,
            self.rows_rejected,
            self.status,
            self.error_message,
            self.extra,
            self.duration_seconds,
            str(self.started_at),
            str(self.completed_at) if self.completed_at else None,
        )


class AuditLog:
    """
    Centralized audit log for all de_utils operations.
    """

    _SCHEMA_DDL = """
        audit_id         STRING,
        job_name         STRING,
        run_id           STRING,
        table_name       STRING,
        operation        STRING,
        rows_read        LONG,
        rows_inserted    LONG,
        rows_updated     LONG,
        rows_deleted     LONG,
        rows_rejected    LONG,
        status           STRING,
        error_message    STRING,
        extra            STRING,
        duration_seconds DOUBLE,
        started_at       STRING,
        completed_at     STRING
    """

    def __init__(
        self,
        spark: "SparkSession",
        audit_table: str,
        job_name: str = "unknown_job",
        run_id: Optional[str] = None,
        auto_flush: bool = True,
    ):
        self.spark       = spark
        self.audit_table = audit_table
        self.job_name    = job_name
        self.run_id      = run_id or str(uuid.uuid4())[:8]
        self.auto_flush  = auto_flush
        self._buffer: List[AuditEntry] = []
        self._ensure_table()

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    @contextmanager
    def capture(self, table_name: str, operation: str):
        """
        Context manager that auto-captures timing and success/failure.
        Yields a mutable AuditEntry so callers can set row counts.

        Example
        -------
        >>> with audit.capture("silver.orders", "MERGE") as e:
        ...     ops.merge(...)
        ...     e.rows_updated = 500
        """
        entry = AuditEntry(
            audit_id=str(uuid.uuid4())[:8],
            job_name=self.job_name,
            run_id=self.run_id,
            table_name=table_name,
            operation=operation,
        )
        self._buffer.append(entry)
        try:
            yield entry
            entry.complete("SUCCESS")
        except Exception as exc:
            entry.fail(str(exc))
            raise
        finally:
            if self.auto_flush:
                self._flush_entry(entry)

    # ------------------------------------------------------------------
    # Manual logging
    # ------------------------------------------------------------------

    def log(
        self,
        table: str,
        operation: str,
        rows_read: int = 0,
        rows_inserted: int = 0,
        rows_updated: int = 0,
        rows_deleted: int = 0,
        rows_rejected: int = 0,
        status: str = "SUCCESS",
        error_message: str = "",
        extra: str = "",
    ) -> AuditEntry:
        """Directly log a completed operation."""
        entry = AuditEntry(
            audit_id=str(uuid.uuid4())[:8],
            job_name=self.job_name,
            run_id=self.run_id,
            table_name=table,
            operation=operation,
            rows_read=rows_read,
            rows_inserted=rows_inserted,
            rows_updated=rows_updated,
            rows_deleted=rows_deleted,
            rows_rejected=rows_rejected,
            status=status,
            error_message=error_message,
            extra=extra,
        )
        entry.complete(status)
        self._buffer.append(entry)
        if self.auto_flush:
            self._flush_entry(entry)
        return entry

    def log_from_load_result(self, result, **kwargs) -> AuditEntry:
        """Create an audit entry from a DataLoader LoadResult."""
        return self.log(
            table=result.target_table,
            operation=result.load_type.value,
            **kwargs,
        )

    def log_from_scd_result(self, result, **kwargs) -> AuditEntry:
        """Create an audit entry from an SCDHandler SCD2Result."""
        return self.log(
            table=result.table,
            operation="SCD2",
            rows_inserted=result.new_inserts,
            rows_updated=result.updates,
            **kwargs,
        )

    def log_from_dq_report(self, report, **kwargs) -> AuditEntry:
        """Create an audit entry from a DQReport."""
        status = "SUCCESS" if report.passed else "FAILED"
        failed_rules = len(report.failures)
        return self.log(
            table=report.table_name,
            operation="DQ_CHECK",
            status=status,
            extra=f"rules_passed={len(report.results)-failed_rules} rules_failed={failed_rules}",
            **kwargs,
        )

    # ------------------------------------------------------------------
    # Flush / persistence
    # ------------------------------------------------------------------

    def flush(self) -> None:
        """Write all buffered entries to the audit table."""
        pending = [e for e in self._buffer if e.status != "FLUSHED"]
        if not pending:
            return
        self._write(pending)
        for e in pending:
            e.status = "FLUSHED"

    def _flush_entry(self, entry: AuditEntry) -> None:
        self._write([entry])

    def _write(self, entries: List[AuditEntry]) -> None:
        from pyspark.sql.types import (
            StructType, StructField, StringType, LongType, DoubleType,
        )
        schema = StructType([
            StructField("audit_id",         StringType()),
            StructField("job_name",         StringType()),
            StructField("run_id",           StringType()),
            StructField("table_name",       StringType()),
            StructField("operation",        StringType()),
            StructField("rows_read",        LongType()),
            StructField("rows_inserted",    LongType()),
            StructField("rows_updated",     LongType()),
            StructField("rows_deleted",     LongType()),
            StructField("rows_rejected",    LongType()),
            StructField("status",           StringType()),
            StructField("error_message",    StringType()),
            StructField("extra",            StringType()),
            StructField("duration_seconds", DoubleType()),
            StructField("started_at",       StringType()),
            StructField("completed_at",     StringType()),
        ])
        rows = [e.to_row() for e in entries]
        df = self.spark.createDataFrame(rows, schema=schema)
        df.write.mode("append").saveAsTable(self.audit_table)
        log.info("Audit: flushed %d entries to %s", len(entries), self.audit_table)

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------

    def get_table_history(self, table_name: str, limit: int = 100) -> "DataFrame":
        """Return all audit records for a specific table."""
        return (
            self.spark.table(self.audit_table)
            .filter(f"table_name = '{table_name}'")
            .orderBy("started_at", ascending=False)
            .limit(limit)
        )

    def get_failed_jobs(self, since: Optional[str] = None) -> "DataFrame":
        """Return all FAILED audit records, optionally filtered by date."""
        df = self.spark.table(self.audit_table).filter("status = 'FAILED'")
        if since:
            df = df.filter(f"started_at >= '{since}'")
        return df.orderBy("started_at", ascending=False)

    def get_run_summary(self, run_id: Optional[str] = None) -> "DataFrame":
        """Summarise a run: total rows, status counts, duration."""
        from pyspark.sql import functions as F
        rid = run_id or self.run_id
        return (
            self.spark.table(self.audit_table)
            .filter(f"run_id = '{rid}'")
            .groupBy("job_name", "run_id")
            .agg(
                F.sum("rows_inserted").alias("total_inserted"),
                F.sum("rows_updated").alias("total_updated"),
                F.sum("rows_deleted").alias("total_deleted"),
                F.sum("duration_seconds").alias("total_duration_s"),
                F.count(F.when(F.col("status") == "SUCCESS", 1)).alias("success_ops"),
                F.count(F.when(F.col("status") == "FAILED",  1)).alias("failed_ops"),
            )
        )

    # ------------------------------------------------------------------
    # Table setup
    # ------------------------------------------------------------------

    def _ensure_table(self) -> None:
        try:
            self.spark.sql(f"DESCRIBE TABLE {self.audit_table}")
        except Exception:
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.audit_table} (
                    {self._SCHEMA_DDL}
                ) STORED AS PARQUET
            """)
            log.info("Created audit table: %s", self.audit_table)
