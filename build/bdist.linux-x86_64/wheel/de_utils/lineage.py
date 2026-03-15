"""
de_utils.lineage
----------------
Automatic data lineage tracking.

Every load/transform writes a lineage record:
    run_id, job_name, source_table, target_table, transformation_type,
    rows_read, rows_written, columns_added, columns_dropped, created_at

Records are appended to a central lineage table and can be queried
to build a full upstream/downstream dependency graph.

Usage
-----
>>> from de_utils.lineage import LineageTracker
>>> tracker = LineageTracker(spark, lineage_table="gold._lineage", job_name="daily_orders_etl")
>>> with tracker.track(source="bronze.orders_raw", target="silver.orders"):
...     transformed_df = transform(raw_df)
...     transformed_df.write.mode("append").saveAsTable("silver.orders")

Or manually:
>>> tracker.record(
...     source="bronze.raw",
...     target="silver.orders",
...     transformation_type="INCREMENTAL",
...     rows_read=50000,
...     rows_written=49800,
... )
>>> tracker.save()
"""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, TYPE_CHECKING

from .utils import get_logger, DataEngineeringError

if TYPE_CHECKING:
    from pyspark.sql import SparkSession, DataFrame

log = get_logger(__name__)


@dataclass
class LineageRecord:
    source_table:        str
    target_table:        str
    transformation_type: str
    job_name:            str
    run_id:              str
    rows_read:           int      = 0
    rows_written:        int      = 0
    columns_added:       List[str]= field(default_factory=list)
    columns_dropped:     List[str]= field(default_factory=list)
    extra_metadata:      Dict     = field(default_factory=dict)
    started_at:          datetime = field(default_factory=datetime.utcnow)
    completed_at:        Optional[datetime] = None
    status:              str      = "RUNNING"   # RUNNING | SUCCESS | FAILED
    error_message:       str      = ""

    def complete(self, rows_written: Optional[int] = None) -> None:
        self.completed_at = datetime.utcnow()
        self.status = "SUCCESS"
        if rows_written is not None:
            self.rows_written = rows_written

    def fail(self, error: str) -> None:
        self.completed_at = datetime.utcnow()
        self.status = "FAILED"
        self.error_message = str(error)

    @property
    def duration_seconds(self) -> Optional[float]:
        if self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None

    def to_row(self) -> tuple:
        return (
            self.run_id,
            self.job_name,
            self.source_table,
            self.target_table,
            self.transformation_type,
            self.rows_read,
            self.rows_written,
            ",".join(self.columns_added),
            ",".join(self.columns_dropped),
            self.status,
            self.error_message,
            self.duration_seconds,
            str(self.started_at),
            str(self.completed_at) if self.completed_at else None,
        )


class LineageTracker:
    """
    Tracks data lineage across ETL jobs.

    Example — context manager (auto-captures timing and status)
    -----------------------------------------------------------
    >>> tracker = LineageTracker(spark, "gold._lineage", job_name="orders_etl")
    >>> with tracker.track("bronze.raw", "silver.orders", "INCREMENTAL") as ctx:
    ...     df = spark.table("bronze.raw")
    ...     ctx.rows_read = df.count()
    ...     result_df = transform(df)
    ...     result_df.write.mode("append").saveAsTable("silver.orders")
    ...     ctx.rows_written = result_df.count()
    >>> tracker.save()
    """

    _SCHEMA_DDL = """
        run_id              STRING,
        job_name            STRING,
        source_table        STRING,
        target_table        STRING,
        transformation_type STRING,
        rows_read           LONG,
        rows_written        LONG,
        columns_added       STRING,
        columns_dropped     STRING,
        status              STRING,
        error_message       STRING,
        duration_seconds    DOUBLE,
        started_at          STRING,
        completed_at        STRING
    """

    def __init__(
        self,
        spark: "SparkSession",
        lineage_table: str,
        job_name: str = "unknown_job",
        run_id: Optional[str] = None,
        auto_save: bool = True,
    ):
        self.spark         = spark
        self.lineage_table = lineage_table
        self.job_name      = job_name
        self.run_id        = run_id or str(uuid.uuid4())[:8]
        self.auto_save     = auto_save
        self._records: List[LineageRecord] = []

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    @contextmanager
    def track(
        self,
        source: str,
        target: str,
        transformation_type: str = "UNKNOWN",
    ):
        """
        Context manager that auto-records success/failure and timing.
        Yields the LineageRecord so callers can set rows_read / rows_written.
        """
        rec = LineageRecord(
            source_table=source,
            target_table=target,
            transformation_type=transformation_type,
            job_name=self.job_name,
            run_id=self.run_id,
        )
        self._records.append(rec)
        try:
            yield rec
            rec.complete()
        except Exception as e:
            rec.fail(str(e))
            raise
        finally:
            if self.auto_save:
                self._save_record(rec)

    # ------------------------------------------------------------------
    # Manual recording
    # ------------------------------------------------------------------

    def record(
        self,
        source: str,
        target: str,
        transformation_type: str = "UNKNOWN",
        rows_read: int = 0,
        rows_written: int = 0,
        columns_added: Optional[List[str]] = None,
        columns_dropped: Optional[List[str]] = None,
        status: str = "SUCCESS",
    ) -> LineageRecord:
        rec = LineageRecord(
            source_table=source,
            target_table=target,
            transformation_type=transformation_type,
            job_name=self.job_name,
            run_id=self.run_id,
            rows_read=rows_read,
            rows_written=rows_written,
            columns_added=columns_added or [],
            columns_dropped=columns_dropped or [],
            status=status,
        )
        rec.complete()
        self._records.append(rec)
        return rec

    def compare_schemas_for_lineage(
        self,
        source_df: "DataFrame",
        target_df: "DataFrame",
    ) -> Dict[str, List[str]]:
        """Return columns added/dropped between source and target schemas."""
        src_cols = set(source_df.columns)
        tgt_cols = set(target_df.columns)
        return {
            "added":   list(tgt_cols - src_cols),
            "dropped": list(src_cols - tgt_cols),
        }

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def save(self) -> None:
        """Write all pending lineage records to the lineage table."""
        if not self._records:
            log.info("No lineage records to save.")
            return
        self._ensure_table()
        rows = [r.to_row() for r in self._records]
        from pyspark.sql.types import (
            StructType, StructField, StringType, LongType, DoubleType,
        )
        schema = StructType([
            StructField("run_id",              StringType()),
            StructField("job_name",            StringType()),
            StructField("source_table",        StringType()),
            StructField("target_table",        StringType()),
            StructField("transformation_type", StringType()),
            StructField("rows_read",           LongType()),
            StructField("rows_written",        LongType()),
            StructField("columns_added",       StringType()),
            StructField("columns_dropped",     StringType()),
            StructField("status",              StringType()),
            StructField("error_message",       StringType()),
            StructField("duration_seconds",    DoubleType()),
            StructField("started_at",          StringType()),
            StructField("completed_at",        StringType()),
        ])
        df = self.spark.createDataFrame(rows, schema=schema)
        df.write.mode("append").saveAsTable(self.lineage_table)
        log.info("Saved %d lineage records to %s", len(rows), self.lineage_table)
        self._records.clear()

    def _save_record(self, rec: LineageRecord) -> None:
        tmp = self._records
        self._records = [rec]
        self.save()
        self._records = tmp

    def _ensure_table(self) -> None:
        try:
            self.spark.sql(f"DESCRIBE TABLE {self.lineage_table}")
        except Exception:
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.lineage_table} (
                    {self._SCHEMA_DDL}
                ) STORED AS PARQUET
            """)
            log.info("Created lineage table: %s", self.lineage_table)

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------

    def get_upstream(self, table: str) -> "DataFrame":
        """Return all tables that have written into *table*."""
        return (
            self.spark.table(self.lineage_table)
            .filter(f"target_table = '{table}' AND status = 'SUCCESS'")
            .select("source_table", "transformation_type", "job_name",
                    "rows_written", "started_at")
            .distinct()
        )

    def get_downstream(self, table: str) -> "DataFrame":
        """Return all tables that were built from *table*."""
        return (
            self.spark.table(self.lineage_table)
            .filter(f"source_table = '{table}' AND status = 'SUCCESS'")
            .select("target_table", "transformation_type", "job_name",
                    "rows_written", "started_at")
            .distinct()
        )

    def get_job_history(self, job_name: Optional[str] = None, limit: int = 50) -> "DataFrame":
        """Return recent lineage records for a job (or all jobs)."""
        df = self.spark.table(self.lineage_table)
        if job_name:
            df = df.filter(f"job_name = '{job_name}'")
        return df.orderBy("started_at", ascending=False).limit(limit)
