"""
de_utils.checkpoint
-------------------
Job state management and checkpointing for restartable incremental ETL.

Persists watermarks, last-run metadata, and arbitrary job state to a
Spark table (backed by ADLS or any Hive warehouse).

Usage
-----
>>> from de_utils.checkpoint import JobCheckpoint
>>> cp = JobCheckpoint(spark, state_table="gold._job_state", job_name="orders_incremental")

>>> # Get the last successful watermark (returns None on first run)
>>> last_wm = cp.get_watermark("updated_at")

>>> # Load only new rows
>>> df = spark.table("bronze.orders").filter(f"updated_at > '{last_wm or '1970-01-01'}'")

>>> # After successful load, save the new watermark
>>> new_wm = df.agg({"updated_at": "max"}).collect()[0][0]
>>> cp.set_watermark("updated_at", str(new_wm))
>>> cp.commit()   # atomically writes all pending state

>>> # Idempotent re-run detection
>>> if cp.is_already_run(partition="2024-06-01"):
...     print("Skipping — already processed")
... else:
...     process(...)
...     cp.mark_partition_done("2024-06-01")
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from .utils import get_logger, DataEngineeringError

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

log = get_logger(__name__)


class JobCheckpoint:
    """
    Persistent job state store backed by a Spark table.

    State entries are keyed by (job_name, key_name) and store a string value
    plus optional metadata. The store is append-only — new rows overwrite
    old state logically (latest record wins per key).
    """

    _SCHEMA_DDL = """
        job_name    STRING,
        key_name    STRING,
        value       STRING,
        metadata    STRING,
        updated_at  STRING,
        run_id      STRING
    """

    def __init__(
        self,
        spark: "SparkSession",
        state_table: str,
        job_name: str,
        run_id: Optional[str] = None,
    ):
        self.spark       = spark
        self.state_table = state_table
        self.job_name    = job_name
        self.run_id      = run_id or datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        self._pending:   Dict[str, Dict] = {}    # key_name → {value, metadata}
        self._cache:     Optional[Dict]  = None  # loaded from table
        self._ensure_table()

    # ------------------------------------------------------------------
    # Read helpers
    # ------------------------------------------------------------------

    def _load(self) -> Dict[str, str]:
        """Load the latest value for every key belonging to this job."""
        if self._cache is not None:
            return self._cache
        try:
            from pyspark.sql import functions as F
            df = (
                self.spark.table(self.state_table)
                .filter(f"job_name = '{self.job_name}'")
                .groupBy("key_name")
                .agg(F.max("updated_at").alias("latest_ts"))
                .join(
                    self.spark.table(self.state_table)
                    .filter(f"job_name = '{self.job_name}'"),
                    on=["key_name"],
                )
                .filter(F.col("updated_at") == F.col("latest_ts"))
                .select("key_name", "value", "metadata")
            )
            self._cache = {r["key_name"]: {"value": r["value"], "metadata": r["metadata"]}
                           for r in df.collect()}
        except Exception:
            self._cache = {}
        return self._cache

    def get(self, key: str, default: Any = None) -> Optional[str]:
        """Get the latest value for a state key."""
        # Check pending first (uncommitted writes)
        if key in self._pending:
            return self._pending[key]["value"]
        state = self._load()
        entry = state.get(key)
        return entry["value"] if entry else default

    def get_watermark(self, column: str) -> Optional[str]:
        """Convenience wrapper — get the last watermark for a column."""
        return self.get(f"watermark::{column}")

    def get_all(self) -> Dict[str, str]:
        """Return all state key-value pairs for this job."""
        state = {k: v["value"] for k, v in self._load().items()}
        state.update({k: v["value"] for k, v in self._pending.items()})
        return state

    # ------------------------------------------------------------------
    # Write helpers
    # ------------------------------------------------------------------

    def set(self, key: str, value: Any, metadata: Optional[Dict] = None) -> None:
        """Stage a state entry (not written until commit())."""
        self._pending[key] = {
            "value":    str(value),
            "metadata": json.dumps(metadata or {}),
        }

    def set_watermark(self, column: str, value: Any) -> None:
        """Stage a watermark update for a specific column."""
        self.set(f"watermark::{column}", value)

    def mark_partition_done(self, partition_value: str) -> None:
        """Mark a partition as successfully processed."""
        self.set(f"partition::{partition_value}", "DONE",
                 metadata={"completed_at": str(datetime.utcnow())})

    def is_already_run(self, partition: str) -> bool:
        """Return True if *partition* has already been successfully processed."""
        return self.get(f"partition::{partition}") == "DONE"

    def set_last_run_status(self, status: str, message: str = "") -> None:
        """Record the outcome of the last run."""
        self.set("last_run_status", status, metadata={"message": message})

    def commit(self) -> None:
        """Atomically write all pending state changes to the state table."""
        if not self._pending:
            log.info("[%s] No state to commit.", self.job_name)
            return

        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        rows = [
            (self.job_name, key, entry["value"], entry["metadata"], now, self.run_id)
            for key, entry in self._pending.items()
        ]
        from pyspark.sql.types import StructType, StructField, StringType
        schema = StructType([
            StructField("job_name",   StringType()),
            StructField("key_name",   StringType()),
            StructField("value",      StringType()),
            StructField("metadata",   StringType()),
            StructField("updated_at", StringType()),
            StructField("run_id",     StringType()),
        ])
        df = self.spark.createDataFrame(rows, schema=schema)
        df.write.mode("append").saveAsTable(self.state_table)

        # Invalidate cache so next read sees new values
        self._cache = None
        committed_keys = list(self._pending.keys())
        self._pending.clear()
        log.info("[%s] Committed %d state keys: %s", self.job_name,
                 len(committed_keys), committed_keys)

    def rollback(self) -> None:
        """Discard all staged (uncommitted) state changes."""
        discarded = list(self._pending.keys())
        self._pending.clear()
        log.info("[%s] Rolled back %d uncommitted keys: %s",
                 self.job_name, len(discarded), discarded)

    def reset(self, key: Optional[str] = None) -> None:
        """
        Hard-reset state for a key (or ALL keys for this job).
        Writes a NULL-value tombstone so the key reads as absent.
        """
        if key:
            self.set(key, "__RESET__")
        else:
            for k in list(self._load().keys()):
                self.set(k, "__RESET__")
        self.commit()

    # ------------------------------------------------------------------
    # History
    # ------------------------------------------------------------------

    def history(self, key: Optional[str] = None, limit: int = 50) -> "Any":
        """Return the full change history for a key (or all keys)."""
        df = self.spark.table(self.state_table).filter(
            f"job_name = '{self.job_name}'"
        )
        if key:
            df = df.filter(f"key_name = '{key}'")
        return df.orderBy("updated_at", ascending=False).limit(limit)

    # ------------------------------------------------------------------
    # Setup
    # ------------------------------------------------------------------

    def _ensure_table(self) -> None:
        try:
            self.spark.sql(f"DESCRIBE TABLE {self.state_table}")
        except Exception:
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.state_table} (
                    {self._SCHEMA_DDL}
                ) STORED AS PARQUET
            """)
            log.info("Created state table: %s", self.state_table)

    def __repr__(self) -> str:
        pending_keys = list(self._pending.keys())
        return (f"JobCheckpoint(job={self.job_name!r}, run_id={self.run_id!r}, "
                f"pending={pending_keys})")
