"""
de_utils.delta
--------------
Delta Lake utility operations: OPTIMIZE, VACUUM, Z-ORDER, time travel,
clone, restore, and change data feed helpers.

All operations are no-ops (with a warning) when run against non-Delta tables,
so they're safe to include in generic pipelines.

Usage
-----
>>> from de_utils.delta import DeltaUtils
>>> du = DeltaUtils(spark)
>>> du.optimize("silver.orders", zorder_by=["customer_id", "order_date"])
>>> du.vacuum("silver.orders", retention_hours=168)
>>> du.show_history("silver.orders")
>>> v5_df = du.read_version("silver.orders", version=5)
>>> du.clone("silver.orders", "gold.orders_backup", shallow=True)
"""

from __future__ import annotations

from typing import List, Optional, TYPE_CHECKING

from .utils import get_logger

if TYPE_CHECKING:
    from pyspark.sql import SparkSession, DataFrame

log = get_logger(__name__)


class DeltaUtils:
    """
    Wrappers around common Delta Lake maintenance and time-travel operations.
    """

    def __init__(self, spark: "SparkSession"):
        self.spark = spark

    def _is_delta(self, table: str) -> bool:
        """Return True if the table is stored in Delta format."""
        try:
            props = self.spark.sql(f"SHOW TBLPROPERTIES {table}").collect()
            for row in props:
                if "delta" in str(row).lower():
                    return True
            # Also check via DESCRIBE DETAIL
            detail = self.spark.sql(f"DESCRIBE DETAIL {table}").collect()
            return any("delta" in str(r).lower() for r in detail)
        except Exception:
            return False

    def _require_delta(self, table: str) -> bool:
        if not self._is_delta(table):
            log.warning(
                "Table '%s' is not a Delta table — skipping operation. "
                "Convert with: de_utils.delta.DeltaUtils.convert_to_delta()", table
            )
            return False
        return True

    # ------------------------------------------------------------------
    # OPTIMIZE + Z-ORDER
    # ------------------------------------------------------------------

    def optimize(
        self,
        table: str,
        where_clause: Optional[str] = None,
        zorder_by: Optional[List[str]] = None,
    ) -> None:
        """
        Run OPTIMIZE on a Delta table to compact small files.
        Optionally scope to a partition subset and/or apply Z-ORDER.

        Example
        -------
        >>> du.optimize("silver.orders", where_clause="year=2024", zorder_by=["customer_id"])
        """
        if not self._require_delta(table):
            return
        sql = f"OPTIMIZE {table}"
        if where_clause:
            sql += f" WHERE {where_clause}"
        if zorder_by:
            cols = ", ".join(zorder_by)
            sql += f" ZORDER BY ({cols})"
        log.info("Running OPTIMIZE on %s (zorder=%s)", table, zorder_by)
        self.spark.sql(sql)
        log.info("OPTIMIZE complete for %s", table)

    # ------------------------------------------------------------------
    # VACUUM
    # ------------------------------------------------------------------

    def vacuum(
        self,
        table: str,
        retention_hours: int = 168,
        dry_run: bool = False,
    ) -> "DataFrame":
        """
        Remove files older than *retention_hours* from a Delta table.
        Default retention is 7 days (168 hours).

        Set dry_run=True to preview what would be deleted without deleting.
        """
        if not self._require_delta(table):
            return self.spark.createDataFrame([], schema="path STRING")
        dry = "DRY RUN" if dry_run else ""
        sql = f"VACUUM {table} RETAIN {retention_hours} HOURS {dry}"
        log.info("VACUUM %s (retain=%dh, dry_run=%s)", table, retention_hours, dry_run)
        return self.spark.sql(sql)

    # ------------------------------------------------------------------
    # Time Travel
    # ------------------------------------------------------------------

    def read_version(self, table: str, version: int) -> "DataFrame":
        """Read a Delta table at a specific version."""
        log.info("Reading %s @ version %d", table, version)
        return self.spark.read.format("delta").option("versionAsOf", version).table(table)

    def read_timestamp(self, table: str, timestamp: str) -> "DataFrame":
        """
        Read a Delta table as of a specific timestamp.

        Example
        -------
        >>> df = du.read_timestamp("silver.orders", "2024-01-01 00:00:00")
        """
        log.info("Reading %s @ timestamp '%s'", table, timestamp)
        return (
            self.spark.read.format("delta")
            .option("timestampAsOf", timestamp)
            .table(table)
        )

    def show_history(self, table: str, limit: int = 20) -> "DataFrame":
        """Return the Delta transaction history for a table."""
        return self.spark.sql(f"DESCRIBE HISTORY {table}").limit(limit)

    def get_latest_version(self, table: str) -> int:
        """Return the current (latest) version number of a Delta table."""
        row = self.spark.sql(f"DESCRIBE HISTORY {table}").orderBy(
            "version", ascending=False
        ).limit(1).collect()
        return row[0]["version"] if row else 0

    # ------------------------------------------------------------------
    # RESTORE
    # ------------------------------------------------------------------

    def restore(self, table: str, version: Optional[int] = None,
                timestamp: Optional[str] = None) -> None:
        """
        Restore a Delta table to a previous version or timestamp.
        Exactly one of *version* or *timestamp* must be provided.
        """
        if not self._require_delta(table):
            return
        if version is not None:
            sql = f"RESTORE TABLE {table} TO VERSION AS OF {version}"
        elif timestamp:
            sql = f"RESTORE TABLE {table} TO TIMESTAMP AS OF '{timestamp}'"
        else:
            raise ValueError("Provide either version or timestamp for restore.")
        log.info("Restoring %s to %s", table, version or timestamp)
        self.spark.sql(sql)
        log.info("Restore complete for %s", table)

    # ------------------------------------------------------------------
    # CLONE
    # ------------------------------------------------------------------

    def clone(
        self,
        source_table: str,
        target_table: str,
        shallow: bool = False,
        version: Optional[int] = None,
        replace: bool = False,
    ) -> None:
        """
        Clone a Delta table (deep or shallow).

        Shallow clone: metadata + transaction log only, no data copy.
        Deep clone: full data copy — independent from source.

        Example
        -------
        >>> du.clone("silver.orders", "silver.orders_backup", shallow=False)
        """
        if not self._require_delta(source_table):
            return
        clone_type = "SHALLOW" if shallow else "DEEP"
        or_replace = "OR REPLACE" if replace else ""
        at_version = f"VERSION AS OF {version}" if version is not None else ""
        sql = (
            f"CREATE {or_replace} {clone_type} CLONE {source_table} "
            f"{at_version} "
            f"AS {target_table}"
        )
        log.info("%s CLONE %s → %s", clone_type, source_table, target_table)
        self.spark.sql(sql)

    # ------------------------------------------------------------------
    # Change Data Feed (CDF)
    # ------------------------------------------------------------------

    def enable_change_data_feed(self, table: str) -> None:
        """Enable the Delta Change Data Feed for incremental CDC consumption."""
        self.spark.sql(
            f"ALTER TABLE {table} SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"
        )
        log.info("Change Data Feed enabled on %s", table)

    def read_changes(
        self,
        table: str,
        starting_version: int = 0,
        ending_version: Optional[int] = None,
    ) -> "DataFrame":
        """
        Read the Delta Change Data Feed between versions.

        Returns a DataFrame with an extra _change_type column:
            insert, update_preimage, update_postimage, delete
        """
        reader = (
            self.spark.read.format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", starting_version)
        )
        if ending_version is not None:
            reader = reader.option("endingVersion", ending_version)
        log.info("Reading CDF from %s [v%d → v%s]", table, starting_version,
                 ending_version or "latest")
        return reader.table(table)

    # ------------------------------------------------------------------
    # Convert Parquet → Delta
    # ------------------------------------------------------------------

    def convert_to_delta(self, table: str, partition_spec: Optional[str] = None) -> None:
        """
        Convert an existing Parquet table to Delta format in-place.

        Example
        -------
        >>> du.convert_to_delta("silver.orders", partition_spec="year INT, month INT")
        """
        sql = f"CONVERT TO DELTA {table}"
        if partition_spec:
            sql += f" PARTITIONED BY ({partition_spec})"
        log.info("Converting %s to Delta format", table)
        self.spark.sql(sql)
        log.info("Conversion complete: %s", table)

    # ------------------------------------------------------------------
    # Table details
    # ------------------------------------------------------------------

    def detail(self, table: str) -> "DataFrame":
        """Return Delta table details (size, file count, partitions, etc.)."""
        return self.spark.sql(f"DESCRIBE DETAIL {table}")

    def table_size_mb(self, table: str) -> float:
        """Return the approximate table size in MB."""
        try:
            row = self.detail(table).collect()[0]
            return round(row["sizeInBytes"] / (1024 * 1024), 2)
        except Exception:
            return 0.0

    def file_count(self, table: str) -> int:
        """Return the number of active files in the table."""
        try:
            row = self.detail(table).collect()[0]
            return row.get("numFiles", 0)
        except Exception:
            return 0
