"""
de_utils.loader
---------------
Load strategies for moving data from source to target tables:

    FULL            — truncate + reload every run
    INCREMENTAL     — append new rows only (watermark-based)
    PARTITION       — overwrite specific partition(s)
    MERGE / CDC     — upsert using match keys
    SNAPSHOT        — write a full snapshot with a snapshot date column
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, TYPE_CHECKING

from .table_ops import TableOperations
from .utils import get_logger, DataEngineeringError, validate_columns

if TYPE_CHECKING:
    from pyspark.sql import SparkSession, DataFrame

log = get_logger(__name__)


class LoadType(str, Enum):
    FULL = "FULL"
    INCREMENTAL = "INCREMENTAL"
    PARTITION = "PARTITION"
    MERGE = "MERGE"
    SNAPSHOT = "SNAPSHOT"


class DataLoader:
    """
    Unified loader that applies the correct load strategy based on *LoadType*.

    Example — Full load
    -------------------
    >>> loader = DataLoader(spark, database="silver")
    >>> loader.load(
    ...     source_df=raw_df,
    ...     target_table="orders",
    ...     load_type=LoadType.FULL,
    ... )

    Example — Partition load
    ------------------------
    >>> loader.load(
    ...     source_df=daily_df,
    ...     target_table="orders",
    ...     load_type=LoadType.PARTITION,
    ...     partition_keys=["year", "month", "day"],
    ... )

    Example — Incremental (watermark)
    ----------------------------------
    >>> loader.load(
    ...     source_df=incremental_df,
    ...     target_table="orders",
    ...     load_type=LoadType.INCREMENTAL,
    ...     watermark_column="updated_at",
    ...     last_watermark="2024-01-01 00:00:00",
    ... )

    Example — Merge / CDC
    ----------------------
    >>> loader.load(
    ...     source_df=cdc_df,
    ...     target_table="orders",
    ...     load_type=LoadType.MERGE,
    ...     match_keys=["order_id"],
    ... )
    """

    def __init__(self, spark: "SparkSession", database: str = "default"):
        self.spark = spark
        self.database = database
        self._ops = TableOperations(spark, database)

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def load(
        self,
        source_df: "DataFrame",
        target_table: str,
        load_type: LoadType = LoadType.FULL,
        # Partition load
        partition_keys: Optional[List[str]] = None,
        # Incremental
        watermark_column: Optional[str] = None,
        last_watermark: Optional[str] = None,
        watermark_output_column: str = "_watermark",
        # Merge
        match_keys: Optional[List[str]] = None,
        update_columns: Optional[List[str]] = None,
        delete_when_not_matched_by_source: bool = False,
        # Snapshot
        snapshot_date_column: str = "snapshot_date",
        snapshot_date: Optional[str] = None,
        # General
        file_format: str = "PARQUET",
        location: Optional[str] = None,
        pre_validate: bool = True,
    ) -> "LoadResult":
        """
        Execute the load and return a LoadResult summary.
        """
        load_type = LoadType(load_type)
        log.info(
            "Loading '%s.%s' — strategy=%s rows=%s",
            self.database, target_table, load_type.value,
            source_df.count() if pre_validate else "?",
        )

        start = datetime.utcnow()

        if load_type == LoadType.FULL:
            self._full_load(source_df, target_table, partition_keys, file_format, location)

        elif load_type == LoadType.INCREMENTAL:
            if not watermark_column:
                raise DataEngineeringError("watermark_column is required for INCREMENTAL loads.")
            source_df = self._apply_watermark(source_df, watermark_column, last_watermark)
            self._ops.insert_append(target_table, source_df)

        elif load_type == LoadType.PARTITION:
            if not partition_keys:
                raise DataEngineeringError("partition_keys are required for PARTITION loads.")
            validate_columns(source_df.columns, partition_keys, "source_df")
            self._ops.insert_overwrite(target_table, source_df, partition_by=partition_keys)

        elif load_type == LoadType.MERGE:
            if not match_keys:
                raise DataEngineeringError("match_keys are required for MERGE loads.")
            self._ops.merge(
                target_table,
                source_df,
                match_keys,
                update_columns=update_columns,
                delete_when_not_matched_by_source=delete_when_not_matched_by_source,
            )

        elif load_type == LoadType.SNAPSHOT:
            source_df = self._add_snapshot_date(source_df, snapshot_date_column, snapshot_date)
            self._ops.insert_append(target_table, source_df)

        else:
            raise DataEngineeringError(f"Unknown LoadType: {load_type}")

        elapsed = (datetime.utcnow() - start).total_seconds()
        log.info("Load complete in %.1fs", elapsed)
        return LoadResult(
            target_table=f"{self.database}.{target_table}",
            load_type=load_type,
            elapsed_seconds=elapsed,
        )

    # ------------------------------------------------------------------
    # Strategy implementations
    # ------------------------------------------------------------------

    def _full_load(
        self,
        df: "DataFrame",
        table: str,
        partition_keys: Optional[List[str]],
        file_format: str,
        location: Optional[str],
    ) -> None:
        """Truncate-and-reload the entire table."""
        self._ops.create_or_replace(table, df, partition_by=partition_keys,
                                    file_format=file_format, location=location)

    def _apply_watermark(
        self,
        df: "DataFrame",
        watermark_col: str,
        last_watermark: Optional[str],
    ) -> "DataFrame":
        """Filter the source to only rows newer than *last_watermark*."""
        if last_watermark:
            log.info("Applying watermark filter: %s > '%s'", watermark_col, last_watermark)
            df = df.filter(f"`{watermark_col}` > '{last_watermark}'")
        row_count = df.count()
        log.info("Incremental rows after watermark: %d", row_count)
        return df

    def _add_snapshot_date(
        self,
        df: "DataFrame",
        col_name: str,
        snapshot_date: Optional[str],
    ) -> "DataFrame":
        from pyspark.sql import functions as F

        val = snapshot_date or datetime.utcnow().strftime("%Y-%m-%d")
        return df.withColumn(col_name, F.lit(val).cast("date"))

    # ------------------------------------------------------------------
    # Convenience methods
    # ------------------------------------------------------------------

    def full_load(self, source_df: "DataFrame", target_table: str, **kwargs) -> "LoadResult":
        return self.load(source_df, target_table, LoadType.FULL, **kwargs)

    def incremental_load(
        self,
        source_df: "DataFrame",
        target_table: str,
        watermark_column: str,
        last_watermark: Optional[str] = None,
        **kwargs,
    ) -> "LoadResult":
        return self.load(
            source_df, target_table, LoadType.INCREMENTAL,
            watermark_column=watermark_column,
            last_watermark=last_watermark,
            **kwargs,
        )

    def partition_load(
        self,
        source_df: "DataFrame",
        target_table: str,
        partition_keys: List[str],
        **kwargs,
    ) -> "LoadResult":
        return self.load(
            source_df, target_table, LoadType.PARTITION,
            partition_keys=partition_keys, **kwargs,
        )

    def merge_load(
        self,
        source_df: "DataFrame",
        target_table: str,
        match_keys: List[str],
        **kwargs,
    ) -> "LoadResult":
        return self.load(
            source_df, target_table, LoadType.MERGE,
            match_keys=match_keys, **kwargs,
        )


class LoadResult:
    """Simple result object returned after each load."""

    def __init__(self, target_table: str, load_type: LoadType, elapsed_seconds: float):
        self.target_table = target_table
        self.load_type = load_type
        self.elapsed_seconds = elapsed_seconds
        self.completed_at = datetime.utcnow()

    def __repr__(self) -> str:
        return (
            f"LoadResult(table={self.target_table!r}, "
            f"type={self.load_type.value}, "
            f"elapsed={self.elapsed_seconds:.1f}s, "
            f"completed_at={self.completed_at.isoformat()})"
        )
