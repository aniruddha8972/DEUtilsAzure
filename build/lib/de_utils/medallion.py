"""
de_utils.medallion
------------------
Opinionated Bronze / Silver / Gold loaders following the Medallion
(multi-hop) architecture pattern.

Each layer has pre-configured, sensible defaults:

    BronzeLoader  — raw ingestion, append-only, no transforms,
                    schema-on-read, adds metadata columns (_ingest_ts,
                    _source_file, _run_id).

    SilverLoader  — cleansed / conformed layer, deduplication,
                    schema enforcement, type casting, null checks,
                    incremental or partition-key loads.

    GoldLoader    — aggregated / dimensional layer, SCD2 dimensions,
                    full or merge loads, DQ checks built-in.

Usage
-----
>>> from de_utils.medallion import BronzeLoader, SilverLoader, GoldLoader
>>>
>>> # Bronze: land raw data as-is
>>> bronze = BronzeLoader(spark, adls_config=adls_cfg, database="bronze")
>>> bronze.ingest(raw_df, target_table="orders_raw", source_tag="s3://orders/2024/06/")
>>>
>>> # Silver: cleanse and deduplicate
>>> silver = SilverLoader(spark, database="silver")
>>> silver.load(
...     source_df=bronze_df,
...     target_table="orders",
...     key_columns=["order_id"],
...     partition_keys=["year", "month"],
...     not_null_columns=["order_id", "customer_id"],
...     cast_map={"amount": "double", "order_date": "date"},
... )
>>>
>>> # Gold: SCD2 dimension or aggregate fact
>>> gold = GoldLoader(spark, database="gold")
>>> gold.load_dimension(
...     source_df=silver_df,
...     target_table="dim_orders",
...     key_columns=["order_id"],
...     tracked_columns=["status", "amount"],
... )
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Dict, List, Optional, TYPE_CHECKING

from .loader import DataLoader, LoadType
from .schema import SchemaManager
from .scd import SCDHandler
from .utils import get_logger, DataEngineeringError

if TYPE_CHECKING:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.types import StructType
    from .config import ADLSConfig

log = get_logger(__name__)

_BRONZE_META_COLS = ["_ingest_ts", "_source_tag", "_run_id", "_batch_id"]


class BronzeLoader:
    """
    Raw ingestion layer — land data exactly as received.

    Responsibilities:
        ✓ Append-only writes (never overwrites raw data)
        ✓ Inject audit metadata columns
        ✓ No schema enforcement — schema-on-read
        ✓ Optional ADLS path writing alongside Hive table registration

    Example
    -------
    >>> bronze = BronzeLoader(spark, database="bronze")
    >>> bronze.ingest(raw_df, "orders_raw", source_tag="orders/2024/06/01/")
    """

    def __init__(
        self,
        spark: "SparkSession",
        database: str = "bronze",
        run_id: Optional[str] = None,
    ):
        self.spark    = spark
        self.database = database
        self.run_id   = run_id or str(uuid.uuid4())[:8]
        self._loader  = DataLoader(spark, database)

    def ingest(
        self,
        df: "DataFrame",
        target_table: str,
        source_tag: str = "",
        batch_id: Optional[str] = None,
        add_metadata: bool = True,
        partition_by: Optional[List[str]] = None,
        mode: str = "append",
    ) -> int:
        """
        Land *df* into the bronze table. Returns the number of rows written.

        Parameters
        ----------
        source_tag  : free-text label for the data source (file path, API name, etc.)
        batch_id    : optional batch identifier (auto-generated if omitted)
        add_metadata: inject _ingest_ts, _source_tag, _run_id, _batch_id columns
        """
        from pyspark.sql import functions as F

        if add_metadata:
            df = (
                df.withColumn("_ingest_ts",  F.current_timestamp())
                  .withColumn("_source_tag", F.lit(source_tag))
                  .withColumn("_run_id",     F.lit(self.run_id))
                  .withColumn("_batch_id",   F.lit(batch_id or datetime.utcnow().strftime("%Y%m%d%H%M%S")))
            )

        row_count = df.count()
        fqn = f"`{self.database}`.`{target_table}`"
        log.info("Bronze INGEST → %s  rows=%d  source=%s", fqn, row_count, source_tag)

        writer = df.write.mode(mode)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.saveAsTable(fqn)
        return row_count

    def ingest_from_adls(
        self,
        adls_connector,
        adls_path: str,
        target_table: str,
        file_format: str = "parquet",
        **kwargs,
    ) -> int:
        """Read from ADLS and ingest into bronze in one step."""
        readers = {
            "parquet": adls_connector.read_parquet,
            "csv":     adls_connector.read_csv,
            "json":    adls_connector.read_json,
            "delta":   adls_connector.read_delta,
        }
        reader = readers.get(file_format.lower())
        if not reader:
            raise DataEngineeringError(f"Unsupported file_format: {file_format}")
        df = reader(adls_path)
        return self.ingest(df, target_table, source_tag=adls_path, **kwargs)


class SilverLoader:
    """
    Cleansed / conformed layer.

    Responsibilities:
        ✓ Schema enforcement (reject or coerce mismatched types)
        ✓ Deduplication on key columns
        ✓ Null checks with configurable rejection behaviour
        ✓ Type casting
        ✓ Incremental or partition-key loads
        ✓ Rejected rows written to a _rejected sidecar table

    Example
    -------
    >>> silver = SilverLoader(spark, database="silver")
    >>> silver.load(
    ...     source_df=raw_df,
    ...     target_table="orders",
    ...     key_columns=["order_id"],
    ...     partition_keys=["year", "month"],
    ...     not_null_columns=["order_id"],
    ...     cast_map={"amount": "double"},
    ...     dedup=True,
    ... )
    """

    def __init__(
        self,
        spark: "SparkSession",
        database: str = "silver",
        reject_action: str = "sidecar",   # "sidecar" | "fail" | "drop"
    ):
        self.spark         = spark
        self.database      = database
        self.reject_action = reject_action
        self._loader       = DataLoader(spark, database)
        self._schema_mgr   = SchemaManager(spark)

    def load(
        self,
        source_df: "DataFrame",
        target_table: str,
        key_columns: Optional[List[str]] = None,
        partition_keys: Optional[List[str]] = None,
        not_null_columns: Optional[List[str]] = None,
        cast_map: Optional[Dict[str, str]] = None,
        dedup: bool = True,
        target_schema: Optional["StructType"] = None,
        load_type: LoadType = LoadType.PARTITION,
        watermark_column: Optional[str] = None,
        last_watermark: Optional[str] = None,
    ) -> Dict:
        """
        Cleanse and load data into the silver layer.
        Returns a summary dict with row counts.
        """
        from pyspark.sql import functions as F

        df = source_df
        total_in = df.count()
        rejected_count = 0

        # 1. Type casting
        if cast_map:
            df = self._schema_mgr.cast_columns(df, cast_map)
            log.info("Silver: applied %d casts", len(cast_map))

        # 2. Schema alignment
        if target_schema:
            df = self._schema_mgr.align_to_schema(df, target_schema)

        # 3. Null rejection
        if not_null_columns:
            null_mask = " OR ".join(f"`{c}` IS NULL" for c in not_null_columns)
            rejected = df.filter(null_mask)
            df = df.filter(f"NOT ({null_mask})")
            rejected_count = rejected.count()
            if rejected_count > 0:
                self._handle_rejected(rejected, target_table)

        # 4. Deduplication
        if dedup and key_columns:
            from pyspark.sql.window import Window
            window = Window.partitionBy(*key_columns).orderBy(
                F.col(watermark_column).desc() if watermark_column else F.lit(1)
            )
            df = (df.withColumn("__rn__", F.row_number().over(window))
                    .filter("__rn__ = 1")
                    .drop("__rn__"))
            log.info("Silver: deduplication applied on keys %s", key_columns)

        # 5. Load
        if load_type == LoadType.PARTITION and partition_keys:
            self._loader.partition_load(df, target_table, partition_keys)
        elif load_type == LoadType.INCREMENTAL and watermark_column:
            self._loader.incremental_load(df, target_table, watermark_column, last_watermark)
        elif load_type == LoadType.MERGE and key_columns:
            self._loader.merge_load(df, target_table, key_columns)
        else:
            self._loader.full_load(df, target_table, partition_by=partition_keys)

        rows_out = df.count()
        log.info("Silver LOAD %s.%s — in=%d rejected=%d out=%d",
                 self.database, target_table, total_in, rejected_count, rows_out)
        return {"rows_in": total_in, "rows_rejected": rejected_count, "rows_out": rows_out}

    def _handle_rejected(self, rejected_df: "DataFrame", source_table: str) -> None:
        from pyspark.sql import functions as F
        rejected_df = rejected_df.withColumn("_rejected_at", F.current_timestamp())
        reject_table = f"`{self.database}`.`{source_table}_rejected`"

        if self.reject_action == "fail":
            raise DataEngineeringError(
                f"Silver validation failed: {rejected_df.count()} rows rejected from {source_table}"
            )
        elif self.reject_action == "sidecar":
            rejected_df.write.mode("append").saveAsTable(reject_table)
            log.warning("Silver: %d rejected rows written to %s",
                        rejected_df.count(), reject_table)
        else:  # "drop"
            log.warning("Silver: %d rows dropped (null check failed)", rejected_df.count())


class GoldLoader:
    """
    Aggregated / dimensional layer with built-in SCD and DQ.

    Responsibilities:
        ✓ SCD Type 1 or 2 for dimensions
        ✓ Full or merge loads for facts
        ✓ Optional inline DQ check before write
        ✓ Partition pruning for large fact tables

    Example — dimension
    -------------------
    >>> gold = GoldLoader(spark, database="gold")
    >>> gold.load_dimension(
    ...     source_df=silver_df,
    ...     target_table="dim_customer",
    ...     key_columns=["customer_id"],
    ...     tracked_columns=["email", "address", "tier"],
    ...     scd_type=2,
    ... )

    Example — fact
    --------------
    >>> gold.load_fact(
    ...     source_df=agg_df,
    ...     target_table="fact_daily_sales",
    ...     match_keys=["sale_date", "product_id"],
    ...     partition_keys=["year", "month"],
    ... )
    """

    def __init__(self, spark: "SparkSession", database: str = "gold"):
        self.spark    = spark
        self.database = database
        self._loader  = DataLoader(spark, database)
        self._scd     = SCDHandler(spark, database)

    def load_dimension(
        self,
        source_df: "DataFrame",
        target_table: str,
        key_columns: List[str],
        tracked_columns: Optional[List[str]] = None,
        scd_type: int = 2,
        dq_rules: Optional[List] = None,
        effective_date: Optional[str] = None,
    ) -> None:
        """
        Load a slowly-changing dimension into the gold layer.

        Parameters
        ----------
        scd_type : 1 (overwrite) or 2 (full history)
        dq_rules : list of Rule objects to validate before writing
        """
        if dq_rules:
            self._run_dq(source_df, target_table, dq_rules)

        log.info("Gold DIMENSION %s.%s (SCD%d)", self.database, target_table, scd_type)
        if scd_type == 1:
            self._scd.apply_scd1(target_table, source_df, key_columns,
                                  update_columns=tracked_columns)
        elif scd_type == 2:
            self._scd.apply_scd2(target_table, source_df, key_columns,
                                  tracked_columns=tracked_columns,
                                  effective_date=effective_date)
        elif scd_type == 6:
            self._scd.apply_scd6(target_table, source_df, key_columns,
                                  tracked_columns=tracked_columns or [],
                                  effective_date=effective_date)
        else:
            raise DataEngineeringError(f"scd_type must be 1, 2, or 6. Got: {scd_type}")

    def load_fact(
        self,
        source_df: "DataFrame",
        target_table: str,
        match_keys: Optional[List[str]] = None,
        partition_keys: Optional[List[str]] = None,
        load_type: LoadType = LoadType.MERGE,
        dq_rules: Optional[List] = None,
    ) -> None:
        """
        Load a fact table into the gold layer.

        Parameters
        ----------
        load_type : FULL, MERGE, or PARTITION
        dq_rules  : list of Rule objects to validate before writing
        """
        if dq_rules:
            self._run_dq(source_df, target_table, dq_rules)

        log.info("Gold FACT %s.%s (%s)", self.database, target_table, load_type.value)
        self._loader.load(
            source_df, target_table, load_type,
            match_keys=match_keys,
            partition_keys=partition_keys,
        )

    def _run_dq(self, df: "DataFrame", table: str, rules: List) -> None:
        from .quality import DataQualityChecker
        qc = DataQualityChecker(self.spark, table_name=f"{self.database}.{table}")
        qc.add_rules(rules)
        report = qc.run(df)
        report.print_summary()
        report.assert_no_failures()
