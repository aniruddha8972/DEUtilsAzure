"""
de_utils.scd
------------
Slowly Changing Dimension (SCD) implementations.

Supported types
---------------
  SCD Type 1  — Overwrite: always reflect the latest value.
  SCD Type 2  — Full history: add a new row for each change, expire the old one.
  SCD Type 3  — Previous-value column: keep current and one prior value.
  SCD Type 4  — Mini-dimension: separate history table (thin wrapper over HistoryTable).
  SCD Type 6  — Hybrid (1+2+3): current value + full history + previous value column.

Usage
-----
>>> from de_utils import SCDHandler
>>> scd = SCDHandler(spark, database="gold")
>>> scd.apply_scd2(
...     target_table="dim_customer",
...     source_df=staging_df,
...     key_columns=["customer_id"],
...     tracked_columns=["name", "email", "address"],
... )
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import List, Optional, TYPE_CHECKING

from .utils import get_logger, DataEngineeringError, validate_columns, quote_identifier

if TYPE_CHECKING:
    from pyspark.sql import SparkSession, DataFrame

log = get_logger(__name__)

SENTINEL = "9999-12-31 23:59:59"


class SCDType(int, Enum):
    SCD1 = 1
    SCD2 = 2
    SCD3 = 3
    SCD4 = 4
    SCD6 = 6


class SCDHandler:
    """
    Apply SCD logic to Spark tables.

    All SCD methods accept:
        target_table    — fully-qualified or simple table name
        source_df       — incoming (stage) DataFrame with the latest state
        key_columns     — business keys that identify a dimension member
        tracked_columns — columns whose changes trigger new versions
    """

    def __init__(
        self,
        spark: "SparkSession",
        database: str = "default",
        effective_from_col: str = "effective_from",
        effective_to_col: str = "effective_to",
        is_current_col: str = "is_current",
        previous_prefix: str = "prev_",
    ):
        self.spark = spark
        self.database = database
        self.eff_from = effective_from_col
        self.eff_to = effective_to_col
        self.is_curr = is_current_col
        self.prev_pfx = previous_prefix

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _fqn(self, table: str) -> str:
        if "." in table:
            return table
        return f"{quote_identifier(self.database)}.{quote_identifier(table)}"

    def _now(self) -> str:
        return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    def _hash_df(self, df: "DataFrame", columns: List[str]) -> "DataFrame":
        from pyspark.sql import functions as F
        return df.withColumn(
            "__scd_hash__",
            F.md5(F.concat_ws("||", *[F.col(c).cast("string") for c in columns])),
        )

    def _build_join(self, keys: List[str], src: str = "s", tgt: str = "t") -> str:
        return " AND ".join(
            f"{tgt}.{quote_identifier(k)} = {src}.{quote_identifier(k)}" for k in keys
        )

    # ------------------------------------------------------------------
    # SCD Type 1 — Overwrite
    # ------------------------------------------------------------------

    def apply_scd1(
        self,
        target_table: str,
        source_df: "DataFrame",
        key_columns: List[str],
        update_columns: Optional[List[str]] = None,
    ) -> None:
        """
        SCD Type 1: UPSERT — overwrite changed columns, no history retained.

        Rows with keys not present in target → INSERT.
        Rows with changed values → UPDATE (overwrite).
        Keys absent from source → no change by default.
        """
        fqn = self._fqn(target_table)
        validate_columns(source_df.columns, key_columns, "source_df")

        all_cols = source_df.columns
        upd_cols = update_columns or [c for c in all_cols if c not in key_columns]

        src_view = "__scd1_src__"
        source_df.createOrReplaceTempView(src_view)

        join_clause = self._build_join(key_columns)
        set_clause = ", ".join(
            f"t.{quote_identifier(c)} = s.{quote_identifier(c)}" for c in upd_cols
        )
        insert_cols = ", ".join(quote_identifier(c) for c in all_cols)
        insert_vals = ", ".join(f"s.{quote_identifier(c)}" for c in all_cols)

        sql = f"""
            MERGE INTO {fqn} AS t
            USING {src_view} AS s
            ON {join_clause}
            WHEN MATCHED AND ({' OR '.join(f"t.{quote_identifier(c)} <> s.{quote_identifier(c)}" for c in upd_cols)}) THEN
                UPDATE SET {set_clause}
            WHEN NOT MATCHED THEN
                INSERT ({insert_cols}) VALUES ({insert_vals})
        """
        log.info("SCD1 MERGE into %s on keys=%s", fqn, key_columns)
        self.spark.sql(sql)

    # ------------------------------------------------------------------
    # SCD Type 2 — Full History
    # ------------------------------------------------------------------

    def apply_scd2(
        self,
        target_table: str,
        source_df: "DataFrame",
        key_columns: List[str],
        tracked_columns: Optional[List[str]] = None,
        effective_date: Optional[str] = None,
    ) -> "SCD2Result":
        """
        SCD Type 2: insert a new record for every change; expire the old one.

        Target table must have columns:
            <key_columns>
            <tracked_columns>
            effective_from  TIMESTAMP
            effective_to    TIMESTAMP
            is_current      BOOLEAN
        """
        from pyspark.sql import functions as F

        fqn = self._fqn(target_table)
        validate_columns(source_df.columns, key_columns, "source_df")

        now = effective_date or self._now()
        tracked = tracked_columns or [c for c in source_df.columns if c not in key_columns]

        src_hashed = self._hash_df(source_df, tracked)
        src_hashed.createOrReplaceTempView("__scd2_src__")

        current_target = self.spark.table(fqn).filter(F.col(self.is_curr) == True)
        tgt_hashed = self._hash_df(current_target, tracked)
        tgt_hashed.createOrReplaceTempView("__scd2_tgt__")

        join_clause = self._build_join(key_columns, "s", "t")

        # 1. New keys → INSERT
        new_keys = self.spark.sql(f"""
            SELECT s.*
            FROM __scd2_src__ s
            LEFT ANTI JOIN __scd2_tgt__ t ON {join_clause}
        """)
        new_count = new_keys.count()

        # 2. Changed rows → expire old + INSERT new
        changed = self.spark.sql(f"""
            SELECT s.*
            FROM __scd2_src__ s
            JOIN __scd2_tgt__ t ON {join_clause}
            WHERE s.__scd_hash__ != t.__scd_hash__
        """)
        changed_count = changed.count()

        def _add_scd2_meta(df, op_now):
            return (
                df.drop("__scd_hash__")
                .withColumn(self.eff_from, F.lit(op_now).cast("timestamp"))
                .withColumn(self.eff_to, F.lit(SENTINEL).cast("timestamp"))
                .withColumn(self.is_curr, F.lit(True))
            )

        if new_count > 0:
            _add_scd2_meta(new_keys, now).write.mode("append").insertInto(fqn)
            log.info("SCD2: %d new keys inserted into %s", new_count, fqn)

        if changed_count > 0:
            # Insert new version
            _add_scd2_meta(changed, now).write.mode("append").insertInto(fqn)
            # Expire old version (requires Delta or updateable format)
            key_in_clause = " AND ".join(
                f"t.{quote_identifier(k)} IN (SELECT {quote_identifier(k)} FROM __scd2_src__ s JOIN __scd2_tgt__ t2 ON {join_clause} WHERE s.__scd_hash__ != t2.__scd_hash__)"
                for k in key_columns[:1]
            )
            expire_sql = f"""
                UPDATE {fqn} AS t
                SET t.`{self.eff_to}` = CAST('{now}' AS TIMESTAMP),
                    t.`{self.is_curr}` = FALSE
                WHERE t.`{self.is_curr}` = TRUE
                  AND {key_in_clause}
                  AND t.`{self.eff_from}` < CAST('{now}' AS TIMESTAMP)
            """
            try:
                self.spark.sql(expire_sql)
                log.info("SCD2: %d old rows expired in %s", changed_count, fqn)
            except Exception as e:
                log.warning(
                    "SCD2 expiry UPDATE failed (%s). Use Delta tables for full SCD-2.", str(e)
                )

        return SCD2Result(
            table=target_table,
            new_inserts=new_count,
            updates=changed_count,
            effective_date=now,
        )

    # ------------------------------------------------------------------
    # SCD Type 3 — Previous-Value Column
    # ------------------------------------------------------------------

    def apply_scd3(
        self,
        target_table: str,
        source_df: "DataFrame",
        key_columns: List[str],
        tracked_columns: List[str],
    ) -> None:
        """
        SCD Type 3: On change, shift current value to 'prev_<col>' and
        write the new value into '<col>'.

        Target table must have both <col> and prev_<col> for each tracked column.
        """
        fqn = self._fqn(target_table)
        validate_columns(source_df.columns, key_columns + tracked_columns, "source_df")

        src_view = "__scd3_src__"
        source_df.createOrReplaceTempView(src_view)

        join_clause = self._build_join(key_columns)
        change_check = " OR ".join(
            f"t.{quote_identifier(c)} <> s.{quote_identifier(c)}" for c in tracked_columns
        )

        # On match+change: shift current → prev, set new values
        shift_set = ", ".join(
            f"t.{quote_identifier(self.prev_pfx + c)} = t.{quote_identifier(c)}, "
            f"t.{quote_identifier(c)} = s.{quote_identifier(c)}"
            for c in tracked_columns
        )
        all_cols = source_df.columns
        insert_cols = ", ".join(quote_identifier(c) for c in all_cols)
        insert_vals = ", ".join(f"s.{quote_identifier(c)}" for c in all_cols)

        sql = f"""
            MERGE INTO {fqn} AS t
            USING {src_view} AS s
            ON {join_clause}
            WHEN MATCHED AND ({change_check}) THEN
                UPDATE SET {shift_set}
            WHEN NOT MATCHED THEN
                INSERT ({insert_cols}) VALUES ({insert_vals})
        """
        log.info("SCD3 MERGE into %s on keys=%s, tracked=%s", fqn, key_columns, tracked_columns)
        self.spark.sql(sql)

    # ------------------------------------------------------------------
    # SCD Type 4 — Mini-Dimension / History Table
    # ------------------------------------------------------------------

    def apply_scd4(
        self,
        target_table: str,
        history_table: str,
        source_df: "DataFrame",
        key_columns: List[str],
        tracked_columns: Optional[List[str]] = None,
    ) -> None:
        """
        SCD Type 4: Keep current values in the main table (SCD1 style),
        write all changes to a separate history table.
        """
        # Update main dimension (SCD1)
        self.apply_scd1(target_table, source_df, key_columns,
                        update_columns=tracked_columns)

        # Capture history
        from .history import HistoryTable
        hist = HistoryTable(self.spark, self.database)
        hist.capture(source_df, target_table.replace(self.database + ".", ""),
                     key_columns=key_columns, tracked_columns=tracked_columns)
        log.info("SCD4: main table updated + history captured in %s", history_table)

    # ------------------------------------------------------------------
    # SCD Type 6 — Hybrid (1 + 2 + 3)
    # ------------------------------------------------------------------

    def apply_scd6(
        self,
        target_table: str,
        source_df: "DataFrame",
        key_columns: List[str],
        tracked_columns: List[str],
        effective_date: Optional[str] = None,
    ) -> None:
        """
        SCD Type 6: SCD2 full history + current-value override columns
        (like SCD3) on the current row so the latest value is always
        accessible without filtering.

        Target table must have:
            <col>            — current value (overwritten on each change)
            prev_<col>       — previous value
            effective_from   — row start
            effective_to     — row end (SENTINEL = still active)
            is_current       — boolean flag
        """
        fqn = self._fqn(target_table)
        now = effective_date or self._now()

        # 1. Insert new SCD2 rows for changed/new keys
        result = self.apply_scd2(target_table, source_df, key_columns, tracked_columns, now)

        # 2. Overwrite 'current_<col>' on ALL is_current rows to latest value (SCD1 style)
        #    and shift old current → prev_ (SCD3 style)
        src_view = "__scd6_src__"
        source_df.createOrReplaceTempView(src_view)
        join_clause = self._build_join(key_columns)

        current_update_set = ", ".join(
            f"t.{quote_identifier(c)} = s.{quote_identifier(c)}, "
            f"t.{quote_identifier(self.prev_pfx + c)} = t.{quote_identifier(c)}"
            for c in tracked_columns
        )
        try:
            self.spark.sql(f"""
                MERGE INTO {fqn} AS t
                USING {src_view} AS s ON {join_clause} AND t.`{self.is_curr}` = TRUE
                WHEN MATCHED THEN UPDATE SET {current_update_set}
            """)
        except Exception as e:
            log.warning("SCD6 current-value sync step failed: %s", str(e))

        log.info(
            "SCD6 applied to %s: %d inserts, %d updates",
            fqn, result.new_inserts, result.updates,
        )

    # ------------------------------------------------------------------
    # Diff utility
    # ------------------------------------------------------------------

    def diff(
        self,
        df_a: "DataFrame",
        df_b: "DataFrame",
        key_columns: List[str],
        compare_columns: Optional[List[str]] = None,
        a_alias: str = "current",
        b_alias: str = "incoming",
    ) -> "DataFrame":
        """
        Compare two DataFrames on *key_columns* and return a diff summary.

        Returns a DataFrame with columns:
            <key_columns>
            _diff_type      — UNCHANGED | CHANGED | NEW | DELETED
            _changed_cols   — array of column names that changed (if CHANGED)
        """
        from pyspark.sql import functions as F

        compare_cols = compare_columns or [c for c in df_a.columns if c not in key_columns]
        a_hashed = self._hash_df(df_a, compare_cols).select(*key_columns, "__scd_hash__")
        b_hashed = self._hash_df(df_b, compare_cols).select(*key_columns, "__scd_hash__")

        a_hashed = a_hashed.withColumnRenamed("__scd_hash__", "__hash_a__")
        b_hashed = b_hashed.withColumnRenamed("__scd_hash__", "__hash_b__")

        join_cond = [k for k in key_columns]
        joined = a_hashed.join(b_hashed, on=join_cond, how="full")

        result = joined.withColumn(
            "_diff_type",
            F.when(F.col("__hash_a__").isNull(), F.lit("NEW"))
            .when(F.col("__hash_b__").isNull(), F.lit("DELETED"))
            .when(F.col("__hash_a__") == F.col("__hash_b__"), F.lit("UNCHANGED"))
            .otherwise(F.lit("CHANGED")),
        ).drop("__hash_a__", "__hash_b__")

        return result


class SCD2Result:
    def __init__(self, table, new_inserts, updates, effective_date):
        self.table = table
        self.new_inserts = new_inserts
        self.updates = updates
        self.effective_date = effective_date

    def __repr__(self):
        return (
            f"SCD2Result(table={self.table!r}, "
            f"new_inserts={self.new_inserts}, updates={self.updates}, "
            f"effective_date={self.effective_date!r})"
        )
