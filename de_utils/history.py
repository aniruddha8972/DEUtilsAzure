"""
de_utils.history
----------------
History table management — capture every version of a row over time.
Supports append-only (insert + expire) and snapshot-based patterns.
"""

from __future__ import annotations

from datetime import datetime
from typing import List, Optional, TYPE_CHECKING

from .utils import get_logger, DataEngineeringError, validate_columns, quote_identifier

if TYPE_CHECKING:
    from pyspark.sql import SparkSession, DataFrame

log = get_logger(__name__)

# Standard history metadata columns
HIST_COLS = {
    "valid_from": "_valid_from",
    "valid_to": "_valid_to",
    "is_current": "_is_current",
    "row_hash": "_row_hash",
    "load_timestamp": "_load_timestamp",
    "operation": "_operation",  # INSERT / UPDATE / DELETE
}


class HistoryTable:
    """
    Manage a history (audit) table that preserves all versions of each row.

    The history table mirrors the source table's columns and adds:
        _valid_from      TIMESTAMP  — when this version became active
        _valid_to        TIMESTAMP  — when it was superseded (NULL = current)
        _is_current      BOOLEAN    — convenience flag
        _row_hash        STRING     — MD5/SHA hash of tracked columns
        _load_timestamp  TIMESTAMP  — when this row was written to the history table
        _operation       STRING     — INSERT | UPDATE | DELETE

    Example
    -------
    >>> from de_utils import HistoryTable
    >>> hist = HistoryTable(spark, database="silver", history_suffix="_hist")
    >>> hist.init_history_table("orders", source_df, key_columns=["order_id"])
    >>> # On subsequent runs:
    >>> hist.capture(source_df, "orders", key_columns=["order_id"])
    """

    def __init__(
        self,
        spark: "SparkSession",
        database: str = "default",
        history_suffix: str = "_hist",
        valid_from_col: str = "_valid_from",
        valid_to_col: str = "_valid_to",
        is_current_col: str = "_is_current",
        row_hash_col: str = "_row_hash",
        load_ts_col: str = "_load_timestamp",
        operation_col: str = "_operation",
        sentinel_date: str = "9999-12-31 23:59:59",
    ):
        self.spark = spark
        self.database = database
        self.history_suffix = history_suffix
        self.valid_from_col = valid_from_col
        self.valid_to_col = valid_to_col
        self.is_current_col = is_current_col
        self.row_hash_col = row_hash_col
        self.load_ts_col = load_ts_col
        self.operation_col = operation_col
        self.sentinel_date = sentinel_date

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _hist_table(self, table_name: str) -> str:
        return f"{table_name}{self.history_suffix}"

    def _fqn(self, table: str) -> str:
        return f"{quote_identifier(self.database)}.{quote_identifier(table)}"

    def _add_metadata(
        self,
        df: "DataFrame",
        operation: str,
        valid_from: Optional[str] = None,
    ) -> "DataFrame":
        """Attach history metadata columns to a DataFrame."""
        from pyspark.sql import functions as F

        now = valid_from or datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        return (
            df.withColumn(self.valid_from_col, F.lit(now).cast("timestamp"))
            .withColumn(self.valid_to_col, F.lit(self.sentinel_date).cast("timestamp"))
            .withColumn(self.is_current_col, F.lit(True))
            .withColumn(self.load_ts_col, F.current_timestamp())
            .withColumn(self.operation_col, F.lit(operation))
        )

    def _hash_columns(self, df: "DataFrame", columns: List[str]) -> "DataFrame":
        """Add a row hash column computed over *columns*."""
        from pyspark.sql import functions as F

        hash_expr = F.md5(F.concat_ws("||", *[F.col(c).cast("string") for c in columns]))
        return df.withColumn(self.row_hash_col, hash_expr)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def init_history_table(
        self,
        base_table: str,
        source_df: "DataFrame",
        key_columns: List[str],
        tracked_columns: Optional[List[str]] = None,
        file_format: str = "PARQUET",
        location: Optional[str] = None,
        valid_from: Optional[str] = None,
    ) -> None:
        """
        Create the history table (if it doesn't exist) and load the initial snapshot.
        Idempotent — safe to call on first run.
        """
        hist_table = self._hist_table(base_table)
        hist_fqn = self._fqn(hist_table)

        try:
            self.spark.sql(f"DESCRIBE TABLE {hist_fqn}")
            log.info("History table %s already exists — skipping init.", hist_fqn)
            return
        except Exception:
            pass

        tracked = tracked_columns or [c for c in source_df.columns if c not in key_columns]
        df = self._hash_columns(source_df, tracked)
        df = self._add_metadata(df, operation="INSERT", valid_from=valid_from)

        writer = df.write.mode("overwrite").format(file_format)
        if location:
            writer = writer.option("path", location)
        writer.saveAsTable(hist_fqn)
        log.info("Initialized history table %s (%d rows)", hist_fqn, source_df.count())

    def capture(
        self,
        source_df: "DataFrame",
        base_table: str,
        key_columns: List[str],
        tracked_columns: Optional[List[str]] = None,
        valid_from: Optional[str] = None,
    ) -> "HistoryCaptureResult":
        """
        Compare *source_df* against the current history snapshot and write:
          - New INSERTs for newly appeared keys
          - New UPDATE records (with expired previous version) for changed rows
          - DELETE records for keys that disappeared from source

        Returns a HistoryCaptureResult with counts of each operation.
        """
        from pyspark.sql import functions as F

        hist_table = self._hist_table(base_table)
        hist_fqn = self._fqn(hist_table)
        validate_columns(source_df.columns, key_columns, "source_df")

        tracked = tracked_columns or [c for c in source_df.columns if c not in key_columns]
        now = valid_from or datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        # Read only currently-active history rows
        current_hist = self.spark.table(hist_fqn).filter(F.col(self.is_current_col) == True)

        # Compute hashes on source
        src_hashed = self._hash_columns(source_df, tracked)

        # Build join condition
        join_cond = " AND ".join(
            f"src.`{k}` = hist.`{k}`" for k in key_columns
        )

        src_hashed.createOrReplaceTempView("__hist_src__")
        current_hist.createOrReplaceTempView("__hist_cur__")

        # --- NEW rows (key not in history) ---
        new_rows_df = self.spark.sql(f"""
            SELECT src.*
            FROM __hist_src__ src
            LEFT ANTI JOIN __hist_cur__ hist ON {join_cond}
        """)
        new_count = new_rows_df.count()

        # --- CHANGED rows (key exists but hash differs) ---
        changed_df = self.spark.sql(f"""
            SELECT src.*
            FROM __hist_src__ src
            JOIN __hist_cur__ hist ON {join_cond}
            WHERE src.`{self.row_hash_col}` != hist.`{self.row_hash_col}`
        """)
        changed_count = changed_df.count()

        # --- DELETED rows (key in history but not in source) ---
        deleted_df = self.spark.sql(f"""
            SELECT hist.*
            FROM __hist_cur__ hist
            LEFT ANTI JOIN __hist_src__ src ON {join_cond}
        """)
        deleted_count = deleted_df.count()

        # Write new INSERT rows
        if new_count > 0:
            df_ins = self._add_metadata(new_rows_df, "INSERT", now)
            df_ins.write.mode("append").insertInto(hist_fqn)
            log.info("History: %d INSERT rows written to %s", new_count, hist_fqn)

        # Write new UPDATE rows + expire old ones
        if changed_count > 0:
            df_upd = self._add_metadata(changed_df, "UPDATE", now)
            df_upd.write.mode("append").insertInto(hist_fqn)
            # Expire old active rows for changed keys
            key_filter = " AND ".join(
                f"`{k}` IN (SELECT `{k}` FROM __hist_src__ src JOIN __hist_cur__ hist ON {join_cond} WHERE src.`{self.row_hash_col}` != hist.`{self.row_hash_col}`)"
                for k in key_columns[:1]  # use first key as proxy for the subquery
            )
            expire_sql = f"""
                UPDATE {hist_fqn}
                SET `{self.valid_to_col}` = CAST('{now}' AS TIMESTAMP),
                    `{self.is_current_col}` = FALSE
                WHERE `{self.is_current_col}` = TRUE
                  AND {key_filter}
            """
            try:
                self.spark.sql(expire_sql)
            except Exception:
                log.warning("Could not expire old rows (UPDATE not supported on this table format); "
                            "consider using Delta format for full SCD-2 history.")
            log.info("History: %d UPDATE rows written to %s", changed_count, hist_fqn)

        # Write DELETE marker rows
        if deleted_count > 0:
            df_del = self._add_metadata(
                deleted_df.drop(self.valid_from_col, self.valid_to_col, self.is_current_col,
                                self.load_ts_col, self.operation_col),
                "DELETE", now,
            ).withColumn(self.valid_to_col, F.lit(now).cast("timestamp")) \
             .withColumn(self.is_current_col, F.lit(False))
            df_del.write.mode("append").insertInto(hist_fqn)
            log.info("History: %d DELETE rows written to %s", deleted_count, hist_fqn)

        return HistoryCaptureResult(
            table=hist_table,
            inserts=new_count,
            updates=changed_count,
            deletes=deleted_count,
            captured_at=datetime.utcnow(),
        )

    def get_version_at(
        self,
        base_table: str,
        as_of: str,
    ) -> "DataFrame":
        """
        Return rows that were active at the given timestamp.

        Example
        -------
        >>> snapshot = hist.get_version_at("orders", "2024-06-01 00:00:00")
        """
        from pyspark.sql import functions as F

        hist_fqn = self._fqn(self._hist_table(base_table))
        return (
            self.spark.table(hist_fqn)
            .filter(
                (F.col(self.valid_from_col) <= F.lit(as_of).cast("timestamp"))
                & (F.col(self.valid_to_col) > F.lit(as_of).cast("timestamp"))
            )
        )

    def get_full_history(self, base_table: str) -> "DataFrame":
        """Return the complete history table."""
        return self.spark.table(self._fqn(self._hist_table(base_table)))


class HistoryCaptureResult:
    def __init__(self, table, inserts, updates, deletes, captured_at):
        self.table = table
        self.inserts = inserts
        self.updates = updates
        self.deletes = deletes
        self.captured_at = captured_at

    def __repr__(self):
        return (
            f"HistoryCaptureResult(table={self.table!r}, "
            f"inserts={self.inserts}, updates={self.updates}, deletes={self.deletes}, "
            f"captured_at={self.captured_at.isoformat()})"
        )
