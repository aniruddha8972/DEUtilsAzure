"""
de_utils.table_ops
------------------
Core DML operations: create_or_replace, insert, update, delete,
and generic merge/upsert for Spark tables (Delta and Hive).
"""

from __future__ import annotations

from typing import Dict, List, Optional, TYPE_CHECKING

from .utils import get_logger, DataEngineeringError, quote_identifier, validate_columns

if TYPE_CHECKING:
    from pyspark.sql import SparkSession, DataFrame

log = get_logger(__name__)


class TableOperations:
    """
    High-level CREATE / INSERT / UPDATE / DELETE / MERGE operations for
    Spark-managed tables.

    Example
    -------
    >>> from de_utils import TableOperations
    >>> ops = TableOperations(spark, database="silver")
    >>> ops.create_or_replace("orders", source_df)
    >>> ops.upsert(
    ...     target_table="orders",
    ...     source_df=updates_df,
    ...     match_keys=["order_id"],
    ... )
    """

    def __init__(self, spark: "SparkSession", database: str = "default"):
        self.spark = spark
        self.database = database

    def _fqn(self, table: str) -> str:
        return f"{quote_identifier(self.database)}.{quote_identifier(table)}"

    # ------------------------------------------------------------------
    # CREATE helpers
    # ------------------------------------------------------------------

    def create_or_replace(
        self,
        table_name: str,
        df: "DataFrame",
        partition_by: Optional[List[str]] = None,
        file_format: str = "PARQUET",
        location: Optional[str] = None,
        comment: Optional[str] = None,
    ) -> None:
        """
        Drop + recreate the table from the given DataFrame.
        Useful for full-refresh landing tables.
        """
        fqn = self._fqn(table_name)
        log.info("CREATE OR REPLACE table %s", fqn)
        writer = df.write.mode("overwrite").format(file_format)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        if location:
            writer = writer.option("path", location)
        writer.saveAsTable(fqn)
        if comment:
            self.spark.sql(f"COMMENT ON TABLE {fqn} IS '{comment}'")

    def create_if_not_exists(
        self,
        table_name: str,
        df: "DataFrame",
        partition_by: Optional[List[str]] = None,
        file_format: str = "PARQUET",
        location: Optional[str] = None,
    ) -> bool:
        """
        Create the table only if it does not already exist.
        Returns True if the table was created, False if it already existed.
        """
        fqn = self._fqn(table_name)
        try:
            self.spark.sql(f"DESCRIBE TABLE {fqn}")
            log.info("Table %s already exists — skipping create.", fqn)
            return False
        except Exception:
            self.create_or_replace(table_name, df, partition_by, file_format, location)
            return True

    # ------------------------------------------------------------------
    # INSERT helpers
    # ------------------------------------------------------------------

    def insert_overwrite(
        self,
        table_name: str,
        df: "DataFrame",
        partition_by: Optional[List[str]] = None,
        dynamic_partition: bool = True,
    ) -> None:
        """
        INSERT OVERWRITE — replaces all (or just matching partition) data.
        When *dynamic_partition* is True and *partition_by* columns are present
        in the DataFrame, Spark will overwrite only the touched partitions.
        """
        fqn = self._fqn(table_name)
        log.info("INSERT OVERWRITE into %s (partitions=%s)", fqn, partition_by)
        if dynamic_partition and partition_by:
            self.spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        df.write.mode("overwrite").insertInto(fqn, overwrite=True)

    def insert_append(self, table_name: str, df: "DataFrame") -> None:
        """Append rows to an existing table."""
        fqn = self._fqn(table_name)
        log.info("INSERT INTO (append) %s", fqn)
        df.write.mode("append").insertInto(fqn)

    # ------------------------------------------------------------------
    # UPDATE / DELETE (Delta only)
    # ------------------------------------------------------------------

    def update(
        self,
        table_name: str,
        condition: str,
        set_clause: Dict[str, str],
    ) -> None:
        """
        UPDATE rows in a Delta table matching *condition*.

        Example
        -------
        >>> ops.update("orders", "status = 'PENDING'", {"status": "'SHIPPED'"})
        """
        fqn = self._fqn(table_name)
        assignments = ", ".join(
            f"{quote_identifier(k)} = {v}" for k, v in set_clause.items()
        )
        sql = f"UPDATE {fqn} SET {assignments} WHERE {condition}"
        log.info("UPDATE %s WHERE %s", fqn, condition)
        self.spark.sql(sql)

    def delete(self, table_name: str, condition: str) -> None:
        """
        DELETE rows from a Delta table matching *condition*.

        Example
        -------
        >>> ops.delete("orders", "created_date < '2020-01-01'")
        """
        fqn = self._fqn(table_name)
        log.info("DELETE FROM %s WHERE %s", fqn, condition)
        self.spark.sql(f"DELETE FROM {fqn} WHERE {condition}")

    # ------------------------------------------------------------------
    # MERGE / UPSERT
    # ------------------------------------------------------------------

    def merge(
        self,
        target_table: str,
        source_df: "DataFrame",
        match_keys: List[str],
        update_columns: Optional[List[str]] = None,
        insert_when_not_matched: bool = True,
        delete_when_not_matched_by_source: bool = False,
        source_alias: str = "s",
        target_alias: str = "t",
    ) -> None:
        """
        MERGE source_df INTO target_table on *match_keys*.

        - Matched rows  → UPDATE the specified (or all non-key) columns.
        - Not matched   → INSERT (if insert_when_not_matched=True).
        - Not matched by source → DELETE (Delta only, optional).

        Example
        -------
        >>> ops.merge(
        ...     target_table="orders",
        ...     source_df=updates_df,
        ...     match_keys=["order_id"],
        ...     update_columns=["status", "updated_at"],
        ... )
        """
        fqn = self._fqn(target_table)
        validate_columns(source_df.columns, match_keys, "source_df")

        tmp_view = f"__de_merge_src_{target_table}__"
        source_df.createOrReplaceTempView(tmp_view)

        # Build ON clause
        on_clause = " AND ".join(
            f"{target_alias}.{quote_identifier(k)} = {source_alias}.{quote_identifier(k)}"
            for k in match_keys
        )

        # Determine columns to update
        all_cols = source_df.columns
        if update_columns is None:
            update_columns = [c for c in all_cols if c not in match_keys]

        set_clause = ", ".join(
            f"{target_alias}.{quote_identifier(c)} = {source_alias}.{quote_identifier(c)}"
            for c in update_columns
        )

        # Build INSERT column/value lists
        insert_cols = ", ".join(quote_identifier(c) for c in all_cols)
        insert_vals = ", ".join(f"{source_alias}.{quote_identifier(c)}" for c in all_cols)

        sql = (
            f"MERGE INTO {fqn} AS {target_alias}\n"
            f"USING {tmp_view} AS {source_alias}\n"
            f"ON {on_clause}\n"
            f"WHEN MATCHED THEN UPDATE SET {set_clause}\n"
        )
        if insert_when_not_matched:
            sql += f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})\n"
        if delete_when_not_matched_by_source:
            sql += "WHEN NOT MATCHED BY SOURCE THEN DELETE\n"

        log.info("MERGE INTO %s on keys=%s", fqn, match_keys)
        self.spark.sql(sql)

    # Alias for convenience
    def upsert(self, target_table: str, source_df: "DataFrame", match_keys: List[str], **kwargs) -> None:
        """Alias for merge() — upsert semantics (update + insert)."""
        self.merge(target_table, source_df, match_keys, **kwargs)
