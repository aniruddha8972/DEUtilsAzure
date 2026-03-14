"""
de_utils.schema
---------------
Schema registry, DDL generation, evolution helpers, and validation utilities.
"""

from __future__ import annotations

from typing import Dict, List, Optional, TYPE_CHECKING

from .utils import get_logger, DataEngineeringError

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

log = get_logger(__name__)


class SchemaManager:
    """
    Manage, validate, and evolve DataFrame schemas.

    Example
    -------
    >>> from de_utils import SchemaManager
    >>> sm = SchemaManager(spark)
    >>> sm.assert_schema_compatible(source_df, target_df)
    >>> evolved_df = sm.add_missing_columns(source_df, target_schema)
    """

    def __init__(self, spark):
        self.spark = spark

    # ------------------------------------------------------------------
    # Schema comparison
    # ------------------------------------------------------------------

    def diff_schemas(
        self, schema_a: "StructType", schema_b: "StructType"
    ) -> Dict[str, List[str]]:
        """
        Return a dict with keys 'added', 'dropped', 'type_changed'
        comparing schema_b relative to schema_a.
        """
        cols_a = {f.name: f.dataType for f in schema_a.fields}
        cols_b = {f.name: f.dataType for f in schema_b.fields}

        added = [c for c in cols_b if c not in cols_a]
        dropped = [c for c in cols_a if c not in cols_b]
        type_changed = [
            c for c in cols_a if c in cols_b and cols_a[c] != cols_b[c]
        ]
        return {"added": added, "dropped": dropped, "type_changed": type_changed}

    def assert_schema_compatible(
        self,
        source: "DataFrame",
        target: "DataFrame",
        ignore_nullable: bool = True,
        raise_on_missing: bool = True,
    ) -> None:
        """
        Raise DataEngineeringError if source columns are missing from target
        or if data types are incompatible.
        """
        diff = self.diff_schemas(target.schema, source.schema)
        if raise_on_missing and diff["dropped"]:
            raise DataEngineeringError(
                f"Source DataFrame is missing columns that exist in target: {diff['dropped']}"
            )
        if diff["type_changed"]:
            raise DataEngineeringError(
                f"Column type mismatch between source and target: {diff['type_changed']}"
            )
        log.info("Schema compatibility check passed.")

    # ------------------------------------------------------------------
    # Schema evolution helpers
    # ------------------------------------------------------------------

    def add_missing_columns(
        self, df: "DataFrame", target_schema: "StructType", fill_value=None
    ) -> "DataFrame":
        """
        Add columns from *target_schema* that are absent in *df*,
        filled with *fill_value* (default None / NULL).
        """
        from pyspark.sql import functions as F

        existing = set(df.columns)
        for field in target_schema.fields:
            if field.name not in existing:
                df = df.withColumn(field.name, F.lit(fill_value).cast(field.dataType))
                log.info("Added missing column '%s' (%s)", field.name, field.dataType)
        return df

    def align_to_schema(self, df: "DataFrame", target_schema: "StructType") -> "DataFrame":
        """
        Reorder and cast columns in *df* to exactly match *target_schema*.
        Adds nulls for missing columns; drops extra columns.
        """
        from pyspark.sql import functions as F

        df = self.add_missing_columns(df, target_schema)
        select_exprs = [
            F.col(f.name).cast(f.dataType).alias(f.name)
            for f in target_schema.fields
        ]
        return df.select(*select_exprs)

    def cast_columns(self, df: "DataFrame", casts: Dict[str, str]) -> "DataFrame":
        """
        Apply explicit casts to named columns.

        Example
        -------
        >>> df = sm.cast_columns(df, {"amount": "double", "event_date": "date"})
        """
        from pyspark.sql import functions as F

        for col_name, dtype in casts.items():
            df = df.withColumn(col_name, F.col(col_name).cast(dtype))
        return df

    # ------------------------------------------------------------------
    # Infer / print schema helpers
    # ------------------------------------------------------------------

    def print_schema_diff(self, schema_a: "StructType", schema_b: "StructType") -> None:
        diff = self.diff_schemas(schema_a, schema_b)
        print("=== Schema Diff ===")
        print(f"  Added   : {diff['added']}")
        print(f"  Dropped : {diff['dropped']}")
        print(f"  Changed : {diff['type_changed']}")

    def schema_to_ddl(self, schema: "StructType", exclude: Optional[List[str]] = None) -> str:
        """Return a Hive/Spark DDL column list string."""
        exclude = set(exclude or [])
        parts = [
            f"`{f.name}` {f.dataType.simpleString()}"
            for f in schema.fields
            if f.name not in exclude
        ]
        return ",\n  ".join(parts)

    def validate_not_null(self, df: "DataFrame", columns: List[str]) -> bool:
        """
        Return True if none of the specified columns contain nulls.
        Logs a warning for each column with nulls found.
        """
        from pyspark.sql import functions as F

        exprs = [F.count(F.when(F.col(c).isNull(), 1)).alias(c) for c in columns]
        result = df.agg(*exprs).collect()[0].asDict()
        has_nulls = False
        for col_name, null_count in result.items():
            if null_count > 0:
                log.warning("Column '%s' has %d null values!", col_name, null_count)
                has_nulls = True
        return not has_nulls

    def validate_unique(self, df: "DataFrame", key_columns: List[str]) -> bool:
        """Return True if *key_columns* form a unique key in *df*."""
        total = df.count()
        distinct = df.select(*key_columns).distinct().count()
        if total != distinct:
            log.warning(
                "Uniqueness check failed: %d rows, %d distinct keys on %s",
                total, distinct, key_columns,
            )
            return False
        return True
