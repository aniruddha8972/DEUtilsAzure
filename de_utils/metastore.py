"""
de_utils.metastore
------------------
Hive Metastore operations: database/table DDL, schema management,
partition registration, and table properties.
"""

from __future__ import annotations

from typing import Dict, List, Optional, TYPE_CHECKING

from .config import HiveConfig
from .utils import get_logger, DataEngineeringError, quote_identifier

if TYPE_CHECKING:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.types import StructType

log = get_logger(__name__)


class HiveMetastore:
    """
    Manage databases, tables, partitions, and table properties
    in a Hive-compatible metastore via Spark SQL.

    Example
    -------
    >>> from de_utils import HiveConfig, HiveMetastore
    >>> meta = HiveMetastore(HiveConfig(database="silver"), spark)
    >>> meta.create_database_if_not_exists()
    >>> meta.create_external_table(
    ...     table_name="orders",
    ...     schema=orders_schema,
    ...     location="abfss://container@acct.dfs.core.windows.net/silver/orders",
    ...     partition_cols=["year", "month"],
    ...     file_format="PARQUET",
    ... )
    """

    def __init__(self, config: HiveConfig, spark: "SparkSession"):
        self.config = config
        self.spark = spark
        self._configure()

    def _configure(self) -> None:
        for k, v in self.config.to_spark_conf().items():
            self.spark.conf.set(k, v)
        log.info("Hive metastore configured — database: '%s'", self.config.database)

    # ------------------------------------------------------------------
    # Database helpers
    # ------------------------------------------------------------------

    @property
    def database(self) -> str:
        return self.config.database

    def create_database_if_not_exists(
        self,
        database: Optional[str] = None,
        location: Optional[str] = None,
        comment: Optional[str] = None,
    ) -> None:
        """Create the configured database if it does not already exist."""
        db = database or self.database
        sql = f"CREATE DATABASE IF NOT EXISTS {quote_identifier(db)}"
        if location:
            sql += f" LOCATION '{location}'"
        if comment:
            sql += f" COMMENT '{comment}'"
        self.spark.sql(sql)
        log.info("Ensured database '%s' exists.", db)

    def drop_database(self, database: Optional[str] = None, cascade: bool = False) -> None:
        db = database or self.database
        cascade_kw = "CASCADE" if cascade else "RESTRICT"
        self.spark.sql(f"DROP DATABASE IF EXISTS {quote_identifier(db)} {cascade_kw}")
        log.info("Dropped database '%s'.", db)

    def use_database(self, database: Optional[str] = None) -> None:
        db = database or self.database
        self.spark.sql(f"USE {quote_identifier(db)}")

    def list_databases(self) -> List[str]:
        return [row.namespace for row in self.spark.sql("SHOW DATABASES").collect()]

    # ------------------------------------------------------------------
    # Table DDL helpers
    # ------------------------------------------------------------------

    def _full_name(self, table_name: str, database: Optional[str] = None) -> str:
        db = database or self.database
        return f"{quote_identifier(db)}.{quote_identifier(table_name)}"

    def table_exists(self, table_name: str, database: Optional[str] = None) -> bool:
        fqn = self._full_name(table_name, database)
        try:
            self.spark.sql(f"DESCRIBE TABLE {fqn}")
            return True
        except Exception:
            return False

    def create_external_table(
        self,
        table_name: str,
        schema: "StructType",
        location: str,
        partition_cols: Optional[List[str]] = None,
        file_format: str = "PARQUET",
        table_properties: Optional[Dict[str, str]] = None,
        database: Optional[str] = None,
        comment: Optional[str] = None,
        if_not_exists: bool = True,
    ) -> None:
        """
        Register an external table in the Hive metastore.
        The data already lives on ADLS at *location*.
        """
        from pyspark.sql.types import StructType

        fqn = self._full_name(table_name, database)
        not_exists = "IF NOT EXISTS" if if_not_exists else ""

        # Build column DDL from schema, excluding partition columns
        partition_cols = partition_cols or []
        data_cols = [f for f in schema.fields if f.name not in partition_cols]
        col_ddl = ",\n  ".join(
            f"{quote_identifier(f.name)} {f.dataType.simpleString()}"
            + (f" COMMENT '{f.metadata.get('comment', '')}'" if f.metadata.get("comment") else "")
            for f in data_cols
        )

        sql = f"CREATE EXTERNAL TABLE {not_exists} {fqn} (\n  {col_ddl}\n)"

        if comment:
            sql += f"\nCOMMENT '{comment}'"

        if partition_cols:
            part_fields = [f for f in schema.fields if f.name in partition_cols]
            part_ddl = ", ".join(
                f"{quote_identifier(f.name)} {f.dataType.simpleString()}" for f in part_fields
            )
            sql += f"\nPARTITIONED BY ({part_ddl})"

        sql += f"\nSTORED AS {file_format}"
        sql += f"\nLOCATION '{location}'"

        if table_properties:
            props = ", ".join(f"'{k}'='{v}'" for k, v in table_properties.items())
            sql += f"\nTBLPROPERTIES ({props})"

        log.info("Creating external table %s at %s", fqn, location)
        self.spark.sql(sql)

    def create_managed_table(
        self,
        table_name: str,
        schema: "StructType",
        partition_cols: Optional[List[str]] = None,
        file_format: str = "PARQUET",
        database: Optional[str] = None,
        if_not_exists: bool = True,
    ) -> None:
        """Create a Hive-managed table (data stored in warehouse directory)."""
        fqn = self._full_name(table_name, database)
        not_exists = "IF NOT EXISTS" if if_not_exists else ""
        partition_cols = partition_cols or []
        data_cols = [f for f in schema.fields if f.name not in partition_cols]
        col_ddl = ", ".join(
            f"{quote_identifier(f.name)} {f.dataType.simpleString()}" for f in data_cols
        )
        sql = f"CREATE TABLE {not_exists} {fqn} ({col_ddl}) STORED AS {file_format}"
        if partition_cols:
            part_fields = [f for f in schema.fields if f.name in partition_cols]
            part_ddl = ", ".join(
                f"{quote_identifier(f.name)} {f.dataType.simpleString()}" for f in part_fields
            )
            sql += f" PARTITIONED BY ({part_ddl})"
        self.spark.sql(sql)
        log.info("Created managed table %s", fqn)

    def drop_table(self, table_name: str, database: Optional[str] = None, purge: bool = False) -> None:
        fqn = self._full_name(table_name, database)
        purge_kw = "PURGE" if purge else ""
        self.spark.sql(f"DROP TABLE IF EXISTS {fqn} {purge_kw}")
        log.info("Dropped table %s", fqn)

    def truncate_table(self, table_name: str, database: Optional[str] = None) -> None:
        fqn = self._full_name(table_name, database)
        self.spark.sql(f"TRUNCATE TABLE {fqn}")
        log.info("Truncated table %s", fqn)

    # ------------------------------------------------------------------
    # Partition helpers
    # ------------------------------------------------------------------

    def add_partition(
        self,
        table_name: str,
        partition_spec: Dict[str, str],
        location: Optional[str] = None,
        database: Optional[str] = None,
    ) -> None:
        """Add a single partition to a table (with optional explicit location)."""
        fqn = self._full_name(table_name, database)
        spec = ", ".join(f"{quote_identifier(k)}='{v}'" for k, v in partition_spec.items())
        sql = f"ALTER TABLE {fqn} ADD IF NOT EXISTS PARTITION ({spec})"
        if location:
            sql += f" LOCATION '{location}'"
        self.spark.sql(sql)
        log.info("Added partition (%s) to %s", spec, fqn)

    def drop_partition(
        self,
        table_name: str,
        partition_spec: Dict[str, str],
        database: Optional[str] = None,
    ) -> None:
        fqn = self._full_name(table_name, database)
        spec = ", ".join(f"{quote_identifier(k)}='{v}'" for k, v in partition_spec.items())
        self.spark.sql(f"ALTER TABLE {fqn} DROP IF EXISTS PARTITION ({spec})")
        log.info("Dropped partition (%s) from %s", spec, fqn)

    def repair_table(self, table_name: str, database: Optional[str] = None) -> None:
        """Run MSCK REPAIR TABLE to sync partitions from storage."""
        fqn = self._full_name(table_name, database)
        self.spark.sql(f"MSCK REPAIR TABLE {fqn}")
        log.info("Repaired (MSCK) table %s", fqn)

    def list_partitions(self, table_name: str, database: Optional[str] = None) -> List[str]:
        fqn = self._full_name(table_name, database)
        rows = self.spark.sql(f"SHOW PARTITIONS {fqn}").collect()
        return [r[0] for r in rows]

    # ------------------------------------------------------------------
    # Table info
    # ------------------------------------------------------------------

    def describe_table(self, table_name: str, database: Optional[str] = None) -> "DataFrame":
        fqn = self._full_name(table_name, database)
        return self.spark.sql(f"DESCRIBE EXTENDED {fqn}")

    def list_tables(self, database: Optional[str] = None) -> List[str]:
        db = database or self.database
        rows = self.spark.sql(f"SHOW TABLES IN {quote_identifier(db)}").collect()
        return [r.tableName for r in rows]

    def set_table_property(
        self,
        table_name: str,
        key: str,
        value: str,
        database: Optional[str] = None,
    ) -> None:
        fqn = self._full_name(table_name, database)
        self.spark.sql(f"ALTER TABLE {fqn} SET TBLPROPERTIES ('{key}'='{value}')")
