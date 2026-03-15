"""
tests/test_metastore.py
-----------------------
Tests for HiveMetastore — database, table DDL, and partition operations.
Uses a local Spark + in-process Hive metastore (no external Thrift server).
"""

import pytest
from pyspark.sql.types import (
    StructType, StructField, LongType, StringType, IntegerType,
)
from de_utils.config import HiveConfig
from de_utils.metastore import HiveMetastore


DB = "test_metastore_db"


@pytest.fixture()
def meta(spark):
    cfg = HiveConfig(database=DB)
    return HiveMetastore(cfg, spark)


@pytest.fixture(autouse=True, scope="module")
def cleanup(spark):
    yield
    spark.sql(f"DROP DATABASE IF EXISTS `{DB}` CASCADE")


@pytest.fixture()
def simple_schema():
    return StructType([
        StructField("id",    LongType()),
        StructField("name",  StringType()),
        StructField("year",  IntegerType()),
        StructField("month", IntegerType()),
    ])


class TestCreateDatabase:

    def test_creates_database(self, meta, spark):
        meta.create_database_if_not_exists()
        dbs = meta.list_databases()
        assert DB in dbs

    def test_idempotent_second_call(self, meta):
        meta.create_database_if_not_exists()
        meta.create_database_if_not_exists()   # should not raise


class TestTableExists:

    def test_non_existent_table_returns_false(self, meta):
        assert meta.table_exists("definitely_does_not_exist_xyz") is False

    def test_existing_table_returns_true(self, meta, spark, simple_schema):
        meta.create_database_if_not_exists()
        spark.createDataFrame([], schema=simple_schema).write.mode("overwrite").saveAsTable(
            f"`{DB}`.`te_table`"
        )
        assert meta.table_exists("te_table") is True


class TestCreateManagedTable:

    def test_creates_managed_table(self, meta, spark, simple_schema):
        meta.create_database_if_not_exists()
        spark.sql(f"DROP TABLE IF EXISTS `{DB}`.`managed_t`")
        meta.create_managed_table("managed_t", simple_schema)
        assert meta.table_exists("managed_t") is True

    def test_if_not_exists_is_idempotent(self, meta, spark, simple_schema):
        meta.create_database_if_not_exists()
        spark.sql(f"DROP TABLE IF EXISTS `{DB}`.`idem_t`")
        meta.create_managed_table("idem_t", simple_schema, if_not_exists=True)
        meta.create_managed_table("idem_t", simple_schema, if_not_exists=True)
        assert meta.table_exists("idem_t") is True


class TestDropTable:

    def test_drops_existing_table(self, meta, spark, simple_schema):
        meta.create_database_if_not_exists()
        spark.createDataFrame([], schema=simple_schema).write.mode("overwrite").saveAsTable(
            f"`{DB}`.`drop_me`"
        )
        meta.drop_table("drop_me")
        assert meta.table_exists("drop_me") is False

    def test_drop_non_existent_is_safe(self, meta):
        meta.drop_table("never_existed_xyzzy")   # should not raise


class TestListTables:

    def test_returns_list(self, meta, spark, simple_schema):
        meta.create_database_if_not_exists()
        spark.createDataFrame([], schema=simple_schema).write.mode("overwrite").saveAsTable(
            f"`{DB}`.`lt_table`"
        )
        tables = meta.list_tables()
        assert isinstance(tables, list)
        assert "lt_table" in tables


class TestDescribeTable:

    def test_returns_dataframe(self, meta, spark, simple_schema):
        meta.create_database_if_not_exists()
        spark.createDataFrame([], schema=simple_schema).write.mode("overwrite").saveAsTable(
            f"`{DB}`.`desc_t`"
        )
        result = meta.describe_table("desc_t")
        assert result is not None
        assert result.count() > 0


class TestSetTableProperty:

    def test_property_can_be_set(self, meta, spark, simple_schema):
        meta.create_database_if_not_exists()
        spark.createDataFrame([], schema=simple_schema).write.mode("overwrite").saveAsTable(
            f"`{DB}`.`prop_t`"
        )
        meta.set_table_property("prop_t", "owner", "data-team")
        desc = meta.describe_table("prop_t")
        rows = desc.collect()
        row_strings = [str(r) for r in rows]
        assert any("owner" in s for s in row_strings)
