"""
tests/test_table_ops.py
-----------------------
Tests for TableOperations: create, insert, update, delete, merge.
Uses in-memory Hive tables — no external storage required.
"""

import pytest
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType
from de_utils.table_ops import TableOperations


DB = "test_table_ops"


@pytest.fixture(autouse=True, scope="module")
def setup_db(spark):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{DB}`")
    yield
    spark.sql(f"DROP DATABASE IF EXISTS `{DB}` CASCADE")


@pytest.fixture()
def ops(spark):
    return TableOperations(spark, database=DB)


@pytest.fixture()
def base_df(spark):
    schema = StructType([
        StructField("order_id",    LongType()),
        StructField("customer_id", LongType()),
        StructField("status",      StringType()),
        StructField("amount",      DoubleType()),
    ])
    return spark.createDataFrame(
        [(1, 10, "PENDING", 99.9), (2, 11, "SHIPPED", 150.0), (3, 12, "DELIVERED", 200.0)],
        schema=schema,
    )


class TestCreateOrReplace:

    def test_creates_table(self, ops, spark, base_df):
        ops.create_or_replace("co_orders", base_df)
        assert spark.catalog.tableExists(f"`{DB}`.`co_orders`")

    def test_row_count_matches(self, ops, spark, base_df):
        ops.create_or_replace("co_count", base_df)
        assert spark.table(f"`{DB}`.`co_count`").count() == 3

    def test_replaces_existing_data(self, ops, spark, base_df):
        ops.create_or_replace("co_replace", base_df)
        small_df = base_df.limit(1)
        ops.create_or_replace("co_replace", small_df)
        assert spark.table(f"`{DB}`.`co_replace`").count() == 1


class TestCreateIfNotExists:

    def test_creates_when_missing(self, ops, spark, base_df):
        created = ops.create_if_not_exists("cine_new", base_df)
        assert created is True
        assert spark.catalog.tableExists(f"`{DB}`.`cine_new`")

    def test_skips_when_exists(self, ops, spark, base_df):
        ops.create_or_replace("cine_exists", base_df)
        created = ops.create_if_not_exists("cine_exists", base_df)
        assert created is False


class TestInsertAppend:

    def test_appends_rows(self, ops, spark, base_df):
        ops.create_or_replace("ia_table", base_df)
        ops.insert_append("ia_table", base_df)
        assert spark.table(f"`{DB}`.`ia_table`").count() == 6


class TestInsertOverwrite:

    def test_overwrites_all_rows(self, ops, spark, base_df):
        ops.create_or_replace("io_table", base_df)
        small_df = base_df.limit(1)
        ops.insert_overwrite("io_table", small_df)
        assert spark.table(f"`{DB}`.`io_table`").count() == 1


class TestMerge:

    def test_inserts_new_rows(self, ops, spark, base_df):
        ops.create_or_replace("merge_ins", base_df)
        new_rows = spark.createDataFrame(
            [(99, 99, "NEW", 5.0)],
            schema=base_df.schema,
        )
        ops.merge("merge_ins", new_rows, match_keys=["order_id"])
        result = spark.table(f"`{DB}`.`merge_ins`")
        assert result.count() == 4
        assert result.filter("order_id = 99").count() == 1

    def test_updates_existing_rows(self, ops, spark, base_df):
        ops.create_or_replace("merge_upd", base_df)
        updates = spark.createDataFrame(
            [(1, 10, "CANCELLED", 0.0)],
            schema=base_df.schema,
        )
        ops.merge("merge_upd", updates, match_keys=["order_id"])
        result = spark.table(f"`{DB}`.`merge_upd`")
        row = result.filter("order_id = 1").collect()[0]
        assert row["status"] == "CANCELLED"

    def test_upsert_alias_works(self, ops, spark, base_df):
        ops.create_or_replace("upsert_tbl", base_df)
        new_row = spark.createDataFrame([(100, 50, "PENDING", 1.0)], schema=base_df.schema)
        ops.upsert("upsert_tbl", new_row, match_keys=["order_id"])
        assert spark.table(f"`{DB}`.`upsert_tbl`").count() == 4

    def test_merge_respects_update_columns(self, ops, spark, base_df):
        ops.create_or_replace("merge_cols", base_df)
        updates = spark.createDataFrame(
            [(2, 99, "DELIVERED", 999.0)],   # customer_id and amount also differ
            schema=base_df.schema,
        )
        # Only update status, not amount
        ops.merge("merge_cols", updates, match_keys=["order_id"], update_columns=["status"])
        row = spark.table(f"`{DB}`.`merge_cols`").filter("order_id = 2").collect()[0]
        assert row["status"] == "DELIVERED"
        assert row["amount"] == 150.0   # unchanged
