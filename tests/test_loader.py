"""
tests/test_loader.py
--------------------
Tests for DataLoader load strategies: FULL, INCREMENTAL, PARTITION, MERGE, SNAPSHOT.
"""

import pytest
from pyspark.sql.types import (
    StructType, StructField, LongType, StringType, DoubleType, IntegerType, TimestampType,
)
from de_utils.loader import DataLoader, LoadType, LoadResult
from de_utils.utils import DataEngineeringError


DB = "test_loader"


@pytest.fixture(autouse=True, scope="module")
def setup_db(spark):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{DB}`")
    yield
    spark.sql(f"DROP DATABASE IF EXISTS `{DB}` CASCADE")


@pytest.fixture()
def loader(spark):
    return DataLoader(spark, database=DB)


@pytest.fixture()
def orders_schema():
    return StructType([
        StructField("order_id",    LongType()),
        StructField("status",      StringType()),
        StructField("amount",      DoubleType()),
        StructField("year",        IntegerType()),
        StructField("month",       IntegerType()),
        StructField("updated_at",  StringType()),
    ])


@pytest.fixture()
def base_orders(spark, orders_schema):
    return spark.createDataFrame(
        [
            (1, "PENDING",   99.9,  2024, 1, "2024-01-10 10:00:00"),
            (2, "SHIPPED",  150.0,  2024, 1, "2024-01-15 10:00:00"),
            (3, "DELIVERED",200.0,  2024, 2, "2024-02-05 10:00:00"),
        ],
        schema=orders_schema,
    )


class TestFullLoad:

    def test_creates_table_with_all_rows(self, loader, spark, base_orders):
        loader.full_load(base_orders, "fl_orders")
        assert spark.table(f"`{DB}`.`fl_orders`").count() == 3

    def test_full_load_overwrites(self, loader, spark, base_orders):
        loader.full_load(base_orders, "fl_overwrite")
        loader.full_load(base_orders.limit(1), "fl_overwrite")
        assert spark.table(f"`{DB}`.`fl_overwrite`").count() == 1

    def test_returns_load_result(self, loader, base_orders):
        result = loader.full_load(base_orders, "fl_result")
        assert isinstance(result, LoadResult)
        assert result.load_type == LoadType.FULL
        assert result.elapsed_seconds >= 0

    def test_load_result_repr(self, loader, base_orders):
        result = loader.full_load(base_orders, "fl_repr")
        assert "FULL" in repr(result)
        assert "fl_repr" in repr(result) or DB in repr(result)


class TestIncrementalLoad:

    def test_appends_new_rows(self, loader, spark, base_orders, orders_schema):
        loader.full_load(base_orders, "inc_orders")
        newer = spark.createDataFrame(
            [(4, "PENDING", 50.0, 2024, 3, "2024-03-01 10:00:00")],
            schema=orders_schema,
        )
        loader.incremental_load(newer, "inc_orders", watermark_column="updated_at",
                                last_watermark="2024-02-28 00:00:00")
        assert spark.table(f"`{DB}`.`inc_orders`").count() == 4

    def test_watermark_filters_old_rows(self, loader, spark, base_orders, orders_schema):
        loader.full_load(base_orders, "inc_filter")
        # Row with updated_at before watermark should be excluded
        mixed = spark.createDataFrame(
            [
                (5, "NEW", 10.0, 2024, 3, "2024-01-01 00:00:00"),  # too old
                (6, "NEW", 20.0, 2024, 3, "2024-03-15 00:00:00"),  # new
            ],
            schema=orders_schema,
        )
        loader.incremental_load(mixed, "inc_filter", watermark_column="updated_at",
                                last_watermark="2024-02-01 00:00:00")
        assert spark.table(f"`{DB}`.`inc_filter`").count() == 4  # 3 base + 1 new

    def test_missing_watermark_column_raises(self, loader, base_orders):
        with pytest.raises(DataEngineeringError, match="watermark_column"):
            loader.load(base_orders, "x", load_type=LoadType.INCREMENTAL)


class TestPartitionLoad:

    def test_loads_partition(self, loader, spark, base_orders):
        loader.full_load(base_orders, "part_orders")
        from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType, IntegerType
        march = spark.createDataFrame(
            [(4, "PENDING", 55.0, 2024, 3, "2024-03-01")],
            schema=base_orders.schema,
        )
        loader.partition_load(march, "part_orders", partition_keys=["year", "month"])
        # Row 4 in a new partition
        tbl = spark.table(f"`{DB}`.`part_orders`")
        assert tbl.filter("month = 3").count() == 1

    def test_missing_partition_keys_raises(self, loader, base_orders):
        with pytest.raises(DataEngineeringError, match="partition_keys"):
            loader.load(base_orders, "x", load_type=LoadType.PARTITION)


class TestMergeLoad:

    def test_merge_inserts_and_updates(self, loader, spark, base_orders, orders_schema):
        loader.full_load(base_orders, "ml_orders")
        cdc = spark.createDataFrame(
            [
                (1, "CANCELLED", 99.9, 2024, 1, "2024-01-20"),   # update
                (99, "NEW", 1.0,  2024, 4, "2024-04-01"),         # insert
            ],
            schema=orders_schema,
        )
        loader.merge_load(cdc, "ml_orders", match_keys=["order_id"])
        tbl = spark.table(f"`{DB}`.`ml_orders`")
        assert tbl.count() == 4
        assert tbl.filter("order_id = 1").collect()[0]["status"] == "CANCELLED"

    def test_missing_match_keys_raises(self, loader, base_orders):
        with pytest.raises(DataEngineeringError, match="match_keys"):
            loader.load(base_orders, "x", load_type=LoadType.MERGE)


class TestSnapshotLoad:

    def test_snapshot_adds_date_column(self, loader, spark, base_orders):
        loader.full_load(base_orders, "snap_base")
        loader.load(base_orders, "snap_base", load_type=LoadType.SNAPSHOT,
                    snapshot_date="2024-06-30")
        tbl = spark.table(f"`{DB}`.`snap_base`")
        assert "snapshot_date" in tbl.columns
        snapped = tbl.filter("snapshot_date IS NOT NULL")
        assert snapped.count() == 3


class TestInvalidLoadType:

    def test_unknown_load_type_raises(self, loader, base_orders):
        with pytest.raises(ValueError):
            loader.load(base_orders, "x", load_type="INVALID")
