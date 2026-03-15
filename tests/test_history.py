"""
tests/test_history.py
---------------------
Tests for HistoryTable — init, capture (insert/update/delete), point-in-time queries.
"""

import pytest
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType
from de_utils.history import HistoryTable, HistoryCaptureResult, HIST_COLS


DB = "test_history"
TABLE = "customers"
HIST_TABLE = "customers_hist"


@pytest.fixture(autouse=True, scope="module")
def setup_db(spark):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{DB}`")
    yield
    spark.sql(f"DROP DATABASE IF EXISTS `{DB}` CASCADE")


@pytest.fixture()
def hist(spark):
    return HistoryTable(spark, database=DB)


@pytest.fixture()
def customer_schema():
    return StructType([
        StructField("customer_id", LongType()),
        StructField("name",        StringType()),
        StructField("email",       StringType()),
    ])


@pytest.fixture()
def v1_df(spark, customer_schema):
    return spark.createDataFrame(
        [(1, "Alice", "alice@a.com"), (2, "Bob", "bob@b.com"), (3, "Charlie", "charlie@c.com")],
        schema=customer_schema,
    )


@pytest.fixture()
def v2_df(spark, customer_schema):
    """Bob email changed, Charlie deleted, Dave is new."""
    return spark.createDataFrame(
        [(1, "Alice", "alice@a.com"), (2, "Bob", "bob_new@b.com"), (4, "Dave", "dave@d.com")],
        schema=customer_schema,
    )


class TestInitHistoryTable:

    def test_creates_hist_table(self, hist, spark, v1_df):
        hist.init_history_table(
            "init_cust", v1_df,
            key_columns=["customer_id"],
            valid_from="2024-01-01 00:00:00",
        )
        assert spark.catalog.tableExists(f"`{DB}`.`init_cust_hist`")

    def test_initial_rows_all_current(self, hist, spark, v1_df):
        hist.init_history_table(
            "curr_cust", v1_df,
            key_columns=["customer_id"],
        )
        tbl = spark.table(f"`{DB}`.`curr_cust_hist`")
        assert tbl.filter("`_is_current` = true").count() == 3

    def test_initial_valid_to_is_sentinel(self, hist, spark, v1_df):
        hist.init_history_table(
            "sent_cust", v1_df,
            key_columns=["customer_id"],
        )
        tbl = spark.table(f"`{DB}`.`sent_cust_hist`")
        # All rows should have the sentinel date as valid_to
        rows = tbl.select("_valid_to").collect()
        assert all("9999" in str(r["_valid_to"]) for r in rows)

    def test_idempotent_on_second_call(self, hist, spark, v1_df):
        hist.init_history_table("idem_cust", v1_df, key_columns=["customer_id"])
        hist.init_history_table("idem_cust", v1_df, key_columns=["customer_id"])
        # Count should still be 3, not doubled
        assert spark.table(f"`{DB}`.`idem_cust_hist`").count() == 3

    def test_row_hash_column_present(self, hist, spark, v1_df):
        hist.init_history_table("hash_cust", v1_df, key_columns=["customer_id"])
        tbl = spark.table(f"`{DB}`.`hash_cust_hist`")
        assert "_row_hash" in tbl.columns

    def test_operation_column_is_insert(self, hist, spark, v1_df):
        hist.init_history_table("op_cust", v1_df, key_columns=["customer_id"])
        tbl = spark.table(f"`{DB}`.`op_cust_hist`")
        ops = [r["_operation"] for r in tbl.select("_operation").collect()]
        assert all(op == "INSERT" for op in ops)


class TestCaptureChanges:

    @pytest.fixture(autouse=True)
    def init_table(self, hist, v1_df):
        hist.init_history_table(
            "cap_cust", v1_df,
            key_columns=["customer_id"],
            valid_from="2024-01-01 00:00:00",
        )

    def test_returns_capture_result(self, hist, v2_df):
        result = hist.capture(v2_df, "cap_cust", key_columns=["customer_id"])
        assert isinstance(result, HistoryCaptureResult)

    def test_new_key_counted_as_insert(self, hist, v2_df):
        result = hist.capture(v2_df, "cap_cust", key_columns=["customer_id"])
        assert result.inserts >= 1  # Dave

    def test_changed_row_counted_as_update(self, hist, v2_df):
        result = hist.capture(v2_df, "cap_cust", key_columns=["customer_id"])
        assert result.updates >= 1  # Bob

    def test_deleted_row_counted_as_delete(self, hist, v2_df):
        result = hist.capture(v2_df, "cap_cust", key_columns=["customer_id"])
        assert result.deletes >= 1  # Charlie

    def test_unchanged_rows_not_duplicated(self, hist, spark, v1_df):
        # Capture with same data — no inserts/updates/deletes
        result = hist.capture(v1_df, "cap_cust", key_columns=["customer_id"])
        assert result.inserts == 0
        assert result.updates == 0
        assert result.deletes == 0


class TestGetVersionAt:

    def test_returns_dataframe(self, hist, spark, v1_df):
        hist.init_history_table("vat_cust", v1_df, key_columns=["customer_id"],
                                valid_from="2024-01-01 00:00:00")
        snapshot = hist.get_version_at("vat_cust", "2024-06-01 00:00:00")
        assert snapshot is not None
        assert snapshot.count() == 3

    def test_get_full_history_returns_all_rows(self, hist, spark, v1_df):
        hist.init_history_table("fh_cust", v1_df, key_columns=["customer_id"])
        all_rows = hist.get_full_history("fh_cust")
        assert all_rows.count() == 3


class TestHistoryCaptureResult:

    def test_repr_contains_table_name(self):
        from datetime import datetime
        r = HistoryCaptureResult("my_table", 1, 2, 3, datetime.utcnow())
        assert "my_table" in repr(r)
        assert "inserts=1" in repr(r)
