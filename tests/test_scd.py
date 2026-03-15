"""
tests/test_scd.py
-----------------
Tests for SCDHandler: SCD1, SCD2, SCD3, SCD6, and the diff() utility.
"""

import pytest
from pyspark.sql.types import (
    StructType, StructField, LongType, StringType, DoubleType, BooleanType, TimestampType,
)
from de_utils.scd import SCDHandler, SCD2Result


DB = "test_scd"


@pytest.fixture(autouse=True, scope="module")
def setup_db(spark):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{DB}`")
    yield
    spark.sql(f"DROP DATABASE IF EXISTS `{DB}` CASCADE")


@pytest.fixture()
def scd(spark):
    return SCDHandler(spark, database=DB)


@pytest.fixture()
def dim_schema():
    return StructType([
        StructField("customer_id", LongType()),
        StructField("name",        StringType()),
        StructField("email",       StringType()),
        StructField("tier",        StringType()),
    ])


@pytest.fixture()
def scd2_schema():
    """Target schema for SCD2 table (includes metadata cols)."""
    return StructType([
        StructField("customer_id",   LongType()),
        StructField("name",          StringType()),
        StructField("email",         StringType()),
        StructField("tier",          StringType()),
        StructField("effective_from",TimestampType()),
        StructField("effective_to",  TimestampType()),
        StructField("is_current",    BooleanType()),
    ])


@pytest.fixture()
def scd3_schema():
    return StructType([
        StructField("customer_id", LongType()),
        StructField("email",       StringType()),
        StructField("prev_email",  StringType()),
        StructField("tier",        StringType()),
        StructField("prev_tier",   StringType()),
    ])


@pytest.fixture()
def initial_customers(spark, dim_schema):
    return spark.createDataFrame(
        [(1, "Alice", "a@a.com", "GOLD"), (2, "Bob", "b@b.com", "SILVER")],
        schema=dim_schema,
    )


@pytest.fixture()
def updated_customers(spark, dim_schema):
    """Alice's tier changes, Charlie is new."""
    return spark.createDataFrame(
        [(1, "Alice", "a@a.com", "PLATINUM"), (2, "Bob", "b@b.com", "SILVER"),
         (3, "Charlie", "c@c.com", "BRONZE")],
        schema=dim_schema,
    )


# ─── SCD 1 ───────────────────────────────────────────────────────────────────

class TestSCD1:

    def test_insert_new_rows(self, scd, spark, initial_customers, dim_schema):
        spark.sql(f"DROP TABLE IF EXISTS `{DB}`.`scd1_dim`")
        initial_customers.write.mode("overwrite").saveAsTable(f"`{DB}`.`scd1_dim`")
        new_customer = spark.createDataFrame([(9, "Zara", "z@z.com", "BRONZE")], schema=dim_schema)
        scd.apply_scd1("scd1_dim", new_customer, key_columns=["customer_id"])
        tbl = spark.table(f"`{DB}`.`scd1_dim`")
        assert tbl.count() == 3
        assert tbl.filter("customer_id = 9").count() == 1

    def test_updates_existing_row(self, scd, spark, initial_customers, dim_schema):
        spark.sql(f"DROP TABLE IF EXISTS `{DB}`.`scd1_upd`")
        initial_customers.write.mode("overwrite").saveAsTable(f"`{DB}`.`scd1_upd`")
        changed = spark.createDataFrame([(1, "Alice", "new@a.com", "PLATINUM")], schema=dim_schema)
        scd.apply_scd1("scd1_upd", changed, key_columns=["customer_id"])
        row = spark.table(f"`{DB}`.`scd1_upd`").filter("customer_id = 1").collect()[0]
        assert row["tier"] == "PLATINUM"
        assert row["email"] == "new@a.com"

    def test_unchanged_rows_not_duplicated(self, scd, spark, initial_customers):
        spark.sql(f"DROP TABLE IF EXISTS `{DB}`.`scd1_nodup`")
        initial_customers.write.mode("overwrite").saveAsTable(f"`{DB}`.`scd1_nodup`")
        scd.apply_scd1("scd1_nodup", initial_customers, key_columns=["customer_id"])
        assert spark.table(f"`{DB}`.`scd1_nodup`").count() == 2

    def test_respects_update_columns(self, scd, spark, initial_customers, dim_schema):
        spark.sql(f"DROP TABLE IF EXISTS `{DB}`.`scd1_cols`")
        initial_customers.write.mode("overwrite").saveAsTable(f"`{DB}`.`scd1_cols`")
        changed = spark.createDataFrame([(2, "Bob", "NEW@b.com", "GOLD")], schema=dim_schema)
        scd.apply_scd1("scd1_cols", changed, key_columns=["customer_id"], update_columns=["tier"])
        row = spark.table(f"`{DB}`.`scd1_cols`").filter("customer_id = 2").collect()[0]
        assert row["tier"] == "GOLD"
        assert row["email"] == "b@b.com"   # email was NOT in update_columns → unchanged


# ─── SCD 2 ───────────────────────────────────────────────────────────────────

class TestSCD2:

    def _make_scd2_table(self, spark, initial_df, table_name, scd2_schema):
        from pyspark.sql import functions as F
        spark.sql(f"DROP TABLE IF EXISTS `{DB}`.`{table_name}`")
        df = (initial_df
              .withColumn("effective_from", F.lit("2024-01-01 00:00:00").cast("timestamp"))
              .withColumn("effective_to",   F.lit("9999-12-31 23:59:59").cast("timestamp"))
              .withColumn("is_current",     F.lit(True)))
        df.write.mode("overwrite").saveAsTable(f"`{DB}`.`{table_name}`")
        return df

    def test_new_keys_inserted(self, scd, spark, initial_customers, updated_customers, scd2_schema, dim_schema):
        self._make_scd2_table(spark, initial_customers, "scd2_ins", scd2_schema)
        result = scd.apply_scd2("scd2_ins", updated_customers,
                                key_columns=["customer_id"],
                                tracked_columns=["tier"],
                                effective_date="2024-06-01 00:00:00")
        assert isinstance(result, SCD2Result)
        assert result.new_inserts >= 1   # Charlie

    def test_changed_rows_add_new_version(self, scd, spark, initial_customers, updated_customers, scd2_schema):
        self._make_scd2_table(spark, initial_customers, "scd2_ver", scd2_schema)
        result = scd.apply_scd2("scd2_ver", updated_customers,
                                key_columns=["customer_id"],
                                tracked_columns=["tier"],
                                effective_date="2024-06-01 00:00:00")
        assert result.updates >= 1  # Alice tier changed

    def test_unchanged_rows_not_duplicated(self, scd, spark, initial_customers, scd2_schema):
        self._make_scd2_table(spark, initial_customers, "scd2_nodup", scd2_schema)
        result = scd.apply_scd2("scd2_nodup", initial_customers,
                                key_columns=["customer_id"],
                                tracked_columns=["tier"])
        assert result.new_inserts == 0
        assert result.updates == 0

    def test_result_repr(self):
        r = SCD2Result("dim_customer", 5, 3, "2024-06-01")
        assert "dim_customer" in repr(r)
        assert "new_inserts=5" in repr(r)


# ─── SCD 3 ───────────────────────────────────────────────────────────────────

class TestSCD3:

    def _make_scd3_table(self, spark, table_name, scd3_schema):
        spark.sql(f"DROP TABLE IF EXISTS `{DB}`.`{table_name}`")
        data = [(1, "a@old.com", None, "SILVER", None),
                (2, "b@old.com", None, "GOLD",   None)]
        spark.createDataFrame(data, schema=scd3_schema).write.mode("overwrite").saveAsTable(
            f"`{DB}`.`{table_name}`"
        )

    def test_prev_column_populated_on_change(self, scd, spark, scd3_schema):
        self._make_scd3_table(spark, "scd3_prev", scd3_schema)
        updates = spark.createDataFrame(
            [(1, "a@new.com", None, "PLATINUM", None)],
            schema=scd3_schema,
        ).drop("prev_email", "prev_tier")
        # Rebuild with just current columns
        from pyspark.sql.types import StructType, StructField, LongType, StringType
        src_schema = StructType([
            StructField("customer_id", LongType()),
            StructField("email",       StringType()),
            StructField("tier",        StringType()),
        ])
        src = spark.createDataFrame([(1, "a@new.com", "PLATINUM")], schema=src_schema)
        # Apply — target has prev_ cols, source doesn't need them
        scd.apply_scd3(
            target_table="scd3_prev",
            source_df=spark.createDataFrame(
                [(1, "a@new.com", None, "PLATINUM", None)], schema=scd3_schema
            ),
            key_columns=["customer_id"],
            tracked_columns=["email", "tier"],
        )
        row = spark.table(f"`{DB}`.`scd3_prev`").filter("customer_id = 1").collect()[0]
        assert row["email"] == "a@new.com"
        assert row["prev_email"] == "a@old.com"


# ─── DIFF ─────────────────────────────────────────────────────────────────────

class TestDiff:

    def test_new_rows_detected(self, scd, spark, initial_customers, updated_customers):
        diff_df = scd.diff(initial_customers, updated_customers, key_columns=["customer_id"])
        new_rows = diff_df.filter("`_diff_type` = 'NEW'").collect()
        assert any(r["customer_id"] == 3 for r in new_rows)  # Charlie

    def test_deleted_rows_detected(self, scd, spark, initial_customers, updated_customers):
        diff_df = scd.diff(initial_customers, updated_customers, key_columns=["customer_id"])
        # No deletes: updated_customers has 3 rows, initial has 2 with keys 1 and 2
        # Alice (1) and Bob (2) are both present → no DELETED
        deleted = diff_df.filter("`_diff_type` = 'DELETED'").count()
        assert deleted == 0

    def test_changed_rows_detected(self, scd, spark, initial_customers, updated_customers):
        diff_df = scd.diff(
            initial_customers, updated_customers,
            key_columns=["customer_id"],
            compare_columns=["tier"],
        )
        changed = diff_df.filter("`_diff_type` = 'CHANGED'").collect()
        assert any(r["customer_id"] == 1 for r in changed)  # Alice: GOLD → PLATINUM

    def test_unchanged_rows_detected(self, scd, spark, initial_customers, updated_customers):
        diff_df = scd.diff(
            initial_customers, updated_customers,
            key_columns=["customer_id"],
            compare_columns=["tier"],
        )
        unchanged = diff_df.filter("`_diff_type` = 'UNCHANGED'").collect()
        assert any(r["customer_id"] == 2 for r in unchanged)  # Bob unchanged

    def test_diff_type_column_present(self, scd, spark, initial_customers, updated_customers):
        diff_df = scd.diff(initial_customers, updated_customers, key_columns=["customer_id"])
        assert "_diff_type" in diff_df.columns

    def test_diff_all_unchanged(self, scd, spark, initial_customers):
        diff_df = scd.diff(initial_customers, initial_customers,
                           key_columns=["customer_id"], compare_columns=["email", "tier"])
        types = {r["_diff_type"] for r in diff_df.select("_diff_type").collect()}
        assert types == {"UNCHANGED"}
