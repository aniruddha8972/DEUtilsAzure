"""tests/test_medallion.py"""
import pytest
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType
from de_utils_v2.medallion import BronzeLoader, SilverLoader, GoldLoader
from de_utils_v2.utils import DataEngineeringError

DB_B = "test_bronze"
DB_S = "test_silver"
DB_G = "test_gold"

@pytest.fixture(autouse=True, scope="module")
def setup_dbs(spark):
    for db in [DB_B, DB_S, DB_G]:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS `{db}`")
    yield
    for db in [DB_B, DB_S, DB_G]:
        spark.sql(f"DROP DATABASE IF EXISTS `{db}` CASCADE")

@pytest.fixture()
def schema():
    return StructType([
        StructField("order_id",    LongType()),
        StructField("customer_id", LongType()),
        StructField("status",      StringType()),
        StructField("amount",      DoubleType()),
    ])

@pytest.fixture()
def sample_df(spark, schema):
    return spark.createDataFrame(
        [(1,10,"PENDING",99.9),(2,11,"SHIPPED",150.0),(3,12,"DELIVERED",200.0)],
        schema=schema)

# ── Bronze ────────────────────────────────────────────────────────────────────

class TestBronzeLoader:
    def test_ingest_creates_table(self, spark, sample_df):
        bl = BronzeLoader(spark, database=DB_B)
        bl.ingest(sample_df, "orders_raw", source_tag="test")
        assert spark.catalog.tableExists(f"`{DB_B}`.`orders_raw`")

    def test_ingest_returns_row_count(self, spark, sample_df):
        bl = BronzeLoader(spark, database=DB_B)
        count = bl.ingest(sample_df, "orders_raw2", source_tag="test")
        assert count == 3

    def test_metadata_columns_added(self, spark, sample_df):
        bl = BronzeLoader(spark, database=DB_B)
        bl.ingest(sample_df, "orders_meta", add_metadata=True)
        tbl = spark.table(f"`{DB_B}`.`orders_meta`")
        assert "_ingest_ts" in tbl.columns
        assert "_source_tag" in tbl.columns
        assert "_run_id" in tbl.columns

    def test_no_metadata_when_disabled(self, spark, sample_df):
        bl = BronzeLoader(spark, database=DB_B)
        bl.ingest(sample_df, "orders_nometa", add_metadata=False)
        tbl = spark.table(f"`{DB_B}`.`orders_nometa`")
        assert "_ingest_ts" not in tbl.columns

    def test_append_mode(self, spark, sample_df):
        bl = BronzeLoader(spark, database=DB_B)
        bl.ingest(sample_df, "orders_append", add_metadata=False, mode="overwrite")
        bl.ingest(sample_df, "orders_append", add_metadata=False, mode="append")
        assert spark.table(f"`{DB_B}`.`orders_append`").count() == 6

    def test_source_tag_stored(self, spark, sample_df):
        bl = BronzeLoader(spark, database=DB_B)
        bl.ingest(sample_df, "orders_tag", source_tag="s3://bucket/path/")
        row = spark.table(f"`{DB_B}`.`orders_tag`").select("_source_tag").first()
        assert row["_source_tag"] == "s3://bucket/path/"

# ── Silver ────────────────────────────────────────────────────────────────────

class TestSilverLoader:
    def test_load_creates_table(self, spark, sample_df):
        sl = SilverLoader(spark, database=DB_S)
        sl.load(sample_df, "orders", key_columns=["order_id"])
        assert spark.catalog.tableExists(f"`{DB_S}`.`orders`")

    def test_dedup_removes_duplicates(self, spark, schema):
        sl = SilverLoader(spark, database=DB_S)
        duped = spark.createDataFrame(
            [(1,10,"PENDING",99.9),(1,10,"PENDING",99.9),(2,11,"SHIPPED",150.0)],
            schema=schema)
        result = sl.load(duped, "orders_dedup", key_columns=["order_id"], dedup=True)
        assert spark.table(f"`{DB_S}`.`orders_dedup`").count() == 2

    def test_null_rejection_sidecar(self, spark, schema):
        sl = SilverLoader(spark, database=DB_S, reject_action="sidecar")
        df_with_nulls = spark.createDataFrame(
            [(1,10,"PENDING",99.9),(None,11,"SHIPPED",150.0)], schema=schema)
        sl.load(df_with_nulls, "orders_nullchk",
                not_null_columns=["order_id"], key_columns=["order_id"])
        main = spark.table(f"`{DB_S}`.`orders_nullchk`").count()
        assert main == 1

    def test_null_rejection_fail_raises(self, spark, schema):
        sl = SilverLoader(spark, database=DB_S, reject_action="fail")
        df_with_null = spark.createDataFrame([(None,11,"X",1.0)], schema=schema)
        with pytest.raises(DataEngineeringError):
            sl.load(df_with_null, "orders_fail_null", not_null_columns=["order_id"])

    def test_cast_map_applied(self, spark):
        from pyspark.sql.types import IntegerType
        str_schema = StructType([
            StructField("order_id", StringType()),
            StructField("amount",   StringType()),
        ])
        df = spark.createDataFrame([("1","99.5")], schema=str_schema)
        sl = SilverLoader(spark, database=DB_S)
        sl.load(df, "orders_cast", cast_map={"order_id":"long","amount":"double"})
        schema_out = spark.table(f"`{DB_S}`.`orders_cast`").schema
        assert str(schema_out["order_id"].dataType) == "LongType()"

    def test_load_returns_summary_dict(self, spark, sample_df):
        sl = SilverLoader(spark, database=DB_S)
        result = sl.load(sample_df, "orders_summary", key_columns=["order_id"])
        assert "rows_in" in result
        assert "rows_out" in result
        assert "rows_rejected" in result

# ── Gold ──────────────────────────────────────────────────────────────────────

class TestGoldLoader:
    def _make_scd2_table(self, spark, table, df):
        from pyspark.sql import functions as F
        spark.sql(f"DROP TABLE IF EXISTS `{DB_G}`.`{table}`")
        (df.withColumn("effective_from", F.lit("2024-01-01").cast("timestamp"))
           .withColumn("effective_to",   F.lit("9999-12-31").cast("timestamp"))
           .withColumn("is_current",     F.lit(True))
           .write.mode("overwrite").saveAsTable(f"`{DB_G}`.`{table}`"))

    def test_load_fact_creates_table(self, spark, sample_df):
        spark.sql(f"DROP TABLE IF EXISTS `{DB_G}`.`fact_orders`")
        sample_df.write.mode("overwrite").saveAsTable(f"`{DB_G}`.`fact_orders`")
        gl = GoldLoader(spark, database=DB_G)
        from de_utils_v2.loader import LoadType
        gl.load_fact(sample_df, "fact_orders",
                     match_keys=["order_id"], load_type=LoadType.MERGE)
        assert spark.table(f"`{DB_G}`.`fact_orders`").count() == 3

    def test_load_dimension_scd1(self, spark, sample_df):
        spark.sql(f"DROP TABLE IF EXISTS `{DB_G}`.`dim_orders_s1`")
        sample_df.write.mode("overwrite").saveAsTable(f"`{DB_G}`.`dim_orders_s1`")
        gl = GoldLoader(spark, database=DB_G)
        gl.load_dimension(sample_df, "dim_orders_s1",
                          key_columns=["order_id"], scd_type=1)

    def test_invalid_scd_type_raises(self, spark, sample_df):
        spark.sql(f"DROP TABLE IF EXISTS `{DB_G}`.`dim_inv`")
        sample_df.write.mode("overwrite").saveAsTable(f"`{DB_G}`.`dim_inv`")
        gl = GoldLoader(spark, database=DB_G)
        with pytest.raises(DataEngineeringError):
            gl.load_dimension(sample_df, "dim_inv",
                              key_columns=["order_id"], scd_type=99)

    def test_load_fact_with_dq_rules(self, spark, sample_df):
        spark.sql(f"DROP TABLE IF EXISTS `{DB_G}`.`fact_dq`")
        sample_df.write.mode("overwrite").saveAsTable(f"`{DB_G}`.`fact_dq`")
        from de_utils_v2.quality import Rule
        from de_utils_v2.loader import LoadType
        gl = GoldLoader(spark, database=DB_G)
        gl.load_fact(sample_df, "fact_dq",
                     match_keys=["order_id"],
                     load_type=LoadType.MERGE,
                     dq_rules=[Rule.not_null("order_id"), Rule.row_count(">=",1)])
