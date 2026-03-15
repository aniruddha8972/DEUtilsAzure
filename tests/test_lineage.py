"""tests/test_lineage.py"""
import pytest
from de_utils_v2.lineage import LineageTracker, LineageRecord
from datetime import datetime

DB = "test_lineage"

@pytest.fixture(autouse=True, scope="module")
def setup_db(spark):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{DB}`")
    yield
    spark.sql(f"DROP DATABASE IF EXISTS `{DB}` CASCADE")

@pytest.fixture()
def tracker(spark):
    return LineageTracker(spark, lineage_table=f"`{DB}`.`lineage_log`",
                          job_name="test_job", auto_save=False)

class TestLineageTracker:
    def test_record_creates_entry(self, tracker):
        rec = tracker.record("bronze.raw", "silver.orders", "FULL", rows_read=100, rows_written=98)
        assert rec.source_table == "bronze.raw"
        assert rec.rows_read == 100

    def test_context_manager_success(self, tracker):
        with tracker.track("bronze.a", "silver.b", "INCREMENTAL") as rec:
            rec.rows_read = 50
        assert rec.status == "SUCCESS"
        assert rec.completed_at is not None

    def test_context_manager_failure(self, tracker):
        with pytest.raises(ValueError):
            with tracker.track("src", "tgt", "MERGE") as rec:
                raise ValueError("simulated failure")
        assert rec.status == "FAILED"
        assert "simulated" in rec.error_message

    def test_duration_computed(self, tracker):
        rec = tracker.record("a", "b", rows_read=10)
        assert rec.duration_seconds is not None
        assert rec.duration_seconds >= 0

    def test_save_writes_to_table(self, spark):
        t = LineageTracker(spark, lineage_table=f"`{DB}`.`lin_save`",
                           job_name="save_test", auto_save=True)
        t.record("src", "tgt", "FULL", rows_read=5, rows_written=5)
        assert spark.table(f"`{DB}`.`lin_save`").count() >= 1

    def test_compare_schemas_for_lineage(self, tracker, spark):
        from pyspark.sql.types import StructType, StructField, StringType, LongType
        src = spark.createDataFrame([], StructType([StructField("id", LongType())]))
        tgt = spark.createDataFrame([], StructType([StructField("id", LongType()),
                                                    StructField("name", StringType())]))
        diff = tracker.compare_schemas_for_lineage(src, tgt)
        assert "name" in diff["added"]

    def test_record_repr(self):
        rec = LineageRecord("src","tgt","FULL","job","run1")
        assert rec.source_table == "src"
