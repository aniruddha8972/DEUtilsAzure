"""tests/test_audit.py"""
import pytest
from de_utils_v2.audit import AuditLog, AuditEntry
from datetime import datetime

DB = "test_audit"

@pytest.fixture(autouse=True, scope="module")
def setup_db(spark):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{DB}`")
    yield
    spark.sql(f"DROP DATABASE IF EXISTS `{DB}` CASCADE")

@pytest.fixture()
def audit(spark):
    return AuditLog(spark, audit_table=f"`{DB}`.`audit_log`",
                    job_name="test_job", run_id="run001", auto_flush=True)

class TestAuditLog:
    def test_log_writes_entry(self, spark, audit):
        audit.log("silver.orders","FULL_LOAD", rows_inserted=1000, status="SUCCESS")
        tbl = spark.table(f"`{DB}`.`audit_log`")
        assert tbl.filter("table_name='silver.orders'").count() >= 1

    def test_context_manager_success(self, spark, audit):
        with audit.capture("silver.orders", "MERGE") as e:
            e.rows_updated = 50
        tbl = spark.table(f"`{DB}`.`audit_log`")
        row = tbl.filter("operation='MERGE'").orderBy("started_at", ascending=False).limit(1).collect()[0]
        assert row["status"] == "SUCCESS"
        assert row["rows_updated"] == 50

    def test_context_manager_failure(self, spark, audit):
        with pytest.raises(RuntimeError):
            with audit.capture("silver.orders","FULL_LOAD") as e:
                raise RuntimeError("boom")
        tbl = spark.table(f"`{DB}`.`audit_log`")
        failed = tbl.filter("status='FAILED'").count()
        assert failed >= 1

    def test_duration_recorded(self, spark, audit):
        audit.log("tbl","OP", status="SUCCESS")
        tbl = spark.table(f"`{DB}`.`audit_log`")
        row = tbl.filter("operation='OP'").collect()[0]
        assert row["duration_seconds"] is not None

    def test_get_table_history(self, audit):
        result = audit.get_table_history("silver.orders")
        assert result is not None

    def test_get_failed_jobs(self, audit):
        result = audit.get_failed_jobs()
        assert result is not None

    def test_get_run_summary(self, audit):
        result = audit.get_run_summary()
        assert result is not None

    def test_entry_complete_sets_status(self):
        e = AuditEntry("id","job","run","tbl","OP")
        e.complete("SUCCESS")
        assert e.status == "SUCCESS"
        assert e.completed_at is not None

    def test_entry_fail_sets_error(self):
        e = AuditEntry("id","job","run","tbl","OP")
        e.fail("something broke")
        assert e.status == "FAILED"
        assert "something broke" in e.error_message

    def test_entry_duration(self):
        e = AuditEntry("id","job","run","tbl","OP")
        e.complete()
        assert e.duration_seconds >= 0

    def test_log_from_scd_result(self, audit):
        from de_utils_v2.scd import SCD2Result
        result = SCD2Result("dim_cust", 10, 5, "2024-01-01")
        entry = audit.log_from_scd_result(result)
        assert entry.rows_inserted == 10
        assert entry.rows_updated == 5
