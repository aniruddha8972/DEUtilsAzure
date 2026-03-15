"""tests/test_checkpoint.py"""
import pytest
from de_utils_v2.checkpoint import JobCheckpoint

DB = "test_checkpoint"

@pytest.fixture(autouse=True, scope="module")
def setup_db(spark):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{DB}`")
    yield
    spark.sql(f"DROP DATABASE IF EXISTS `{DB}` CASCADE")

@pytest.fixture()
def cp(spark):
    return JobCheckpoint(spark, state_table=f"`{DB}`.`job_state`",
                         job_name="test_job", run_id="run001")

class TestJobCheckpoint:
    def test_get_returns_none_before_set(self, cp):
        assert cp.get("never_set_key_xyz") is None

    def test_set_and_get_before_commit(self, cp):
        cp.set("my_key", "hello")
        assert cp.get("my_key") == "hello"

    def test_commit_persists_value(self, spark, cp):
        cp.set("wm::col", "2024-06-01")
        cp.commit()
        cp2 = JobCheckpoint(spark, f"`{DB}`.`job_state`", "test_job")
        assert cp2.get_watermark("col") == "2024-06-01"

    def test_rollback_discards_pending(self, cp):
        cp.set("discarded_key", "value")
        cp.rollback()
        assert cp.get("discarded_key") is None

    def test_watermark_helpers(self, cp):
        cp.set_watermark("updated_at", "2024-07-01")
        assert cp.get_watermark("updated_at") == "2024-07-01"

    def test_partition_done_tracking(self, spark, cp):
        cp.mark_partition_done("2024-01-01")
        cp.commit()
        cp2 = JobCheckpoint(spark, f"`{DB}`.`job_state`", "test_job")
        assert cp2.is_already_run("2024-01-01") is True

    def test_is_already_run_false_for_new(self, cp):
        assert cp.is_already_run("9999-01-01") is False

    def test_get_all_returns_dict(self, cp):
        cp.set("k1", "v1")
        result = cp.get_all()
        assert isinstance(result, dict)

    def test_default_value_returned(self, cp):
        val = cp.get("nonexistent", default="fallback")
        assert val == "fallback"

    def test_repr(self, cp):
        assert "test_job" in repr(cp)

    def test_set_last_run_status(self, cp):
        cp.set_last_run_status("SUCCESS", "all good")
        assert cp.get("last_run_status") == "SUCCESS"
