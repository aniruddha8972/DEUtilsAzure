"""tests/test_delta.py"""
import pytest
from unittest.mock import MagicMock, patch, call
from de_utils_v2.delta import DeltaUtils

@pytest.fixture()
def du(spark):
    return DeltaUtils(spark)

class TestDeltaUtils:
    def test_non_delta_table_skips_optimize(self, du):
        """optimize() should skip gracefully for non-Delta tables."""
        with patch.object(du, "_is_delta", return_value=False):
            du.optimize("some.parquet_table")   # should not raise

    def test_optimize_calls_sql(self, du):
        with patch.object(du, "_is_delta", return_value=True):
            with patch.object(du.spark, "sql") as mock_sql:
                du.optimize("silver.orders")
                mock_sql.assert_called_once()
                assert "OPTIMIZE" in mock_sql.call_args[0][0]

    def test_optimize_with_zorder(self, du):
        with patch.object(du, "_is_delta", return_value=True):
            with patch.object(du.spark, "sql") as mock_sql:
                du.optimize("silver.orders", zorder_by=["customer_id"])
                sql = mock_sql.call_args[0][0]
                assert "ZORDER" in sql

    def test_vacuum_calls_sql(self, du):
        with patch.object(du, "_is_delta", return_value=True):
            with patch.object(du.spark, "sql") as mock_sql:
                mock_sql.return_value = MagicMock()
                du.vacuum("silver.orders", retention_hours=168)
                sql = mock_sql.call_args[0][0]
                assert "VACUUM" in sql
                assert "168" in sql

    def test_vacuum_dry_run(self, du):
        with patch.object(du, "_is_delta", return_value=True):
            with patch.object(du.spark, "sql") as mock_sql:
                mock_sql.return_value = MagicMock()
                du.vacuum("tbl", dry_run=True)
                assert "DRY RUN" in mock_sql.call_args[0][0]

    def test_restore_by_version(self, du):
        with patch.object(du, "_is_delta", return_value=True):
            with patch.object(du.spark, "sql") as mock_sql:
                du.restore("silver.orders", version=5)
                assert "RESTORE" in mock_sql.call_args[0][0]
                assert "5" in mock_sql.call_args[0][0]

    def test_restore_requires_version_or_timestamp(self, du):
        with patch.object(du, "_is_delta", return_value=True):
            with pytest.raises(ValueError):
                du.restore("tbl")

    def test_clone_shallow(self, du):
        with patch.object(du, "_is_delta", return_value=True):
            with patch.object(du.spark, "sql") as mock_sql:
                du.clone("silver.orders", "silver.orders_bak", shallow=True)
                assert "SHALLOW" in mock_sql.call_args[0][0]

    def test_clone_deep(self, du):
        with patch.object(du, "_is_delta", return_value=True):
            with patch.object(du.spark, "sql") as mock_sql:
                du.clone("silver.orders", "gold.orders", shallow=False)
                assert "DEEP" in mock_sql.call_args[0][0]

    def test_enable_cdf(self, du):
        with patch.object(du.spark, "sql") as mock_sql:
            du.enable_change_data_feed("silver.orders")
            assert "enableChangeDataFeed" in mock_sql.call_args[0][0]

    def test_convert_to_delta(self, du):
        with patch.object(du.spark, "sql") as mock_sql:
            du.convert_to_delta("silver.orders")
            assert "CONVERT TO DELTA" in mock_sql.call_args[0][0]
