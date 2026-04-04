"""
tests/test_databricks.py
------------------------
Tests for the Databricks compatibility layer.
Most tests mock dbutils and use local Spark.
"""

import os
import pytest
from unittest.mock import MagicMock, patch, PropertyMock
from de_utils.databricks import (
    is_databricks,
    is_community_edition,
    DatabricksConfig,
    DatabricksSession,
    DatabricksSecrets,
    DBFSConnector,
    DatabricksTableOps,
    DatabricksLoader,
    DatabricksCheckpoint,
    DatabricksAuditLog,
    NotebookUtils,
    DBLoadResult,
)
from de_utils.utils import DataEngineeringError


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture()
def mock_dbutils():
    du = MagicMock()
    du.secrets.get.return_value = "mock_secret_value"
    du.secrets.list.return_value = [MagicMock(key="test-key")]
    du.fs.ls.return_value = [MagicMock(path="dbfs:/test/file.parquet")]
    du.fs.mounts.return_value = []
    du.widgets.get.return_value = "widget_value"
    return du


@pytest.fixture()
def orders_df(spark):
    from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType
    schema = StructType([
        StructField("order_id",   LongType()),
        StructField("status",     StringType()),
        StructField("amount",     DoubleType()),
    ])
    return spark.createDataFrame(
        [(1, "PENDING", 99.9), (2, "SHIPPED", 150.0), (3, "DELIVERED", 200.0)],
        schema=schema,
    )


DB = "test_databricks_db"


@pytest.fixture(autouse=True, scope="module")
def setup_db(spark):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{DB}`")
    yield
    spark.sql(f"DROP DATABASE IF EXISTS `{DB}` CASCADE")


# ── Environment detection ─────────────────────────────────────────────────────

class TestEnvironmentDetection:

    def test_is_databricks_false_locally(self, monkeypatch):
        monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)
        monkeypatch.delenv("DB_HOME", raising=False)
        assert is_databricks() is False

    def test_is_databricks_true_with_env_var(self, monkeypatch):
        monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "13.3.x-scala2.12")
        assert is_databricks() is True

    def test_is_community_false_locally(self, monkeypatch):
        monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)
        assert is_community_edition() is False


# ── DatabricksConfig ──────────────────────────────────────────────────────────

class TestDatabricksConfig:

    def test_default_dbfs_root(self):
        cfg = DatabricksConfig()
        assert cfg.dbfs_root == "dbfs:/de_utils"

    def test_custom_dbfs_root(self):
        cfg = DatabricksConfig(dbfs_root="dbfs:/myproject")
        assert cfg.dbfs_root == "dbfs:/myproject"

    def test_path_joins_correctly(self):
        cfg = DatabricksConfig(dbfs_root="dbfs:/de_utils")
        assert cfg.path("silver", "orders") == "dbfs:/de_utils/silver/orders"

    def test_path_strips_slashes(self):
        cfg = DatabricksConfig(dbfs_root="dbfs:/de_utils/")
        result = cfg.path("/orders/")
        assert "//" not in result.replace("dbfs://", "")

    def test_table_name_no_catalog(self):
        cfg = DatabricksConfig(default_database="silver")
        assert cfg.table_name("orders") == "`silver`.`orders`"

    def test_table_name_with_catalog(self):
        cfg = DatabricksConfig(default_database="silver", catalog="main")
        assert cfg.table_name("orders") == "`main`.`silver`.`orders`"

    def test_mount_path_raises_without_mount(self):
        cfg = DatabricksConfig()
        with pytest.raises(DataEngineeringError):
            cfg.mount_path("orders")

    def test_mount_path_with_mount(self):
        cfg = DatabricksConfig(mount_point="/mnt/datalake")
        assert cfg.mount_path("silver", "orders") == "/mnt/datalake/silver/orders"

    def test_repr_contains_key_info(self):
        cfg = DatabricksConfig(dbfs_root="dbfs:/test")
        r = repr(cfg)
        assert "dbfs:/test" in r
        assert "community" in r


# ── DatabricksSession ─────────────────────────────────────────────────────────

class TestDatabricksSession:

    def test_get_returns_spark_session(self, spark):
        session = DatabricksSession.get()
        assert session is not None

    def test_get_require_databricks_raises_locally(self, monkeypatch):
        monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)
        monkeypatch.delenv("DB_HOME", raising=False)
        with pytest.raises(DataEngineeringError, match="Databricks"):
            DatabricksSession.get(require_databricks=True)

    def test_set_shuffle_partitions(self, spark):
        DatabricksSession.set_shuffle_partitions(spark, n=4)
        assert spark.conf.get("spark.sql.shuffle.partitions") == "4"

    def test_configure_for_delta_does_not_raise(self, spark):
        DatabricksSession.configure_for_delta(spark)   # should not raise


# ── DatabricksSecrets ─────────────────────────────────────────────────────────

class TestDatabricksSecrets:

    def test_get_returns_secret(self, mock_dbutils):
        secrets = DatabricksSecrets(scope="test-scope", dbutils=mock_dbutils)
        assert secrets.get("test-key") == "mock_secret_value"

    def test_get_returns_none_when_not_found(self, mock_dbutils):
        mock_dbutils.secrets.get.side_effect = Exception("SecretDoesNotExist")
        secrets = DatabricksSecrets(scope="test-scope", dbutils=mock_dbutils)
        assert secrets.get("missing") is None

    def test_get_required_raises_when_none(self, mock_dbutils):
        mock_dbutils.secrets.get.side_effect = Exception("SecretDoesNotExist")
        secrets = DatabricksSecrets(scope="test-scope", dbutils=mock_dbutils)
        with pytest.raises(DataEngineeringError):
            secrets.get_required("missing")

    def test_get_required_returns_value(self, mock_dbutils):
        secrets = DatabricksSecrets(scope="test-scope", dbutils=mock_dbutils)
        assert secrets.get_required("test-key") == "mock_secret_value"

    def test_list_keys(self, mock_dbutils):
        secrets = DatabricksSecrets(scope="test-scope", dbutils=mock_dbutils)
        keys = secrets.list_keys()
        assert "test-key" in keys

    def test_raises_without_dbutils(self):
        secrets = DatabricksSecrets(scope="test-scope", dbutils=None)
        with pytest.raises(DataEngineeringError, match="dbutils"):
            secrets.get("any-key")


# ── DBFSConnector ─────────────────────────────────────────────────────────────

class TestDBFSConnector:

    def test_read_write_parquet_round_trip(self, spark, orders_df, tmp_path):
        conn = DBFSConnector(spark)
        path = str(tmp_path / "orders_parquet")
        conn.write_parquet(orders_df, path)
        result = conn.read_parquet(path)
        assert result.count() == 3

    def test_read_write_delta_round_trip(self, spark, orders_df, tmp_path):
        conn = DBFSConnector(spark)
        path = str(tmp_path / "orders_delta")
        conn.write_delta(orders_df, path)
        result = conn.read_delta(path)
        assert result.count() == 3

    def test_write_table_registers_table(self, spark, orders_df):
        conn = DBFSConnector(spark)
        conn.write_table(orders_df, f"`{DB}`.`dbfs_test_table`", mode="overwrite")
        result = spark.table(f"`{DB}`.`dbfs_test_table`")
        assert result.count() == 3

    def test_write_delta_with_partition(self, spark, orders_df, tmp_path):
        conn = DBFSConnector(spark)
        path = str(tmp_path / "partitioned")
        conn.write_delta(orders_df, path, partition_by=["status"])
        result = conn.read_delta(path)
        assert result.count() == 3

    def test_path_exists_returns_false_without_dbutils(self, spark):
        conn = DBFSConnector(spark)
        # Without dbutils, defaults to False
        assert conn.path_exists("dbfs:/nonexistent/path") is False

    def test_list_paths_with_mock_dbutils(self, spark, mock_dbutils):
        conn = DBFSConnector(spark)
        paths = conn.list_paths("dbfs:/test/", dbutils=mock_dbutils)
        assert "dbfs:/test/file.parquet" in paths

    def test_delete_path_calls_dbutils(self, spark, mock_dbutils):
        conn = DBFSConnector(spark)
        conn.delete_path("dbfs:/test/tmp", dbutils=mock_dbutils)
        mock_dbutils.fs.rm.assert_called_once()

    def test_copy_path_calls_dbutils(self, spark, mock_dbutils):
        conn = DBFSConnector(spark)
        conn.copy_path("dbfs:/src", "dbfs:/dst", dbutils=mock_dbutils)
        mock_dbutils.fs.cp.assert_called_once()


# ── DatabricksTableOps ────────────────────────────────────────────────────────

class TestDatabricksTableOps:

    def test_create_or_replace(self, spark, orders_df):
        ops = DatabricksTableOps(spark, database=DB)
        ops.create_or_replace("ops_orders", orders_df)
        assert spark.catalog.tableExists(f"`{DB}`.`ops_orders`")

    def test_create_if_not_exists_returns_true_on_create(self, spark, orders_df):
        ops = DatabricksTableOps(spark, database=DB)
        spark.sql(f"DROP TABLE IF EXISTS `{DB}`.`cine_ops`")
        created = ops.create_if_not_exists("cine_ops", orders_df)
        assert created is True

    def test_create_if_not_exists_returns_false_when_exists(self, spark, orders_df):
        ops = DatabricksTableOps(spark, database=DB)
        ops.create_or_replace("cine_exists_ops", orders_df)
        created = ops.create_if_not_exists("cine_exists_ops", orders_df)
        assert created is False

    def test_insert_append(self, spark, orders_df):
        ops = DatabricksTableOps(spark, database=DB)
        ops.create_or_replace("append_ops", orders_df)
        ops.insert_append("append_ops", orders_df)
        assert spark.table(f"`{DB}`.`append_ops`").count() == 6

    def test_merge_inserts_new_rows(self, spark, orders_df):
        from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType
        schema = orders_df.schema
        ops = DatabricksTableOps(spark, database=DB)
        ops.create_or_replace("merge_ops", orders_df)
        new_row = spark.createDataFrame([(99, "PENDING", 5.0)], schema=schema)
        ops.merge("merge_ops", new_row, match_keys=["order_id"])
        assert spark.table(f"`{DB}`.`merge_ops`").count() == 4

    def test_merge_updates_existing_rows(self, spark, orders_df):
        schema = orders_df.schema
        ops = DatabricksTableOps(spark, database=DB)
        ops.create_or_replace("merge_upd_ops", orders_df)
        updated = spark.createDataFrame([(1, "CANCELLED", 0.0)], schema=schema)
        ops.merge("merge_upd_ops", updated, match_keys=["order_id"])
        row = spark.table(f"`{DB}`.`merge_upd_ops`").filter("order_id = 1").collect()[0]
        assert row["status"] == "CANCELLED"

    def test_drop_table(self, spark, orders_df):
        ops = DatabricksTableOps(spark, database=DB)
        ops.create_or_replace("drop_me_ops", orders_df)
        ops.drop_table("drop_me_ops")
        assert not spark.catalog.tableExists(f"`{DB}`.`drop_me_ops`")

    def test_list_tables(self, spark, orders_df):
        ops = DatabricksTableOps(spark, database=DB)
        ops.create_or_replace("list_me_ops", orders_df)
        tables = ops.list_tables()
        assert "list_me_ops" in tables

    def test_fqn_without_catalog(self):
        ops = DatabricksTableOps(MagicMock(), database="silver")
        assert ops._fqn("orders") == "`silver`.`orders`"

    def test_fqn_with_catalog(self):
        ops = DatabricksTableOps(MagicMock(), database="silver", catalog="main")
        assert ops._fqn("orders") == "`main`.`silver`.`orders`"


# ── DatabricksLoader ──────────────────────────────────────────────────────────

class TestDatabricksLoader:

    def test_full_load_returns_result(self, spark, orders_df):
        loader = DatabricksLoader(spark, database=DB)
        result = loader.full_load(orders_df, "dl_orders")
        assert isinstance(result, DBLoadResult)
        assert result.load_type == "FULL"
        assert result.rows_inserted == 3

    def test_full_load_creates_table(self, spark, orders_df):
        loader = DatabricksLoader(spark, database=DB)
        loader.full_load(orders_df, "dl_created")
        assert spark.catalog.tableExists(f"`{DB}`.`dl_created`")

    def test_incremental_load_appends(self, spark, orders_df):
        loader = DatabricksLoader(spark, database=DB)
        loader.full_load(orders_df, "dl_inc")
        new_row = spark.createDataFrame([(99, "PENDING", 5.0)], schema=orders_df.schema)
        loader.incremental_load(new_row, "dl_inc",
                                watermark_col="order_id", last_watermark="3")
        assert spark.table(f"`{DB}`.`dl_inc`").count() == 4

    def test_watermark_filters_old_rows(self, spark, orders_df):
        loader = DatabricksLoader(spark, database=DB)
        loader.full_load(orders_df, "dl_wm")
        # All rows have order_id <= 3, watermark=3 means none pass
        result = loader.incremental_load(orders_df, "dl_wm",
                                         watermark_col="order_id",
                                         last_watermark="3")
        assert result.rows_inserted == 0

    def test_merge_load_upserts(self, spark, orders_df):
        loader = DatabricksLoader(spark, database=DB)
        loader.full_load(orders_df, "dl_merge")
        updates = spark.createDataFrame([(1, "CANCELLED", 0.0), (99, "NEW", 1.0)],
                                        schema=orders_df.schema)
        loader.merge_load(updates, "dl_merge", match_keys=["order_id"])
        tbl = spark.table(f"`{DB}`.`dl_merge`")
        assert tbl.count() == 4
        assert tbl.filter("order_id = 1").collect()[0]["status"] == "CANCELLED"

    def test_snapshot_load_adds_date_column(self, spark, orders_df):
        loader = DatabricksLoader(spark, database=DB)
        loader.full_load(orders_df, "dl_snap")
        loader.snapshot_load(orders_df, "dl_snap", snapshot_date="2024-06-01")
        tbl = spark.table(f"`{DB}`.`dl_snap`")
        assert "snapshot_date" in tbl.columns


# ── DatabricksCheckpoint ──────────────────────────────────────────────────────

class TestDatabricksCheckpoint:

    def test_get_watermark_returns_none_initially(self, spark):
        cp = DatabricksCheckpoint(spark, table=f"`{DB}`.`cp_state`",
                                  job_name="test_job_1")
        assert cp.get_watermark("updated_at") is None

    def test_set_and_get_watermark(self, spark):
        cp = DatabricksCheckpoint(spark, table=f"`{DB}`.`cp_state2`",
                                  job_name="test_job_2")
        cp.set_watermark("updated_at", "2024-06-01")
        cp.commit()
        assert cp.get_watermark("updated_at") == "2024-06-01"

    def test_idempotent_partition(self, spark):
        cp = DatabricksCheckpoint(spark, table=f"`{DB}`.`cp_state3`",
                                  job_name="test_job_3")
        assert cp.is_already_run("2024-06-01") is False
        cp.mark_partition_done("2024-06-01")
        assert cp.is_already_run("2024-06-01") is True

    def test_arbitrary_key_value(self, spark):
        cp = DatabricksCheckpoint(spark, table=f"`{DB}`.`cp_state4`",
                                  job_name="test_job_4")
        cp.set("row_count", "12345")
        cp.commit()
        assert cp.get("row_count") == "12345"

    def test_pending_not_visible_before_commit(self, spark):
        cp = DatabricksCheckpoint(spark, table=f"`{DB}`.`cp_state5`",
                                  job_name="test_job_5")
        cp.set_watermark("ts", "2024-01-01")
        # Not committed yet
        assert cp.get_watermark("ts") is None
        cp.commit()
        assert cp.get_watermark("ts") == "2024-01-01"


# ── DatabricksAuditLog ────────────────────────────────────────────────────────

class TestDatabricksAuditLog:

    def test_log_writes_entry(self, spark):
        audit = DatabricksAuditLog(spark, table=f"`{DB}`.`audit_tbl`",
                                   job_name="test_audit")
        audit.log("silver.orders", "FULL_LOAD", rows_inserted=100)
        result = spark.table(f"`{DB}`.`audit_tbl`")
        assert result.count() >= 1

    def test_context_manager_success(self, spark, orders_df):
        audit = DatabricksAuditLog(spark, table=f"`{DB}`.`audit_ctx`",
                                   job_name="test_ctx")
        with audit.capture("silver.orders", "MERGE") as entry:
            entry.rows_inserted = 50
            entry.rows_updated  = 10
        result = spark.table(f"`{DB}`.`audit_ctx`")
        assert result.filter("status = 'SUCCESS'").count() == 1

    def test_context_manager_failure_logged(self, spark):
        audit = DatabricksAuditLog(spark, table=f"`{DB}`.`audit_fail`",
                                   job_name="test_fail")
        with pytest.raises(ValueError):
            with audit.capture("silver.orders", "MERGE"):
                raise ValueError("intentional failure")
        result = spark.table(f"`{DB}`.`audit_fail`")
        assert result.filter("status = 'FAILED'").count() == 1


# ── NotebookUtils ─────────────────────────────────────────────────────────────

class TestNotebookUtils:

    def test_widget_text_calls_dbutils(self, mock_dbutils):
        nu = NotebookUtils(mock_dbutils)
        nu.widget_text("env", "dev", "Environment")
        mock_dbutils.widgets.text.assert_called_once_with("env", "dev", "Environment")

    def test_get_widget_returns_value(self, mock_dbutils):
        nu = NotebookUtils(mock_dbutils)
        assert nu.get_widget("env") == "widget_value"

    def test_ls_returns_paths(self, mock_dbutils):
        nu = NotebookUtils(mock_dbutils)
        paths = nu.ls("dbfs:/test/")
        assert len(paths) > 0

    def test_get_secret_calls_dbutils(self, mock_dbutils):
        nu = NotebookUtils(mock_dbutils)
        val = nu.get_secret("my-scope", "my-key")
        assert val == "mock_secret_value"
        mock_dbutils.secrets.get.assert_called_with(scope="my-scope", key="my-key")

    def test_raises_without_dbutils(self):
        nu = NotebookUtils(dbutils=None)
        with pytest.raises(DataEngineeringError, match="dbutils"):
            nu.get_widget("x")

    def test_exit_calls_dbutils(self, mock_dbutils):
        nu = NotebookUtils(mock_dbutils)
        nu.exit("SUCCESS")
        mock_dbutils.notebook.exit.assert_called_with("SUCCESS")

    def test_mkdirs_calls_dbutils(self, mock_dbutils):
        nu = NotebookUtils(mock_dbutils)
        nu.mkdirs("dbfs:/test/new_dir")
        mock_dbutils.fs.mkdirs.assert_called_with("dbfs:/test/new_dir")


# ── DBLoadResult ─────────────────────────────────────────────────────────────

class TestDBLoadResult:

    def test_repr_contains_table_and_type(self):
        from datetime import timedelta
        r = DBLoadResult("silver.orders", "FULL", 1000, 0, timedelta(seconds=5))
        assert "silver.orders" in repr(r)
        assert "FULL" in repr(r)
        assert "1000" in repr(r)
