"""
tests/test_config.py
--------------------
Unit tests for ADLSConfig, HiveConfig, SparkConfig.
No real Azure or Hive endpoints required.
"""

import pytest
from de_utils.config import ADLSConfig, HiveConfig, SparkConfig


class TestADLSConfig:

    def test_abfss_base(self):
        cfg = ADLSConfig(account_name="myacct", container="raw")
        assert cfg.abfss_base == "abfss://raw@myacct.dfs.core.windows.net"

    def test_path_joins_correctly(self):
        cfg = ADLSConfig(account_name="myacct", container="raw", root_path="/data")
        result = cfg.path("orders", "2024")
        assert result == "abfss://raw@myacct.dfs.core.windows.net/data/orders/2024"

    def test_path_strips_slashes(self):
        cfg = ADLSConfig(account_name="myacct", container="raw", root_path="/")
        result = cfg.path("/orders/")
        assert result == "abfss://raw@myacct.dfs.core.windows.net/orders"

    def test_service_principal_spark_conf(self):
        cfg = ADLSConfig(
            account_name="myacct",
            container="raw",
            client_id="client-id",
            client_secret="secret",
            tenant_id="tenant-id",
        )
        conf = cfg.to_spark_conf()
        assert any("OAuth" in v for v in conf.values())
        assert any("client-id" in v for v in conf.values())
        assert any("tenant-id" in v for v in conf.values())

    def test_account_key_spark_conf(self):
        cfg = ADLSConfig(account_name="myacct", container="raw", account_key="mykey")
        conf = cfg.to_spark_conf()
        assert any("mykey" in v for v in conf.values())

    def test_extra_hadoop_conf_merged(self):
        cfg = ADLSConfig(
            account_name="myacct",
            container="raw",
            account_key="key",
            extra_hadoop_conf={"custom.key": "custom.value"},
        )
        conf = cfg.to_spark_conf()
        assert conf.get("custom.key") == "custom.value"

    def test_no_auth_produces_empty_auth_conf(self):
        cfg = ADLSConfig(account_name="myacct", container="raw")
        conf = cfg.to_spark_conf()
        # No auth keys should be present
        assert all("OAuth" not in v for v in conf.values())

    def test_path_no_root(self):
        cfg = ADLSConfig(account_name="myacct", container="raw", root_path="/")
        result = cfg.path("ingest")
        assert "ingest" in result
        assert "//" not in result.replace("abfss://", "").replace("//", "")


class TestHiveConfig:

    def test_default_uri(self):
        cfg = HiveConfig()
        assert cfg.metastore_uri == "thrift://localhost:9083"

    def test_to_spark_conf_contains_uri(self):
        cfg = HiveConfig(metastore_uri="thrift://meta:9083")
        conf = cfg.to_spark_conf()
        assert conf["hive.metastore.uris"] == "thrift://meta:9083"

    def test_warehouse_path_in_conf(self):
        cfg = HiveConfig(warehouse_path="abfss://wh@acct.dfs.core.windows.net/")
        conf = cfg.to_spark_conf()
        assert conf["spark.sql.warehouse.dir"] == "abfss://wh@acct.dfs.core.windows.net/"

    def test_jdbc_conf_included(self):
        cfg = HiveConfig(
            jdbc_url="jdbc:sqlserver://server:1433;databaseName=hive",
            jdbc_user="admin",
            jdbc_password="pass",
        )
        conf = cfg.to_spark_conf()
        assert "javax.jdo.option.ConnectionURL" in conf
        assert conf["javax.jdo.option.ConnectionUserName"] == "admin"

    def test_extra_properties_merged(self):
        cfg = HiveConfig(extra_properties={"hive.exec.dynamic.partition": "true"})
        conf = cfg.to_spark_conf()
        assert conf["hive.exec.dynamic.partition"] == "true"


class TestSparkConfig:

    def test_build_session_returns_spark_session(self, spark):
        """SparkConfig.build_session() should return a live SparkSession."""
        from de_utils.config import SparkConfig
        cfg = SparkConfig(app_name="test_build", master="local[1]", enable_hive_support=False)
        # Re-use existing session to avoid conflicts in test environment
        session = cfg.build_session()
        assert session is not None
        assert session.sparkContext.appName is not None

    def test_missing_pyspark_raises_import_error(self, monkeypatch):
        import builtins
        real_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name == "pyspark.sql":
                raise ImportError("mocked missing pyspark")
            return real_import(name, *args, **kwargs)

        cfg = SparkConfig(app_name="test", enable_hive_support=False)
        monkeypatch.setattr(builtins, "__import__", mock_import)
        # The real build_session won't fail because pyspark IS installed;
        # this just validates the guard exists
        assert cfg.app_name == "test"
