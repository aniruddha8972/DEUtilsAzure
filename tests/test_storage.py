"""
tests/test_storage.py
---------------------
Tests for ADLSConnector.
Real ADLS calls are mocked; Spark reads/writes use local paths.
"""

import pytest
from unittest.mock import MagicMock, patch, PropertyMock
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType
from de_utils.config import ADLSConfig
from de_utils.storage import ADLSConnector


@pytest.fixture()
def adls_cfg():
    return ADLSConfig(
        account_name="testacct",
        container="testcontainer",
        client_id="client-id",
        client_secret="secret",
        tenant_id="tenant-id",
        root_path="/data",
    )


@pytest.fixture()
def connector(adls_cfg, spark):
    """ADLSConnector with Spark conf setting mocked out."""
    with patch.object(ADLSConnector, "_configure_spark", return_value=None):
        return ADLSConnector(adls_cfg, spark)


@pytest.fixture()
def sample_df(spark):
    schema = StructType([
        StructField("id",   LongType()),
        StructField("name", StringType()),
        StructField("val",  DoubleType()),
    ])
    return spark.createDataFrame([(1, "A", 1.1), (2, "B", 2.2)], schema=schema)


class TestAbfssPath:

    def test_abfss_method_returns_correct_uri(self, connector):
        path = connector.abfss("orders", "2024")
        assert path.startswith("abfss://testcontainer@testacct.dfs.core.windows.net")
        assert "orders" in path
        assert "2024" in path


class TestConfigureSpark:

    def test_configure_spark_calls_conf_set(self, adls_cfg, spark):
        """_configure_spark pushes all ADLS conf keys into Spark."""
        set_calls = []
        original_set = spark.conf.set

        def tracking_set(k, v):
            set_calls.append(k)
            original_set(k, v)

        with patch.object(spark.conf, "set", side_effect=tracking_set):
            ADLSConnector(adls_cfg, spark)

        assert any("azure" in k.lower() for k in set_calls)


class TestReadWriteParquet:

    def test_write_and_read_parquet(self, connector, spark, sample_df, tmp_path):
        """Round-trip write/read using a local tmp path (bypasses ADLS)."""
        out = str(tmp_path / "parquet_out")
        sample_df.write.mode("overwrite").parquet(out)
        result = spark.read.parquet(out)
        assert result.count() == 2
        assert set(result.columns) == {"id", "name", "val"}

    def test_write_parquet_with_partition(self, connector, spark, sample_df, tmp_path):
        out = str(tmp_path / "parquet_part")
        sample_df.write.mode("overwrite").partitionBy("id").parquet(out)
        result = spark.read.parquet(out)
        assert result.count() == 2


class TestReadWriteCsv:

    def test_write_and_read_csv(self, spark, sample_df, tmp_path):
        out = str(tmp_path / "csv_out")
        sample_df.write.mode("overwrite").option("header", True).csv(out)
        result = spark.read.option("header", True).option("inferSchema", True).csv(out)
        assert result.count() == 2


class TestFileSystemHelpers:

    def test_path_exists_returns_true_when_exists(self, connector):
        mock_fs = MagicMock()
        mock_file_client = MagicMock()
        mock_file_client.get_file_properties.return_value = {}
        mock_fs.get_file_client.return_value = mock_file_client

        with patch.object(connector, "_get_fs_client", return_value=mock_fs):
            assert connector.path_exists("/data/orders") is True

    def test_path_exists_returns_false_when_missing(self, connector):
        mock_fs = MagicMock()
        mock_file_client = MagicMock()
        mock_file_client.get_file_properties.side_effect = Exception("not found")
        mock_fs.get_file_client.return_value = mock_file_client

        with patch.object(connector, "_get_fs_client", return_value=mock_fs):
            assert connector.path_exists("/data/missing") is False

    def test_list_paths_returns_names(self, connector):
        mock_fs = MagicMock()
        mock_path_a = MagicMock()
        mock_path_a.name = "data/orders/2024"
        mock_path_b = MagicMock()
        mock_path_b.name = "data/customers/2024"
        mock_fs.get_paths.return_value = [mock_path_a, mock_path_b]

        with patch.object(connector, "_get_fs_client", return_value=mock_fs):
            paths = connector.list_paths("/data/")
            assert "data/orders/2024" in paths
            assert len(paths) == 2

    def test_delete_path_calls_sdk(self, connector):
        mock_fs = MagicMock()
        mock_dir_client = MagicMock()
        mock_fs.get_directory_client.return_value = mock_dir_client

        with patch.object(connector, "_get_fs_client", return_value=mock_fs):
            connector.delete_path("/data/tmp/")
            mock_dir_client.delete_directory.assert_called_once()

    def test_create_directory_calls_sdk(self, connector):
        mock_fs = MagicMock()
        with patch.object(connector, "_get_fs_client", return_value=mock_fs):
            connector.create_directory("/data/new_dir/")
            mock_fs.create_directory.assert_called_once()

    def test_get_fs_client_raises_without_auth(self, adls_cfg, spark):
        cfg = ADLSConfig(account_name="x", container="y")  # no auth
        with patch.object(ADLSConnector, "_configure_spark", return_value=None):
            conn = ADLSConnector(cfg, spark)
        with pytest.raises(Exception):
            conn._get_fs_client()

    def test_get_fs_client_missing_sdk_raises(self, connector):
        import builtins
        real_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if "azure.storage.filedatalake" in name:
                raise ImportError("mocked missing azure sdk")
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=mock_import):
            with pytest.raises(ImportError, match="azure-storage-file-datalake"):
                connector._get_fs_client()
