"""tests/test_config_loader.py"""
import os
import pytest
from de_utils_v2.config_loader import ConfigLoader, _interpolate_env, _deep_interpolate
from de_utils_v2.utils import DataEngineeringError

class TestEnvInterpolation:
    def test_replaces_env_var(self, monkeypatch):
        monkeypatch.setenv("MY_VAR", "hello")
        result = _interpolate_env("prefix_${MY_VAR}_suffix")
        assert result == "prefix_hello_suffix"

    def test_raises_on_missing_env_var(self):
        with pytest.raises(DataEngineeringError, match="MISSING_VAR_XYZ"):
            _interpolate_env("${MISSING_VAR_XYZ}")

    def test_non_string_passthrough(self):
        assert _interpolate_env(42) == 42
        assert _interpolate_env(None) is None

    def test_no_substitution_needed(self):
        assert _interpolate_env("plain_value") == "plain_value"

    def test_deep_interpolate_dict(self, monkeypatch):
        monkeypatch.setenv("DB_NAME", "silver")
        d = {"database": "${DB_NAME}", "nested": {"key": "plain"}}
        result = _deep_interpolate(d)
        assert result["database"] == "silver"
        assert result["nested"]["key"] == "plain"

    def test_deep_interpolate_list(self, monkeypatch):
        monkeypatch.setenv("V", "value")
        result = _deep_interpolate(["${V}", "plain"])
        assert result == ["value", "plain"]


class TestAdlsFromEnv:
    def test_loads_from_env(self, monkeypatch):
        monkeypatch.setenv("DE_ADLS_ACCOUNT_NAME", "myacct")
        monkeypatch.setenv("DE_ADLS_CONTAINER", "mycontainer")
        monkeypatch.setenv("DE_ADLS_ACCOUNT_KEY", "mykey")
        cfg = ConfigLoader.adls_from_env()
        assert cfg.account_name == "myacct"
        assert cfg.container == "mycontainer"

    def test_raises_on_missing_account_name(self, monkeypatch):
        monkeypatch.delenv("DE_ADLS_ACCOUNT_NAME", raising=False)
        monkeypatch.setenv("DE_ADLS_CONTAINER", "c")
        with pytest.raises(DataEngineeringError):
            ConfigLoader.adls_from_env()

    def test_raises_on_missing_container(self, monkeypatch):
        monkeypatch.setenv("DE_ADLS_ACCOUNT_NAME", "acct")
        monkeypatch.delenv("DE_ADLS_CONTAINER", raising=False)
        with pytest.raises(DataEngineeringError):
            ConfigLoader.adls_from_env()

    def test_custom_prefix(self, monkeypatch):
        monkeypatch.setenv("CUSTOM_ACCOUNT_NAME", "custom")
        monkeypatch.setenv("CUSTOM_CONTAINER", "ctn")
        cfg = ConfigLoader.adls_from_env(prefix="CUSTOM_")
        assert cfg.account_name == "custom"


class TestHiveFromEnv:
    def test_loads_defaults(self):
        cfg = ConfigLoader.hive_from_env()
        assert cfg.metastore_uri == "thrift://localhost:9083"
        assert cfg.database == "default"

    def test_overrides_from_env(self, monkeypatch):
        monkeypatch.setenv("DE_HIVE_METASTORE_URI", "thrift://meta:9083")
        monkeypatch.setenv("DE_HIVE_DATABASE", "gold")
        cfg = ConfigLoader.hive_from_env()
        assert cfg.metastore_uri == "thrift://meta:9083"
        assert cfg.database == "gold"


class TestFromYaml:
    def test_missing_pyyaml_raises(self, monkeypatch, tmp_path):
        import builtins
        real_import = builtins.__import__
        def mock_import(name, *args, **kwargs):
            if name == "yaml":
                raise ImportError("no yaml")
            return real_import(name, *args, **kwargs)
        monkeypatch.setattr(builtins, "__import__", mock_import)
        with pytest.raises(ImportError, match="PyYAML"):
            ConfigLoader.from_yaml(str(tmp_path / "nonexistent.yml"))

    def test_dump_sample_yaml(self, tmp_path):
        path = str(tmp_path / "sample.yml")
        ConfigLoader.dump_sample_yaml(path)
        assert os.path.exists(path)
        with open(path) as f:
            content = f.read()
        assert "adls" in content
        assert "hive" in content

    def test_parse_adls_from_dict(self):
        raw = {"account_name": "a", "container": "b", "account_key": "k"}
        cfg = ConfigLoader._parse_adls(raw)
        assert cfg.account_name == "a"
        assert cfg.account_key == "k"

    def test_parse_hive_from_dict(self):
        raw = {"metastore_uri": "thrift://x:9083", "database": "silver"}
        cfg = ConfigLoader._parse_hive(raw)
        assert cfg.database == "silver"

    def test_parse_empty_adls_returns_none(self):
        assert ConfigLoader._parse_adls({}) is None

    def test_parse_empty_hive_returns_none(self):
        assert ConfigLoader._parse_hive({}) is None
