"""
de_utils.config_loader
-----------------------
Load ADLSConfig, HiveConfig, and SparkConfig from YAML files or
environment variables — no hardcoded credentials in code.

YAML format
-----------
    adls:
      account_name: myadlsaccount
      container: raw
      client_id: ${AZURE_CLIENT_ID}       # env var interpolation
      client_secret: ${AZURE_CLIENT_SECRET}
      tenant_id: ${AZURE_TENANT_ID}
      root_path: /data

    hive:
      metastore_uri: thrift://meta:9083
      database: silver
      warehouse_path: abfss://wh@acct.dfs.core.windows.net/

    spark:
      app_name: MyETLJob
      master: local[*]
      enable_hive_support: true
      extra_conf:
        spark.sql.shuffle.partitions: "200"

Environment variable format
---------------------------
    DE_ADLS_ACCOUNT_NAME=myadlsaccount
    DE_ADLS_CONTAINER=raw
    DE_ADLS_CLIENT_ID=...
    DE_ADLS_CLIENT_SECRET=...
    DE_ADLS_TENANT_ID=...
    DE_HIVE_METASTORE_URI=thrift://meta:9083
    DE_HIVE_DATABASE=silver
    DE_SPARK_APP_NAME=MyETLJob

Usage
-----
>>> from de_utils.config_loader import ConfigLoader
>>> spark_cfg = ConfigLoader.from_yaml("pipeline.yml")
>>> spark = spark_cfg.build_session()

>>> # Or from environment
>>> adls_cfg = ConfigLoader.adls_from_env()
>>> hive_cfg = ConfigLoader.hive_from_env()
"""

from __future__ import annotations

import os
import re
from typing import Any, Dict, Optional

from .config import ADLSConfig, HiveConfig, SparkConfig
from .utils import get_logger, DataEngineeringError

log = get_logger(__name__)

_ENV_VAR_PATTERN = re.compile(r"\$\{([^}]+)\}")


def _interpolate_env(value: Any) -> Any:
    """Replace ${VAR_NAME} placeholders with environment variable values."""
    if not isinstance(value, str):
        return value
    def replace(match):
        var = match.group(1)
        val = os.environ.get(var)
        if val is None:
            raise DataEngineeringError(
                f"Environment variable '{var}' referenced in config but not set."
            )
        return val
    return _ENV_VAR_PATTERN.sub(replace, value)


def _deep_interpolate(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: _deep_interpolate(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_deep_interpolate(i) for i in obj]
    return _interpolate_env(obj)


class ConfigLoader:
    """
    Load de_utils configuration from YAML files or environment variables.
    """

    # ------------------------------------------------------------------
    # YAML loaders
    # ------------------------------------------------------------------

    @staticmethod
    def from_yaml(path: str) -> SparkConfig:
        """
        Load a full SparkConfig (with embedded ADLS + Hive) from a YAML file.
        Supports ${ENV_VAR} interpolation throughout.
        """
        raw = ConfigLoader._load_yaml(path)
        raw = _deep_interpolate(raw)

        adls_cfg  = ConfigLoader._parse_adls(raw.get("adls", {}))
        hive_cfg  = ConfigLoader._parse_hive(raw.get("hive", {}))
        spark_raw = raw.get("spark", {})

        return SparkConfig(
            app_name=spark_raw.get("app_name", "DataEngineeringJob"),
            adls=adls_cfg,
            hive=hive_cfg,
            master=spark_raw.get("master", "local[*]"),
            extra_conf=spark_raw.get("extra_conf", {}),
            enable_hive_support=spark_raw.get("enable_hive_support", True),
        )

    @staticmethod
    def adls_from_yaml(path: str) -> ADLSConfig:
        raw = _deep_interpolate(ConfigLoader._load_yaml(path))
        return ConfigLoader._parse_adls(raw.get("adls", raw))

    @staticmethod
    def hive_from_yaml(path: str) -> HiveConfig:
        raw = _deep_interpolate(ConfigLoader._load_yaml(path))
        return ConfigLoader._parse_hive(raw.get("hive", raw))

    # ------------------------------------------------------------------
    # Environment variable loaders
    # ------------------------------------------------------------------

    @staticmethod
    def adls_from_env(prefix: str = "DE_ADLS_") -> ADLSConfig:
        """
        Build ADLSConfig from environment variables.

        Variables (prefix=DE_ADLS_):
            DE_ADLS_ACCOUNT_NAME, DE_ADLS_CONTAINER,
            DE_ADLS_CLIENT_ID, DE_ADLS_CLIENT_SECRET, DE_ADLS_TENANT_ID,
            DE_ADLS_ACCOUNT_KEY, DE_ADLS_ROOT_PATH
        """
        def g(key, default=None):
            return os.environ.get(f"{prefix}{key}", default)

        account_name = g("ACCOUNT_NAME")
        container    = g("CONTAINER")
        if not account_name or not container:
            raise DataEngineeringError(
                f"Missing required env vars: {prefix}ACCOUNT_NAME and {prefix}CONTAINER"
            )

        return ADLSConfig(
            account_name=account_name,
            container=container,
            client_id=g("CLIENT_ID"),
            client_secret=g("CLIENT_SECRET"),
            tenant_id=g("TENANT_ID"),
            account_key=g("ACCOUNT_KEY"),
            root_path=g("ROOT_PATH", "/"),
        )

    @staticmethod
    def hive_from_env(prefix: str = "DE_HIVE_") -> HiveConfig:
        """
        Build HiveConfig from environment variables.

        Variables (prefix=DE_HIVE_):
            DE_HIVE_METASTORE_URI, DE_HIVE_DATABASE,
            DE_HIVE_WAREHOUSE_PATH,
            DE_HIVE_JDBC_URL, DE_HIVE_JDBC_USER, DE_HIVE_JDBC_PASSWORD
        """
        def g(key, default=None):
            return os.environ.get(f"{prefix}{key}", default)

        return HiveConfig(
            metastore_uri=g("METASTORE_URI", "thrift://localhost:9083"),
            database=g("DATABASE", "default"),
            warehouse_path=g("WAREHOUSE_PATH"),
            jdbc_url=g("JDBC_URL"),
            jdbc_user=g("JDBC_USER"),
            jdbc_password=g("JDBC_PASSWORD"),
        )

    @staticmethod
    def spark_from_env(prefix: str = "DE_SPARK_") -> SparkConfig:
        """Build a full SparkConfig by combining adls_from_env + hive_from_env."""
        def g(key, default=None):
            return os.environ.get(f"{prefix}{key}", default)

        return SparkConfig(
            app_name=g("APP_NAME", "DataEngineeringJob"),
            adls=ConfigLoader.adls_from_env(),
            hive=ConfigLoader.hive_from_env(),
            master=g("MASTER", "local[*]"),
            enable_hive_support=g("ENABLE_HIVE_SUPPORT", "true").lower() == "true",
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _load_yaml(path: str) -> Dict:
        try:
            import yaml
        except ImportError as exc:
            raise ImportError(
                "PyYAML is required for YAML config loading. "
                "Install with: pip install pyyaml"
            ) from exc
        with open(path) as f:
            return yaml.safe_load(f) or {}

    @staticmethod
    def _parse_adls(raw: Dict) -> Optional[ADLSConfig]:
        if not raw:
            return None
        return ADLSConfig(
            account_name=raw.get("account_name", ""),
            container=raw.get("container", ""),
            client_id=raw.get("client_id"),
            client_secret=raw.get("client_secret"),
            tenant_id=raw.get("tenant_id"),
            account_key=raw.get("account_key"),
            root_path=raw.get("root_path", "/"),
            extra_hadoop_conf=raw.get("extra_hadoop_conf", {}),
        )

    @staticmethod
    def _parse_hive(raw: Dict) -> Optional[HiveConfig]:
        if not raw:
            return None
        return HiveConfig(
            metastore_uri=raw.get("metastore_uri", "thrift://localhost:9083"),
            database=raw.get("database", "default"),
            warehouse_path=raw.get("warehouse_path"),
            jdbc_url=raw.get("jdbc_url"),
            jdbc_user=raw.get("jdbc_user"),
            jdbc_password=raw.get("jdbc_password"),
            extra_properties=raw.get("extra_properties", {}),
        )

    # ------------------------------------------------------------------
    # Dump sample YAML
    # ------------------------------------------------------------------

    @staticmethod
    def dump_sample_yaml(path: str = "pipeline.yml") -> None:
        """Write a sample config YAML to *path* as a starting template."""
        sample = """\
# de_utils pipeline configuration
# Use ${ENV_VAR} syntax for secrets

adls:
  account_name: myadlsaccount
  container: raw
  client_id: ${AZURE_CLIENT_ID}
  client_secret: ${AZURE_CLIENT_SECRET}
  tenant_id: ${AZURE_TENANT_ID}
  root_path: /data

hive:
  metastore_uri: thrift://hive-metastore.example.com:9083
  database: silver
  warehouse_path: abfss://warehouse@myadlsaccount.dfs.core.windows.net/

spark:
  app_name: MyETLJob
  master: local[*]
  enable_hive_support: true
  extra_conf:
    spark.sql.shuffle.partitions: "200"
    spark.sql.adaptive.enabled: "true"
"""
        with open(path, "w") as f:
            f.write(sample)
        log.info("Sample config written to %s", path)
