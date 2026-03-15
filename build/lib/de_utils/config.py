"""
de_utils.config
---------------
Configuration dataclasses for Azure ADLS, Hive Metastore, and Spark.
"""

from dataclasses import dataclass, field
from typing import Optional, Dict, Any


@dataclass
class ADLSConfig:
    """
    Azure Data Lake Storage Gen2 configuration.

    Example
    -------
    >>> cfg = ADLSConfig(
    ...     account_name="myadlsaccount",
    ...     container="raw",
    ...     client_id="<app-id>",
    ...     client_secret="<secret>",
    ...     tenant_id="<tenant>",
    ... )
    """

    account_name: str
    container: str
    # Auth: service principal
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    tenant_id: Optional[str] = None
    # Auth: storage account key (fallback)
    account_key: Optional[str] = None
    # Optional root path prefix inside the container
    root_path: str = "/"
    # Connection / request timeouts
    connection_timeout: int = 30
    read_timeout: int = 30
    # Extra Spark / Hadoop config that should be merged into SparkConf
    extra_hadoop_conf: Dict[str, str] = field(default_factory=dict)

    @property
    def abfss_base(self) -> str:
        """Return the abfss:// base URI for this container."""
        return f"abfss://{self.container}@{self.account_name}.dfs.core.windows.net"

    def path(self, *parts: str) -> str:
        """Build an absolute abfss:// path inside the container."""
        joined = "/".join(p.strip("/") for p in (self.root_path, *parts) if p.strip("/"))
        return f"{self.abfss_base}/{joined}"

    def to_spark_conf(self) -> Dict[str, str]:
        """
        Return a dict of Spark / Hadoop config entries for this ADLS account.
        Merge the result into your SparkConf or SparkSession builder.
        """
        prefix = f"fs.azure.account"
        conf: Dict[str, str] = {}

        if self.client_id and self.client_secret and self.tenant_id:
            conf[f"{prefix}.auth.type.{self.account_name}.dfs.core.windows.net"] = (
                "OAuth"
            )
            conf[
                f"{prefix}.oauth.provider.type.{self.account_name}.dfs.core.windows.net"
            ] = "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
            conf[
                f"{prefix}.oauth2.client.id.{self.account_name}.dfs.core.windows.net"
            ] = self.client_id
            conf[
                f"{prefix}.oauth2.client.secret.{self.account_name}.dfs.core.windows.net"
            ] = self.client_secret
            conf[
                f"{prefix}.oauth2.client.endpoint.{self.account_name}.dfs.core.windows.net"
            ] = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/token"

        elif self.account_key:
            conf[f"{prefix}.key.{self.account_name}.dfs.core.windows.net"] = (
                self.account_key
            )

        conf.update(self.extra_hadoop_conf)
        return conf


@dataclass
class HiveConfig:
    """
    Hive Metastore configuration for Azure HDInsight / Databricks external
    metastore or any Thrift-compatible metastore.

    Example
    -------
    >>> hive_cfg = HiveConfig(
    ...     metastore_uri="thrift://hive-metastore.example.com:9083",
    ...     database="gold",
    ...     warehouse_path="abfss://warehouse@account.dfs.core.windows.net/",
    ... )
    """

    metastore_uri: str = "thrift://localhost:9083"
    database: str = "default"
    warehouse_path: Optional[str] = None
    # Optional JDBC metastore (for Azure SQL / MySQL backed metastores)
    jdbc_url: Optional[str] = None
    jdbc_user: Optional[str] = None
    jdbc_password: Optional[str] = None
    jdbc_driver: str = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    # Extra Hive properties
    extra_properties: Dict[str, str] = field(default_factory=dict)

    def to_spark_conf(self) -> Dict[str, str]:
        """Return Spark conf entries to point at this Hive metastore."""
        conf: Dict[str, str] = {
            "hive.metastore.uris": self.metastore_uri,
        }
        if self.warehouse_path:
            conf["spark.sql.warehouse.dir"] = self.warehouse_path
        if self.jdbc_url:
            conf["javax.jdo.option.ConnectionURL"] = self.jdbc_url
            conf["javax.jdo.option.ConnectionUserName"] = self.jdbc_user or ""
            conf["javax.jdo.option.ConnectionPassword"] = self.jdbc_password or ""
            conf["javax.jdo.option.ConnectionDriverName"] = self.jdbc_driver
        conf.update(self.extra_properties)
        return conf


@dataclass
class SparkConfig:
    """
    Convenience wrapper that combines ADLS + Hive configs and exposes a
    configured SparkSession.

    Example
    -------
    >>> from de_utils import SparkConfig, ADLSConfig, HiveConfig
    >>> spark_cfg = SparkConfig(
    ...     app_name="MyETLJob",
    ...     adls=ADLSConfig(account_name="acct", container="raw", account_key="key"),
    ...     hive=HiveConfig(metastore_uri="thrift://meta:9083", database="silver"),
    ... )
    >>> spark = spark_cfg.build_session()
    """

    app_name: str = "DataEngineeringJob"
    adls: Optional[ADLSConfig] = None
    hive: Optional[HiveConfig] = None
    master: str = "local[*]"
    extra_conf: Dict[str, str] = field(default_factory=dict)
    # Enable Hive support (required for metastore operations)
    enable_hive_support: bool = True

    def build_session(self) -> Any:
        """
        Build and return a configured SparkSession.
        Requires pyspark to be installed.
        """
        try:
            from pyspark.sql import SparkSession
        except ImportError as exc:
            raise ImportError(
                "pyspark is required. Install with: pip install pyspark"
            ) from exc

        builder = SparkSession.builder.appName(self.app_name).master(self.master)

        if self.enable_hive_support:
            builder = builder.enableHiveSupport()

        all_conf: Dict[str, str] = {}
        if self.adls:
            all_conf.update(self.adls.to_spark_conf())
        if self.hive:
            all_conf.update(self.hive.to_spark_conf())
        all_conf.update(self.extra_conf)

        for k, v in all_conf.items():
            builder = builder.config(k, v)

        return builder.getOrCreate()
