"""
de_utils.storage
----------------
Azure Data Lake Storage Gen2 connector built on top of the
azure-storage-file-datalake SDK and abfss:// Spark paths.
"""

from __future__ import annotations

import os
from pathlib import PurePosixPath
from typing import List, Optional, TYPE_CHECKING

from .config import ADLSConfig
from .utils import get_logger, DataEngineeringError

if TYPE_CHECKING:
    from pyspark.sql import SparkSession, DataFrame

log = get_logger(__name__)


class ADLSConnector:
    """
    High-level helper for common ADLS Gen2 operations and Spark reads/writes.

    Example
    -------
    >>> from de_utils import ADLSConfig, ADLSConnector
    >>> cfg = ADLSConfig(
    ...     account_name="myadls",
    ...     container="raw",
    ...     client_id="...", client_secret="...", tenant_id="...",
    ... )
    >>> conn = ADLSConnector(cfg, spark)
    >>> df = conn.read_parquet("ingest/orders/2024/01/")
    >>> conn.write_parquet(df, "staging/orders/", partition_by=["year", "month"])
    """

    def __init__(self, config: ADLSConfig, spark: "SparkSession"):
        self.config = config
        self.spark = spark
        self._configure_spark()

    # ------------------------------------------------------------------
    # Internal setup
    # ------------------------------------------------------------------

    def _configure_spark(self) -> None:
        """Push ADLS auth configuration into the active SparkSession."""
        for k, v in self.config.to_spark_conf().items():
            self.spark.conf.set(k, v)
        log.info(
            "Configured ADLS account '%s' on container '%s'",
            self.config.account_name,
            self.config.container,
        )

    # ------------------------------------------------------------------
    # Path helpers
    # ------------------------------------------------------------------

    def abfss(self, *parts: str) -> str:
        """Return a fully qualified abfss:// path."""
        return self.config.path(*parts)

    # ------------------------------------------------------------------
    # Read helpers
    # ------------------------------------------------------------------

    def read_parquet(self, path: str, **options) -> "DataFrame":
        """Read Parquet files from ADLS."""
        full = self.abfss(path)
        log.info("Reading parquet from %s", full)
        return self.spark.read.options(**options).parquet(full)

    def read_delta(self, path: str, version: Optional[int] = None, **options) -> "DataFrame":
        """Read a Delta table from ADLS, optionally at a specific version."""
        full = self.abfss(path)
        reader = self.spark.read.format("delta").options(**options)
        if version is not None:
            reader = reader.option("versionAsOf", version)
        log.info("Reading delta from %s (version=%s)", full, version)
        return reader.load(full)

    def read_csv(self, path: str, header: bool = True, infer_schema: bool = True, **options) -> "DataFrame":
        """Read CSV files from ADLS."""
        full = self.abfss(path)
        log.info("Reading CSV from %s", full)
        return (
            self.spark.read.options(**options)
            .option("header", header)
            .option("inferSchema", infer_schema)
            .csv(full)
        )

    def read_json(self, path: str, multiline: bool = False, **options) -> "DataFrame":
        """Read JSON files from ADLS."""
        full = self.abfss(path)
        log.info("Reading JSON from %s", full)
        return (
            self.spark.read.options(**options)
            .option("multiline", multiline)
            .json(full)
        )

    # ------------------------------------------------------------------
    # Write helpers
    # ------------------------------------------------------------------

    def write_parquet(
        self,
        df: "DataFrame",
        path: str,
        mode: str = "overwrite",
        partition_by: Optional[List[str]] = None,
        **options,
    ) -> None:
        """Write a DataFrame as Parquet to ADLS."""
        full = self.abfss(path)
        log.info("Writing parquet to %s (mode=%s, partitions=%s)", full, mode, partition_by)
        writer = df.write.mode(mode).options(**options)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.parquet(full)

    def write_delta(
        self,
        df: "DataFrame",
        path: str,
        mode: str = "overwrite",
        partition_by: Optional[List[str]] = None,
        **options,
    ) -> None:
        """Write a DataFrame as Delta format to ADLS."""
        full = self.abfss(path)
        log.info("Writing delta to %s (mode=%s, partitions=%s)", full, mode, partition_by)
        writer = df.write.format("delta").mode(mode).options(**options)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(full)

    def write_csv(
        self,
        df: "DataFrame",
        path: str,
        mode: str = "overwrite",
        header: bool = True,
        **options,
    ) -> None:
        """Write a DataFrame as CSV to ADLS."""
        full = self.abfss(path)
        log.info("Writing CSV to %s", full)
        df.write.mode(mode).option("header", header).options(**options).csv(full)

    # ------------------------------------------------------------------
    # File-system helpers (requires azure-storage-file-datalake)
    # ------------------------------------------------------------------

    def _get_fs_client(self):
        """Return an azure-storage-file-datalake FileSystemClient."""
        try:
            from azure.storage.filedatalake import DataLakeServiceClient
            from azure.identity import ClientSecretCredential
        except ImportError as exc:
            raise ImportError(
                "Install azure-storage-file-datalake and azure-identity: "
                "pip install azure-storage-file-datalake azure-identity"
            ) from exc

        cfg = self.config
        if cfg.client_id and cfg.client_secret and cfg.tenant_id:
            credential = ClientSecretCredential(cfg.tenant_id, cfg.client_id, cfg.client_secret)
        elif cfg.account_key:
            credential = cfg.account_key  # type: ignore[assignment]
        else:
            raise DataEngineeringError("No valid credential found in ADLSConfig.")

        service = DataLakeServiceClient(
            account_url=f"https://{cfg.account_name}.dfs.core.windows.net",
            credential=credential,
        )
        return service.get_file_system_client(cfg.container)

    def list_paths(self, path: str = "/", recursive: bool = False) -> List[str]:
        """Return a list of paths under the given directory."""
        fs = self._get_fs_client()
        paths = fs.get_paths(path=path.lstrip("/"), recursive=recursive)
        return [p.name for p in paths]

    def path_exists(self, path: str) -> bool:
        """Return True if the given path exists in ADLS."""
        try:
            fs = self._get_fs_client()
            fc = fs.get_file_client(path.lstrip("/"))
            fc.get_file_properties()
            return True
        except Exception:
            return False

    def delete_path(self, path: str, recursive: bool = True) -> None:
        """Delete a file or directory from ADLS."""
        fs = self._get_fs_client()
        dc = fs.get_directory_client(path.lstrip("/"))
        dc.delete_directory()
        log.info("Deleted ADLS path: %s", path)

    def create_directory(self, path: str) -> None:
        """Create a directory in ADLS."""
        fs = self._get_fs_client()
        fs.create_directory(path.lstrip("/"))
        log.info("Created ADLS directory: %s", path)
