"""
de_utils.databricks
-------------------
Databricks-compatible helpers for the free Community Edition and
standard Databricks workspaces.

Databricks Community Edition constraints
-----------------------------------------
  - No ADLS / external storage auth via service principal
    (use DBFS or Mount points instead)
  - No external Hive metastore — uses the built-in workspace metastore
  - Single-node cluster (no multi-node parallelism)
  - Secrets via Databricks Secret Scope (not Azure Key Vault)
  - No direct pip install of private packages — use %pip or DBFS wheels
  - SparkSession already exists as `spark` — never call SparkSession.builder

What this module provides
--------------------------
  DatabricksConfig      — Auto-detect environment, expose DBFS + mount paths
  DatabricksSession     — Get or validate the active SparkSession safely
  DatabricksSecrets     — Read secrets from Databricks Secret Scopes via dbutils
  DBFSConnector         — Read/write Parquet, Delta, CSV using DBFS paths
  MountConnector        — Read/write via mounted external storage (ADLS, S3, GCS)
  DatabricksTableOps    — CREATE, MERGE, INSERT using Unity Catalog or workspace metastore
  DatabricksLoader      — FULL, INCREMENTAL, PARTITION, MERGE loads — CE-compatible
  DatabricksAuditLog    — Persist audit entries to a Delta table in DBFS
  DatabricksCheckpoint  — Watermarks stored in a Delta table on DBFS
  NotebookUtils         — Thin wrapper around dbutils for widgets, jobs, file ops
  is_databricks()       — Detect if running inside a Databricks environment

Usage — Databricks Notebook
----------------------------
# Cell 1 — install
%pip install de_utils

# Cell 2 — import and use
from de_utils.databricks import (
    DatabricksSession, DatabricksConfig, DatabricksSecrets,
    DBFSConnector, DatabricksLoader, DatabricksAuditLog,
)

cfg     = DatabricksConfig()                         # auto-detects environment
session = DatabricksSession.get()                    # uses existing `spark`
secrets = DatabricksSecrets(scope="my-scope")        # reads from secret scope
conn    = DBFSConnector(session)                     # DBFS read/write
loader  = DatabricksLoader(session, database="silver")

df = conn.read_parquet("/mnt/raw/orders/2024/")
loader.full_load(df, "orders")
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from .utils import get_logger, DataEngineeringError

if TYPE_CHECKING:
    from pyspark.sql import SparkSession, DataFrame

log = get_logger(__name__)


# ── Environment detection ─────────────────────────────────────────────────────

def is_databricks() -> bool:
    """Return True when running inside any Databricks environment."""
    indicators = [
        os.environ.get("DATABRICKS_RUNTIME_VERSION"),
        os.environ.get("DB_HOME"),
        os.environ.get("SPARK_LOCAL_DIRS", "").startswith("/local_disk"),
    ]
    return any(bool(i) for i in indicators)


def is_community_edition() -> bool:
    """
    Heuristic: Community Edition clusters have a single executor
    and the runtime version string contains 'community'.
    """
    if not is_databricks():
        return False
    rt = os.environ.get("DATABRICKS_RUNTIME_VERSION", "").lower()
    # CE clusters often expose this env var as empty or missing cluster tags
    cluster_name = os.environ.get("CLUSTER_NAME", "").lower()
    return "community" in rt or "community" in cluster_name or not os.environ.get("DATABRICKS_HOST")


def get_runtime_version() -> str:
    return os.environ.get("DATABRICKS_RUNTIME_VERSION", "unknown")


# ── DatabricksConfig ──────────────────────────────────────────────────────────

class DatabricksConfig:
    """
    Auto-detect Databricks environment and expose common paths and settings.

    Works on Community Edition, standard workspaces, and Unity Catalog.

    Example
    -------
    >>> cfg = DatabricksConfig()
    >>> cfg.dbfs_root          # "dbfs:/user/hive/warehouse"
    >>> cfg.is_community       # True / False
    >>> cfg.default_database   # "default"
    >>> cfg.path("silver", "orders")  # "dbfs:/de_utils/silver/orders"
    """

    def __init__(
        self,
        dbfs_root: str = "dbfs:/de_utils",
        default_database: str = "default",
        catalog: Optional[str] = None,       # Unity Catalog name (None = legacy metastore)
        mount_point: Optional[str] = None,   # e.g. "/mnt/datalake"
    ):
        self.dbfs_root        = dbfs_root.rstrip("/")
        self.default_database = default_database
        self.catalog          = catalog
        self.mount_point      = mount_point
        self.is_databricks    = is_databricks()
        self.is_community     = is_community_edition()
        self.runtime_version  = get_runtime_version()

        if not self.is_databricks:
            log.warning(
                "DatabricksConfig: not running inside Databricks. "
                "Methods will work but DBFS paths won't resolve locally."
            )

    def path(self, *parts: str) -> str:
        """Build a DBFS path under the configured root."""
        joined = "/".join(p.strip("/") for p in parts if p.strip("/"))
        return f"{self.dbfs_root}/{joined}"

    def mount_path(self, *parts: str) -> str:
        """Build a path under the configured mount point."""
        if not self.mount_point:
            raise DataEngineeringError(
                "mount_point not configured. Pass mount_point='/mnt/...' to DatabricksConfig()."
            )
        joined = "/".join(p.strip("/") for p in parts if p.strip("/"))
        return f"{self.mount_point}/{joined}"

    def table_name(self, table: str, database: Optional[str] = None) -> str:
        """Return a fully qualified table name respecting catalog if set."""
        db = database or self.default_database
        if self.catalog:
            return f"`{self.catalog}`.`{db}`.`{table}`"
        return f"`{db}`.`{table}`"

    def __repr__(self) -> str:
        return (
            f"DatabricksConfig(dbfs_root={self.dbfs_root!r}, "
            f"database={self.default_database!r}, "
            f"community={self.is_community}, "
            f"runtime={self.runtime_version!r})"
        )


# ── DatabricksSession ─────────────────────────────────────────────────────────

class DatabricksSession:
    """
    Safe SparkSession accessor for Databricks notebooks.

    In Databricks, `spark` is already provided by the runtime.
    Never call SparkSession.builder yourself — use this class instead.

    Example
    -------
    >>> session = DatabricksSession.get()
    >>> session.version   # Spark version string
    """

    @staticmethod
    def get(require_databricks: bool = False) -> "SparkSession":
        """
        Return the active SparkSession.

        If inside Databricks, returns the pre-existing session.
        If outside Databricks (e.g. local dev), creates a local session.

        Parameters
        ----------
        require_databricks : If True, raises if not running inside Databricks.
        """
        from pyspark.sql import SparkSession

        if require_databricks and not is_databricks():
            raise DataEngineeringError(
                "This operation requires a Databricks environment. "
                "Not running inside Databricks."
            )

        spark = SparkSession.getActiveSession()
        if spark is None:
            if is_databricks():
                raise DataEngineeringError(
                    "No active SparkSession found inside Databricks — "
                    "this should not happen. Check cluster health."
                )
            log.info("No active Spark session — creating local session for dev/test.")
            spark = (
                SparkSession.builder
                .appName("de_utils_local")
                .master("local[*]")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "4")
                .getOrCreate()
            )
        return spark

    @staticmethod
    def configure_for_delta(spark: "SparkSession") -> None:
        """Apply Delta Lake session config (safe to call on CE)."""
        confs = {
            "spark.sql.extensions":
                "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog":
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.databricks.delta.optimizeWrite.enabled": "true",
            "spark.databricks.delta.autoCompact.enabled":   "true",
        }
        for k, v in confs.items():
            try:
                spark.conf.set(k, v)
            except Exception:
                pass   # CE may block some confs — skip silently

    @staticmethod
    def set_shuffle_partitions(spark: "SparkSession", n: int = 8) -> None:
        """
        Set shuffle partitions — use a low number (4-8) on CE single-node clusters.
        Default Spark value of 200 causes severe overhead on small clusters.
        """
        spark.conf.set("spark.sql.shuffle.partitions", str(n))
        log.info("Shuffle partitions set to %d", n)


# ── DatabricksSecrets ─────────────────────────────────────────────────────────

class DatabricksSecrets:
    """
    Read secrets from Databricks Secret Scopes via dbutils.

    Compatible with Community Edition (manual secret scopes),
    standard workspaces (Databricks-backed scopes), and
    Azure Key Vault-backed scopes.

    Example
    -------
    >>> secrets = DatabricksSecrets(scope="my-scope")
    >>> password = secrets.get("db-password")
    >>> adls_key = secrets.get("adls-account-key")

    Creating a secret (Databricks CLI):
        databricks secrets create-scope my-scope
        databricks secrets put --scope my-scope --key db-password
    """

    def __init__(self, scope: str, dbutils: Any = None):
        self.scope   = scope
        self._dbutils = dbutils   # injected or auto-detected

    def _get_dbutils(self) -> Any:
        if self._dbutils:
            return self._dbutils
        # Try to get dbutils from IPython globals (works in Databricks notebooks)
        try:
            import IPython
            shell = IPython.get_ipython()
            if shell and "dbutils" in shell.user_ns:
                return shell.user_ns["dbutils"]
        except Exception:
            pass
        raise DataEngineeringError(
            "dbutils not available. Pass dbutils explicitly: "
            "DatabricksSecrets(scope='...', dbutils=dbutils)"
        )

    def get(self, key: str) -> Optional[str]:
        """Fetch a secret value. Returns None if not found."""
        try:
            return self._get_dbutils().secrets.get(scope=self.scope, key=key)
        except Exception as exc:
            if "SecretDoesNotExist" in str(exc) or "does not exist" in str(exc).lower():
                return None
            log.warning("Secret '%s' in scope '%s' could not be fetched: %s", key, self.scope, exc)
            return None

    def get_required(self, key: str) -> str:
        """Fetch a secret, raising DataEngineeringError if missing."""
        value = self.get(key)
        if value is None:
            raise DataEngineeringError(
                f"Required secret '{key}' not found in scope '{self.scope}'."
            )
        return value

    def list_keys(self) -> List[str]:
        """List all secret keys in this scope (values are never returned)."""
        try:
            return [m.key for m in self._get_dbutils().secrets.list(self.scope)]
        except Exception as exc:
            log.warning("Could not list secrets in scope '%s': %s", self.scope, exc)
            return []

    def to_adls_config(
        self,
        account_name_key: str = "adls-account-name",
        container_key:    str = "adls-container",
        client_id_key:    str = "adls-client-id",
        client_secret_key:str = "adls-client-secret",
        tenant_id_key:    str = "adls-tenant-id",
        root_path:        str = "/",
    ):
        """
        Build an ADLSConfig from secrets in this scope.
        Use this on standard workspaces; on CE use DBFSConnector instead.
        """
        from .config import ADLSConfig
        return ADLSConfig(
            account_name  = self.get_required(account_name_key),
            container     = self.get_required(container_key),
            client_id     = self.get(client_id_key),
            client_secret = self.get(client_secret_key),
            tenant_id     = self.get(tenant_id_key),
            root_path     = root_path,
        )


# ── DBFSConnector ─────────────────────────────────────────────────────────────

class DBFSConnector:
    """
    Read and write DataFrames using DBFS paths.
    Works on Databricks Community Edition with no credentials required.

    DBFS paths look like:  dbfs:/de_utils/silver/orders/
    Mount paths look like: /mnt/datalake/silver/orders/

    Example
    -------
    >>> conn = DBFSConnector(spark)
    >>> df   = conn.read_parquet("dbfs:/de_utils/raw/orders/")
    >>> conn.write_delta(df, "dbfs:/de_utils/silver/orders/")
    >>> conn.write_delta(df, "/mnt/datalake/silver/orders/")  # mount
    """

    def __init__(self, spark: "SparkSession"):
        self.spark = spark

    # ── Read ──────────────────────────────────────────────────────────────────

    def read_parquet(self, path: str, **options) -> "DataFrame":
        log.info("DBFSConnector: reading parquet from %s", path)
        return self.spark.read.options(**options).parquet(path)

    def read_delta(self, path: str, version: Optional[int] = None,
                   timestamp: Optional[str] = None, **options) -> "DataFrame":
        log.info("DBFSConnector: reading delta from %s (version=%s)", path, version)
        reader = self.spark.read.format("delta").options(**options)
        if version is not None:
            reader = reader.option("versionAsOf", version)
        if timestamp is not None:
            reader = reader.option("timestampAsOf", timestamp)
        return reader.load(path)

    def read_csv(self, path: str, header: bool = True,
                 infer_schema: bool = True, **options) -> "DataFrame":
        log.info("DBFSConnector: reading CSV from %s", path)
        return (
            self.spark.read
            .option("header", header)
            .option("inferSchema", infer_schema)
            .options(**options)
            .csv(path)
        )

    def read_json(self, path: str, multiline: bool = False, **options) -> "DataFrame":
        log.info("DBFSConnector: reading JSON from %s", path)
        return self.spark.read.option("multiline", multiline).options(**options).json(path)

    def read_table(self, table_name: str) -> "DataFrame":
        """Read a table registered in the workspace metastore."""
        return self.spark.table(table_name)

    # ── Write ─────────────────────────────────────────────────────────────────

    def write_delta(self, df: "DataFrame", path: str, mode: str = "overwrite",
                    partition_by: Optional[List[str]] = None, **options) -> None:
        log.info("DBFSConnector: writing delta to %s (mode=%s)", path, mode)
        writer = df.write.format("delta").mode(mode).options(**options)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(path)

    def write_parquet(self, df: "DataFrame", path: str, mode: str = "overwrite",
                      partition_by: Optional[List[str]] = None, **options) -> None:
        log.info("DBFSConnector: writing parquet to %s (mode=%s)", path, mode)
        writer = df.write.mode(mode).options(**options)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.parquet(path)

    def write_csv(self, df: "DataFrame", path: str, mode: str = "overwrite",
                  header: bool = True, **options) -> None:
        df.write.mode(mode).option("header", header).options(**options).csv(path)

    def write_table(self, df: "DataFrame", table_name: str,
                    mode: str = "overwrite", partition_by: Optional[List[str]] = None,
                    format: str = "delta") -> None:
        """Write DataFrame and register it as a table in the workspace metastore."""
        writer = df.write.format(format).mode(mode)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.saveAsTable(table_name)
        log.info("DBFSConnector: saved table '%s'", table_name)

    # ── File-system helpers via dbutils ───────────────────────────────────────

    def list_paths(self, path: str, dbutils: Any = None) -> List[str]:
        """List files/dirs at a DBFS path. Requires dbutils."""
        du = self._get_dbutils(dbutils)
        if du:
            return [f.path for f in du.fs.ls(path)]
        log.warning("dbutils not available — cannot list DBFS paths")
        return []

    def path_exists(self, path: str, dbutils: Any = None) -> bool:
        """Check if a DBFS path exists."""
        du = self._get_dbutils(dbutils)
        if du:
            try:
                du.fs.ls(path)
                return True
            except Exception:
                return False
        return False

    def delete_path(self, path: str, recurse: bool = True, dbutils: Any = None) -> None:
        """Delete a DBFS path."""
        du = self._get_dbutils(dbutils)
        if du:
            du.fs.rm(path, recurse=recurse)
            log.info("DBFSConnector: deleted %s", path)

    def copy_path(self, src: str, dst: str, recurse: bool = True, dbutils: Any = None) -> None:
        """Copy files within DBFS."""
        du = self._get_dbutils(dbutils)
        if du:
            du.fs.cp(src, dst, recurse=recurse)
            log.info("DBFSConnector: copied %s → %s", src, dst)

    def _get_dbutils(self, dbutils: Any = None) -> Optional[Any]:
        if dbutils:
            return dbutils
        try:
            import IPython
            shell = IPython.get_ipython()
            if shell and "dbutils" in shell.user_ns:
                return shell.user_ns["dbutils"]
        except Exception:
            pass
        return None


# ── MountConnector ────────────────────────────────────────────────────────────

class MountConnector:
    """
    Mount external storage (ADLS Gen2, S3, GCS) in Databricks and
    read/write through the mount point.

    On Community Edition: you can mount ADLS using a SAS token or
    account key — service principal OAuth is not supported.

    Example
    -------
    >>> mnt = MountConnector(spark, dbutils=dbutils)

    >>> # Mount ADLS Gen2 with account key (CE compatible)
    >>> mnt.mount_adls(
    ...     account_name="myadlsaccount",
    ...     container="raw",
    ...     mount_name="raw",
    ...     account_key="YOUR_ACCOUNT_KEY",   # or use DatabricksSecrets.get()
    ... )

    >>> # Mount ADLS Gen2 with service principal (standard workspace)
    >>> mnt.mount_adls_sp(
    ...     account_name="myadlsaccount",
    ...     container="silver",
    ...     mount_name="silver",
    ...     client_id="...", client_secret="...", tenant_id="...",
    ... )

    >>> # Read from mount
    >>> df = mnt.read_parquet("orders/2024/")   # → /mnt/raw/orders/2024/
    """

    def __init__(
        self,
        spark: "SparkSession",
        dbutils: Any = None,
        default_mount: str = "raw",
    ):
        self.spark         = spark
        self._dbutils      = dbutils
        self.default_mount = default_mount

    def _du(self) -> Any:
        if self._dbutils:
            return self._dbutils
        try:
            import IPython
            shell = IPython.get_ipython()
            if shell and "dbutils" in shell.user_ns:
                return shell.user_ns["dbutils"]
        except Exception:
            pass
        raise DataEngineeringError(
            "dbutils required for mount operations. "
            "Pass dbutils=dbutils to MountConnector()."
        )

    # ── Mount management ──────────────────────────────────────────────────────

    def mount_adls(
        self,
        account_name: str,
        container:    str,
        mount_name:   str,
        account_key:  str,
    ) -> str:
        """
        Mount ADLS Gen2 using a storage account key.
        This works on Community Edition.
        Returns the mount point path.
        """
        mount_point = f"/mnt/{mount_name}"
        source      = f"abfss://{container}@{account_name}.dfs.core.windows.net/"
        conf_key    = f"fs.azure.account.key.{account_name}.dfs.core.windows.net"

        if self._is_mounted(mount_point):
            log.info("Already mounted at %s — skipping", mount_point)
            return mount_point

        self._du().fs.mount(
            source=source,
            mount_point=mount_point,
            extra_configs={conf_key: account_key},
        )
        log.info("Mounted %s at %s", source, mount_point)
        return mount_point

    def mount_adls_sp(
        self,
        account_name:  str,
        container:     str,
        mount_name:    str,
        client_id:     str,
        client_secret: str,
        tenant_id:     str,
    ) -> str:
        """
        Mount ADLS Gen2 using a service principal (standard workspace only).
        Returns the mount point path.
        """
        mount_point = f"/mnt/{mount_name}"
        source      = f"abfss://{container}@{account_name}.dfs.core.windows.net/"
        configs = {
            f"fs.azure.account.auth.type.{account_name}.dfs.core.windows.net": "OAuth",
            f"fs.azure.account.oauth.provider.type.{account_name}.dfs.core.windows.net":
                "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            f"fs.azure.account.oauth2.client.id.{account_name}.dfs.core.windows.net": client_id,
            f"fs.azure.account.oauth2.client.secret.{account_name}.dfs.core.windows.net": client_secret,
            f"fs.azure.account.oauth2.client.endpoint.{account_name}.dfs.core.windows.net":
                f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
        }
        if self._is_mounted(mount_point):
            log.info("Already mounted at %s — skipping", mount_point)
            return mount_point
        self._du().fs.mount(source=source, mount_point=mount_point, extra_configs=configs)
        log.info("Mounted %s (SP auth) at %s", source, mount_point)
        return mount_point

    def unmount(self, mount_name: str) -> None:
        mount_point = f"/mnt/{mount_name}"
        self._du().fs.unmount(mount_point)
        log.info("Unmounted %s", mount_point)

    def list_mounts(self) -> List[str]:
        return [m.mountPoint for m in self._du().fs.mounts()]

    def _is_mounted(self, mount_point: str) -> bool:
        try:
            return any(m.mountPoint == mount_point for m in self._du().fs.mounts())
        except Exception:
            return False

    # ── Read / Write via mount ────────────────────────────────────────────────

    def _path(self, relative: str, mount_name: Optional[str] = None) -> str:
        mn = mount_name or self.default_mount
        return f"/mnt/{mn}/{relative.lstrip('/')}"

    def read_parquet(self, path: str, mount_name: Optional[str] = None, **options) -> "DataFrame":
        full = self._path(path, mount_name)
        log.info("MountConnector: reading parquet from %s", full)
        return self.spark.read.options(**options).parquet(full)

    def read_delta(self, path: str, mount_name: Optional[str] = None,
                   version: Optional[int] = None, **options) -> "DataFrame":
        full   = self._path(path, mount_name)
        reader = self.spark.read.format("delta").options(**options)
        if version is not None:
            reader = reader.option("versionAsOf", version)
        return reader.load(full)

    def write_delta(self, df: "DataFrame", path: str, mount_name: Optional[str] = None,
                    mode: str = "overwrite", partition_by: Optional[List[str]] = None) -> None:
        full   = self._path(path, mount_name)
        writer = df.write.format("delta").mode(mode)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(full)
        log.info("MountConnector: wrote delta to %s", full)


# ── DatabricksTableOps ────────────────────────────────────────────────────────

class DatabricksTableOps:
    """
    CREATE / MERGE / INSERT / DROP for Databricks — works with both
    workspace metastore and Unity Catalog.

    Example
    -------
    >>> ops = DatabricksTableOps(spark, database="silver")
    >>> ops.create_or_replace("orders", df)
    >>> ops.merge("orders", updates_df, match_keys=["order_id"])
    """

    def __init__(
        self,
        spark:    "SparkSession",
        database: str = "default",
        catalog:  Optional[str] = None,
    ):
        self.spark    = spark
        self.database = database
        self.catalog  = catalog

    def _fqn(self, table: str) -> str:
        if self.catalog:
            return f"`{self.catalog}`.`{self.database}`.`{table}`"
        return f"`{self.database}`.`{table}`"

    def create_database(self, database: Optional[str] = None,
                        location: Optional[str] = None) -> None:
        db  = database or self.database
        sql = f"CREATE DATABASE IF NOT EXISTS `{db}`"
        if location:
            sql += f" LOCATION '{location}'"
        self.spark.sql(sql)
        log.info("Created database '%s'", db)

    def create_or_replace(
        self,
        table_name: str,
        df: "DataFrame",
        partition_by: Optional[List[str]] = None,
        location: Optional[str] = None,
        format: str = "delta",
    ) -> None:
        fqn    = self._fqn(table_name)
        writer = df.write.format(format).mode("overwrite")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        if location:
            writer = writer.option("path", location)
        writer.saveAsTable(fqn)
        log.info("Created/replaced table %s", fqn)

    def create_if_not_exists(self, table_name: str, df: "DataFrame",
                              **kwargs) -> bool:
        fqn = self._fqn(table_name)
        try:
            self.spark.sql(f"DESCRIBE TABLE {fqn}")
            log.info("Table %s exists — skipping", fqn)
            return False
        except Exception:
            self.create_or_replace(table_name, df, **kwargs)
            return True

    def insert_append(self, table_name: str, df: "DataFrame") -> None:
        fqn = self._fqn(table_name)
        df.write.mode("append").insertInto(fqn)

    def merge(
        self,
        target_table:  str,
        source_df:     "DataFrame",
        match_keys:    List[str],
        update_columns: Optional[List[str]] = None,
        insert_when_not_matched: bool = True,
    ) -> None:
        fqn     = self._fqn(target_table)
        view    = "__db_merge_src__"
        source_df.createOrReplaceTempView(view)

        all_cols  = source_df.columns
        upd_cols  = update_columns or [c for c in all_cols if c not in match_keys]
        on_clause = " AND ".join(f"t.`{k}` = s.`{k}`" for k in match_keys)
        set_clause= ", ".join(f"t.`{c}` = s.`{c}`" for c in upd_cols)
        ins_cols  = ", ".join(f"`{c}`" for c in all_cols)
        ins_vals  = ", ".join(f"s.`{c}`" for c in all_cols)

        sql = (
            f"MERGE INTO {fqn} AS t "
            f"USING {view} AS s ON {on_clause} "
            f"WHEN MATCHED THEN UPDATE SET {set_clause} "
        )
        if insert_when_not_matched:
            sql += f"WHEN NOT MATCHED THEN INSERT ({ins_cols}) VALUES ({ins_vals})"

        log.info("MERGE INTO %s on keys=%s", fqn, match_keys)
        self.spark.sql(sql)

    def drop_table(self, table_name: str) -> None:
        self.spark.sql(f"DROP TABLE IF EXISTS {self._fqn(table_name)}")

    def optimize(self, table_name: str, zorder_by: Optional[List[str]] = None) -> None:
        """Run OPTIMIZE on a Delta table (Databricks built-in)."""
        fqn = self._fqn(table_name)
        sql = f"OPTIMIZE {fqn}"
        if zorder_by:
            sql += f" ZORDER BY ({', '.join(zorder_by)})"
        self.spark.sql(sql)
        log.info("OPTIMIZE %s", fqn)

    def vacuum(self, table_name: str, retention_hours: int = 168) -> None:
        """Run VACUUM on a Delta table."""
        fqn = self._fqn(table_name)
        self.spark.sql(f"VACUUM {fqn} RETAIN {retention_hours} HOURS")
        log.info("VACUUM %s (retention=%dh)", fqn, retention_hours)

    def list_tables(self) -> List[str]:
        rows = self.spark.sql(f"SHOW TABLES IN `{self.database}`").collect()
        return [r.tableName for r in rows]


# ── DatabricksLoader ──────────────────────────────────────────────────────────

class DatabricksLoader:
    """
    FULL / INCREMENTAL / PARTITION / MERGE loads — CE-compatible.
    Uses the workspace metastore (no external Hive required).

    Example
    -------
    >>> loader = DatabricksLoader(spark, database="silver")
    >>> loader.full_load(raw_df, "orders", partition_by=["year","month"])
    >>> loader.merge_load(cdc_df, "orders", match_keys=["order_id"])
    >>> loader.incremental_load(df, "orders", watermark_col="updated_at",
    ...                         last_watermark="2024-01-01")
    """

    def __init__(
        self,
        spark:    "SparkSession",
        database: str = "default",
        catalog:  Optional[str] = None,
        format:   str = "delta",
    ):
        self.spark    = spark
        self.database = database
        self.catalog  = catalog
        self.format   = format
        self._ops     = DatabricksTableOps(spark, database, catalog)

    def full_load(self, df: "DataFrame", table: str,
                  partition_by: Optional[List[str]] = None,
                  location: Optional[str] = None) -> "DBLoadResult":
        start = datetime.utcnow()
        rows  = df.count()
        self._ops.create_or_replace(table, df, partition_by=partition_by,
                                    location=location, format=self.format)
        return DBLoadResult(table, "FULL", rows, 0, datetime.utcnow() - start)

    def incremental_load(
        self,
        df:             "DataFrame",
        table:          str,
        watermark_col:  str,
        last_watermark: Optional[str] = None,
    ) -> "DBLoadResult":
        if last_watermark:
            df = df.filter(f"`{watermark_col}` > '{last_watermark}'")
        rows = df.count()
        self._ops.insert_append(table, df)
        return DBLoadResult(table, "INCREMENTAL", rows, 0,
                            datetime.utcnow() - datetime.utcnow())

    def partition_load(self, df: "DataFrame", table: str,
                       partition_keys: List[str]) -> "DBLoadResult":
        from datetime import timedelta
        start = datetime.utcnow()
        self.spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        fqn = self._ops._fqn(table)
        df.write.format(self.format).mode("overwrite").insertInto(fqn, overwrite=True)
        rows = df.count()
        return DBLoadResult(table, "PARTITION", rows, 0, datetime.utcnow() - start)

    def merge_load(self, df: "DataFrame", table: str,
                   match_keys: List[str],
                   update_columns: Optional[List[str]] = None) -> "DBLoadResult":
        start = datetime.utcnow()
        rows  = df.count()
        self._ops.merge(table, df, match_keys, update_columns)
        return DBLoadResult(table, "MERGE", 0, rows, datetime.utcnow() - start)

    def snapshot_load(self, df: "DataFrame", table: str,
                      snapshot_date: Optional[str] = None) -> "DBLoadResult":
        from pyspark.sql import functions as F
        start = datetime.utcnow()
        date  = snapshot_date or datetime.utcnow().strftime("%Y-%m-%d")
        df    = df.withColumn("snapshot_date", F.lit(date).cast("date"))
        rows  = df.count()
        self._ops.insert_append(table, df)
        return DBLoadResult(table, "SNAPSHOT", rows, 0, datetime.utcnow() - start)


class DBLoadResult:
    def __init__(self, table, load_type, rows_inserted, rows_updated, elapsed):
        self.table         = table
        self.load_type     = load_type
        self.rows_inserted = rows_inserted
        self.rows_updated  = rows_updated
        self.elapsed       = elapsed

    def __repr__(self):
        return (
            f"DBLoadResult(table={self.table!r}, type={self.load_type}, "
            f"inserted={self.rows_inserted}, updated={self.rows_updated})"
        )


# ── DatabricksAuditLog ────────────────────────────────────────────────────────

class DatabricksAuditLog:
    """
    Lightweight audit log backed by a Delta table on DBFS.
    Works on Community Edition — no external dependencies.

    Example
    -------
    >>> audit = DatabricksAuditLog(spark, table="silver._audit_log")
    >>> with audit.capture("silver.orders", "FULL_LOAD") as entry:
    ...     loader.full_load(df, "orders")
    ...     entry.rows_inserted = df.count()
    """

    def __init__(self, spark: "SparkSession",
                 table: str = "default._audit_log",
                 job_name: str = "de_utils_job"):
        self.spark    = spark
        self.table    = table
        self.job_name = job_name
        self._buffer: List[Dict] = []
        self._ensure_table()

    def _ensure_table(self) -> None:
        try:
            self.spark.sql(f"DESCRIBE TABLE {self.table}")
        except Exception:
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.table} (
                    run_id          STRING,
                    job_name        STRING,
                    table_name      STRING,
                    operation       STRING,
                    rows_inserted   LONG,
                    rows_updated    LONG,
                    rows_deleted    LONG,
                    status          STRING,
                    error_message   STRING,
                    started_at      TIMESTAMP,
                    ended_at        TIMESTAMP,
                    duration_sec    DOUBLE
                ) USING DELTA
            """)
            log.info("Created audit table %s", self.table)

    def log(self, table: str, operation: str, rows_inserted: int = 0,
            rows_updated: int = 0, rows_deleted: int = 0,
            status: str = "SUCCESS", error: str = "") -> None:
        import uuid
        self._buffer.append({
            "run_id":        str(uuid.uuid4())[:8],
            "job_name":      self.job_name,
            "table_name":    table,
            "operation":     operation,
            "rows_inserted": rows_inserted,
            "rows_updated":  rows_updated,
            "rows_deleted":  rows_deleted,
            "status":        status,
            "error_message": error,
            "started_at":    datetime.utcnow().isoformat(),
            "ended_at":      datetime.utcnow().isoformat(),
            "duration_sec":  0.0,
        })
        self.flush()

    def capture(self, table: str, operation: str):
        """Context manager — auto-captures timing and errors."""
        return _AuditContext(self, table, operation)

    def flush(self) -> None:
        if not self._buffer:
            return
        from pyspark.sql import functions as F
        rows = [(
            r["run_id"], r["job_name"], r["table_name"], r["operation"],
            r["rows_inserted"], r["rows_updated"], r["rows_deleted"],
            r["status"], r["error_message"],
            r["started_at"], r["ended_at"], r["duration_sec"],
        ) for r in self._buffer]
        schema = ("run_id STRING, job_name STRING, table_name STRING, operation STRING, "
                  "rows_inserted LONG, rows_updated LONG, rows_deleted LONG, "
                  "status STRING, error_message STRING, "
                  "started_at STRING, ended_at STRING, duration_sec DOUBLE")
        self.spark.createDataFrame(rows, schema=schema) \
            .write.mode("append").saveAsTable(self.table)
        self._buffer.clear()


class _AuditContext:
    def __init__(self, audit: DatabricksAuditLog, table: str, operation: str):
        self._audit     = audit
        self.table      = table
        self.operation  = operation
        self.rows_inserted = 0
        self.rows_updated  = 0
        self.rows_deleted  = 0
        self._start = None

    def __enter__(self):
        self._start = datetime.utcnow()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        elapsed = (datetime.utcnow() - self._start).total_seconds()
        status  = "FAILED" if exc_type else "SUCCESS"
        error   = str(exc_val) if exc_val else ""
        self._audit.log(
            self.table, self.operation,
            self.rows_inserted, self.rows_updated, self.rows_deleted,
            status, error,
        )
        return False   # never suppress exceptions


# ── DatabricksCheckpoint ──────────────────────────────────────────────────────

class DatabricksCheckpoint:
    """
    Persistent watermark and job state backed by a Delta table on DBFS.
    CE-compatible — no external storage needed.

    Example
    -------
    >>> cp = DatabricksCheckpoint(spark, table="default._job_state",
    ...                           job_name="daily_orders")
    >>> last = cp.get_watermark("updated_at")   # None on first run
    >>> cp.set_watermark("updated_at", "2024-06-01")
    >>> cp.commit()
    """

    def __init__(self, spark: "SparkSession",
                 table: str = "default._job_state",
                 job_name: str = "de_utils_job"):
        self.spark    = spark
        self.table    = table
        self.job_name = job_name
        self._pending: Dict[str, str] = {}
        self._ensure_table()

    def _ensure_table(self) -> None:
        try:
            self.spark.sql(f"DESCRIBE TABLE {self.table}")
        except Exception:
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.table} (
                    job_name    STRING,
                    key_name    STRING,
                    value       STRING,
                    updated_at  TIMESTAMP
                ) USING DELTA
            """)

    def _latest(self, key: str) -> Optional[str]:
        from pyspark.sql import functions as F
        try:
            row = (
                self.spark.table(self.table)
                .filter((F.col("job_name") == self.job_name) & (F.col("key_name") == key))
                .orderBy(F.desc("updated_at"))
                .limit(1)
                .collect()
            )
            return row[0]["value"] if row else None
        except Exception:
            return None

    def get_watermark(self, key: str) -> Optional[str]:
        return self._latest(key)

    def set_watermark(self, key: str, value: str) -> None:
        self._pending[key] = value

    def get(self, key: str) -> Optional[str]:
        return self._latest(key)

    def set(self, key: str, value: str) -> None:
        self._pending[key] = value

    def commit(self) -> None:
        if not self._pending:
            return
        now  = datetime.utcnow().isoformat()
        rows = [(self.job_name, k, v, now) for k, v in self._pending.items()]
        self.spark.createDataFrame(
            rows, schema="job_name STRING, key_name STRING, value STRING, updated_at STRING"
        ).write.mode("append").saveAsTable(self.table)
        self._pending.clear()
        log.info("Checkpoint committed for job '%s'", self.job_name)

    def is_already_run(self, partition: str) -> bool:
        return self._latest(f"done:{partition}") == "1"

    def mark_partition_done(self, partition: str) -> None:
        self._pending[f"done:{partition}"] = "1"
        self.commit()

    def clear_state(self) -> None:
        from pyspark.sql import functions as F
        self.spark.sql(
            f"DELETE FROM {self.table} WHERE job_name = '{self.job_name}'"
        )
        log.info("Checkpoint state cleared for job '%s'", self.job_name)


# ── NotebookUtils ─────────────────────────────────────────────────────────────

class NotebookUtils:
    """
    Thin, type-hinted wrapper around Databricks dbutils for use in
    notebooks and jobs. Makes dbutils testable outside Databricks.

    Example
    -------
    >>> nu = NotebookUtils(dbutils)
    >>> nu.widget_text("env", "dev", "Environment")
    >>> env = nu.get_widget("env")
    >>> nu.exit("done")
    """

    def __init__(self, dbutils: Any = None):
        self._dbutils = dbutils

    def _du(self) -> Any:
        if self._dbutils:
            return self._dbutils
        try:
            import IPython
            shell = IPython.get_ipython()
            if shell and "dbutils" in shell.user_ns:
                return shell.user_ns["dbutils"]
        except Exception:
            pass
        raise DataEngineeringError(
            "dbutils not available. Pass dbutils=dbutils to NotebookUtils()."
        )

    def widget_text(self, name: str, default: str = "", label: str = "") -> None:
        """Create a text input widget."""
        self._du().widgets.text(name, default, label or name)

    def widget_dropdown(self, name: str, default: str, choices: List[str],
                        label: str = "") -> None:
        """Create a dropdown widget."""
        self._du().widgets.dropdown(name, default, choices, label or name)

    def get_widget(self, name: str) -> str:
        """Get widget value."""
        return self._du().widgets.get(name)

    def remove_all_widgets(self) -> None:
        self._du().widgets.removeAll()

    def exit(self, value: str = "") -> None:
        """Exit notebook and pass a value to the calling job."""
        self._du().notebook.exit(value)

    def run_notebook(self, path: str, timeout: int = 3600,
                     arguments: Optional[Dict[str, str]] = None) -> str:
        """Run another notebook and return its exit value."""
        return self._du().notebook.run(path, timeout, arguments or {})

    def ls(self, path: str) -> List[str]:
        return [f.path for f in self._du().fs.ls(path)]

    def cp(self, src: str, dst: str, recurse: bool = False) -> None:
        self._du().fs.cp(src, dst, recurse)

    def rm(self, path: str, recurse: bool = True) -> None:
        self._du().fs.rm(path, recurse)

    def mkdirs(self, path: str) -> None:
        self._du().fs.mkdirs(path)

    def get_secret(self, scope: str, key: str) -> str:
        return self._du().secrets.get(scope=scope, key=key)
