"""
de_utils — Data Engineering Utility Library for Azure
======================================================
v2.1 — batteries-included library for Azure ADLS, Hive Metastore,
SCD operations, data loading, data quality, profiling, lineage,
job checkpointing, Delta Lake, medallion architecture, alerting,
secrets management, retry/resilience, and pipeline orchestration.
"""

from .config import ADLSConfig, HiveConfig, SparkConfig
from .config_loader import ConfigLoader
from .storage import ADLSConnector
from .metastore import HiveMetastore
from .schema import SchemaManager
from .table_ops import TableOperations
from .loader import DataLoader, LoadType, LoadResult
from .scd import SCDHandler, SCDType
from .history import HistoryTable
from .quality import DataQualityChecker, Rule, DQReport, Severity
from .profiler import DataProfiler, DataProfile, ColumnProfile
from .lineage import LineageTracker
from .checkpoint import JobCheckpoint
from .delta import DeltaUtils
from .audit import AuditLog
from .medallion import BronzeLoader, SilverLoader, GoldLoader
from .expectations import ExpectationSuiteBuilder, ExpectationSuite, ValidatingLoader
from .alerts import AlertRouter, EmailAlerter, TeamsAlerter, SlackAlerter, PagerDutyAlerter, alert_on_failure
from .secrets import SecretsChain, AzureKeyVaultSecrets, EnvSecrets, StaticSecrets
from .retry import retry, RetryPolicy, CircuitBreaker, with_retry
from .pipeline import Pipeline, PipelineResult, TaskStatus
from .utils import get_logger, DataEngineeringError

__version__ = "2.1.0"
__author__  = "Data Engineering Team"

__all__ = [
    "ADLSConfig", "HiveConfig", "SparkConfig", "ConfigLoader",
    "ADLSConnector", "HiveMetastore", "SchemaManager",
    "TableOperations", "DataLoader", "LoadType", "LoadResult",
    "SCDHandler", "SCDType", "HistoryTable",
    "DataQualityChecker", "Rule", "DQReport", "Severity",
    "DataProfiler", "DataProfile", "ColumnProfile",
    "LineageTracker", "JobCheckpoint", "DeltaUtils", "AuditLog",
    "BronzeLoader", "SilverLoader", "GoldLoader",
    "ExpectationSuiteBuilder", "ExpectationSuite", "ValidatingLoader",
    "AlertRouter", "EmailAlerter", "TeamsAlerter", "SlackAlerter",
    "PagerDutyAlerter", "alert_on_failure",
    "SecretsChain", "AzureKeyVaultSecrets", "EnvSecrets", "StaticSecrets",
    "retry", "RetryPolicy", "CircuitBreaker", "with_retry",
    "Pipeline", "PipelineResult", "TaskStatus",
    "get_logger", "DataEngineeringError",
]

# ── Databricks compatibility ───────────────────────────────────────────────────
from .databricks import (
    DatabricksConfig,
    DatabricksSession,
    DatabricksSecrets,
    DBFSConnector,
    MountConnector,
    DatabricksTableOps,
    DatabricksLoader,
    DatabricksAuditLog,
    DatabricksCheckpoint,
    NotebookUtils,
    is_databricks,
    is_community_edition,
)
