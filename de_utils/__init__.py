"""
de_utils — Data Engineering Utility Library for Azure
======================================================
v2.0 — batteries-included library for Azure ADLS, Hive Metastore,
SCD operations, data loading, data quality, lineage tracking,
job checkpointing, Delta Lake utilities, medallion architecture,
and Great Expectations integration.
"""

# ── Core config ───────────────────────────────────────────────────────────────
from .config import ADLSConfig, HiveConfig, SparkConfig
from .config_loader import ConfigLoader

# ── Connectivity ──────────────────────────────────────────────────────────────
from .storage import ADLSConnector
from .metastore import HiveMetastore

# ── Schema ────────────────────────────────────────────────────────────────────
from .schema import SchemaManager

# ── Table operations ──────────────────────────────────────────────────────────
from .table_ops import TableOperations
from .loader import DataLoader, LoadType, LoadResult

# ── SCD + History ─────────────────────────────────────────────────────────────
from .scd import SCDHandler, SCDType
from .history import HistoryTable

# ── Data Quality ──────────────────────────────────────────────────────────────
from .quality import DataQualityChecker, Rule, DQReport

# ── Lineage ───────────────────────────────────────────────────────────────────
from .lineage import LineageTracker

# ── Checkpointing ─────────────────────────────────────────────────────────────
from .checkpoint import JobCheckpoint

# ── Delta utilities ───────────────────────────────────────────────────────────
from .delta import DeltaUtils

# ── Audit log ─────────────────────────────────────────────────────────────────
from .audit import AuditLog

# ── Medallion architecture ────────────────────────────────────────────────────
from .medallion import BronzeLoader, SilverLoader, GoldLoader

# ── Great Expectations integration ────────────────────────────────────────────
from .expectations import ExpectationSuiteBuilder, ExpectationSuite, ValidatingLoader

# ── Shared helpers ────────────────────────────────────────────────────────────
from .utils import get_logger, DataEngineeringError

__version__ = "2.0.0"
__author__  = "Data Engineering Team"

__all__ = [
    "ADLSConfig", "HiveConfig", "SparkConfig", "ConfigLoader",
    "ADLSConnector", "HiveMetastore",
    "SchemaManager",
    "TableOperations", "DataLoader", "LoadType", "LoadResult",
    "SCDHandler", "SCDType", "HistoryTable",
    "DataQualityChecker", "Rule", "DQReport",
    "LineageTracker",
    "JobCheckpoint",
    "DeltaUtils",
    "AuditLog",
    "BronzeLoader", "SilverLoader", "GoldLoader",
    "ExpectationSuiteBuilder", "ExpectationSuite", "ValidatingLoader",
    "get_logger", "DataEngineeringError",
]
