"""
de_utils - Data Engineering Utility Library for Azure
======================================================
A batteries-included library for Azure Data Lake Storage,
Hive Metastore, SCD operations, and data loading patterns.
"""

from .config import ADLSConfig, HiveConfig, SparkConfig
from .storage import ADLSConnector
from .metastore import HiveMetastore
from .table_ops import TableOperations
from .loader import DataLoader
from .scd import SCDHandler
from .history import HistoryTable
from .schema import SchemaManager
from .utils import get_logger, DataEngineeringError

__version__ = "1.0.0"
__author__ = "Data Engineering Team"

__all__ = [
    "ADLSConfig",
    "HiveConfig",
    "SparkConfig",
    "ADLSConnector",
    "HiveMetastore",
    "TableOperations",
    "DataLoader",
    "SCDHandler",
    "HistoryTable",
    "SchemaManager",
    "get_logger",
    "DataEngineeringError",
]
