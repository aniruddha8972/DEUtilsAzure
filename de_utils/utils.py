"""
de_utils.utils
--------------
Shared helpers: logging, exceptions, and small utility functions.
"""

import logging
import sys
from typing import List, Optional


class DataEngineeringError(Exception):
    """Base exception for all de_utils errors."""


def get_logger(name: str = "de_utils", level: int = logging.INFO) -> logging.Logger:
    """
    Return a consistently formatted logger.

    Example
    -------
    >>> log = get_logger("my_job")
    >>> log.info("Starting ETL...")
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        fmt = logging.Formatter(
            "[%(asctime)s] %(levelname)-8s [%(name)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(fmt)
        logger.addHandler(handler)
    logger.setLevel(level)
    return logger


def validate_columns(df_columns: List[str], required: List[str], label: str = "DataFrame") -> None:
    """Raise DataEngineeringError if any required columns are missing."""
    missing = set(required) - set(df_columns)
    if missing:
        raise DataEngineeringError(
            f"{label} is missing required columns: {sorted(missing)}"
        )


def quote_identifier(name: str) -> str:
    """Wrap a table/column name in backticks for Spark SQL safety."""
    return f"`{name}`"


def build_partition_clause(partition_keys: List[str], alias: Optional[str] = None) -> str:
    """
    Build a SQL WHERE clause fragment for partition pruning.

    Example
    -------
    >>> build_partition_clause(["year", "month"], alias="t")
    't.`year` = s.`year` AND t.`month` = s.`month`'
    """
    prefix = f"{alias}." if alias else ""
    parts = [
        f"{prefix}{quote_identifier(k)} = s.{quote_identifier(k)}"
        for k in partition_keys
    ]
    return " AND ".join(parts)
