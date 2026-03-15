"""
de_utils.profiler
-----------------
Automatic data profiling — generates statistical summaries, distributions,
null rates, cardinality estimates, outlier detection, and correlation matrices
for any Spark DataFrame.

The profile report can be printed, serialised to JSON, or persisted to a
Hive table for trending over time.

Usage
-----
>>> from de_utils.profiler import DataProfiler

>>> profiler = DataProfiler(spark)
>>> profile  = profiler.profile(orders_df, table_name="silver.orders")
>>> profile.show()
>>> profile.to_json()
>>> profiler.save(profile, "gold._profiles")

>>> # Profile only specific columns
>>> profile = profiler.profile(df, columns=["amount", "status", "customer_id"])

>>> # Compare two profiles (e.g. today vs yesterday) to detect drift
>>> drift = profiler.compare(profile_today, profile_yesterday)
>>> drift.show()
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from .utils import get_logger, DataEngineeringError

if TYPE_CHECKING:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.types import StructType

log = get_logger(__name__)


@dataclass
class ColumnProfile:
    name:          str
    dtype:         str
    total_rows:    int
    null_count:    int
    null_pct:      float
    distinct_count: int
    distinct_pct:  float
    # Numeric stats (None for non-numeric)
    min_val:       Optional[float] = None
    max_val:       Optional[float] = None
    mean_val:      Optional[float] = None
    stddev_val:    Optional[float] = None
    p25:           Optional[float] = None
    p50:           Optional[float] = None
    p75:           Optional[float] = None
    p95:           Optional[float] = None
    p99:           Optional[float] = None
    # String stats (None for non-string)
    min_length:    Optional[int]   = None
    max_length:    Optional[int]   = None
    avg_length:    Optional[float] = None
    blank_count:   Optional[int]   = None
    # Top frequent values (all columns)
    top_values:    List[Dict]      = field(default_factory=list)

    @property
    def is_complete(self) -> bool:
        return self.null_pct == 0.0

    @property
    def is_unique(self) -> bool:
        return self.distinct_count == self.total_rows

    def to_dict(self) -> Dict:
        return {k: v for k, v in self.__dict__.items()}


@dataclass
class DataProfile:
    table_name:    str
    profiled_at:   datetime
    total_rows:    int
    total_columns: int
    columns:       List[ColumnProfile] = field(default_factory=list)

    # Summary stats
    @property
    def completeness_pct(self) -> float:
        """Average non-null rate across all columns."""
        if not self.columns:
            return 100.0
        return round(
            sum(1.0 - c.null_pct for c in self.columns) / len(self.columns) * 100, 2
        )

    @property
    def columns_with_nulls(self) -> List[str]:
        return [c.name for c in self.columns if c.null_count > 0]

    @property
    def constant_columns(self) -> List[str]:
        """Columns with only one distinct value — often a sign of bad data."""
        return [c.name for c in self.columns if c.distinct_count == 1]

    @property
    def high_cardinality_columns(self) -> List[str]:
        """Columns where distinct count > 50% of rows."""
        return [c.name for c in self.columns if c.distinct_pct > 0.5]

    def get_column(self, name: str) -> Optional[ColumnProfile]:
        return next((c for c in self.columns if c.name == name), None)

    def show(self) -> None:
        print(f"\n{'='*76}")
        print(f"  Data Profile — {self.table_name}")
        print(f"  Profiled: {self.profiled_at.isoformat()}   "
              f"Rows: {self.total_rows:,}   Cols: {self.total_columns}   "
              f"Completeness: {self.completeness_pct:.1f}%")
        print(f"{'='*76}")
        print(f"  {'Column':<22} {'Type':<12} {'Nulls':>7} {'Distinct':>9} "
              f"{'Min':>12} {'Max':>12} {'Mean':>12}")
        print(f"  {'-'*22} {'-'*12} {'-'*7} {'-'*9} {'-'*12} {'-'*12} {'-'*12}")
        for c in self.columns:
            null_str  = f"{c.null_pct*100:.1f}%" if c.null_count > 0 else "—"
            dist_str  = f"{c.distinct_count:,} ({c.distinct_pct*100:.0f}%)"
            min_str   = f"{c.min_val:.2f}"  if c.min_val  is not None else "—"
            max_str   = f"{c.max_val:.2f}"  if c.max_val  is not None else "—"
            mean_str  = f"{c.mean_val:.2f}" if c.mean_val is not None else "—"
            print(f"  {c.name:<22} {c.dtype:<12} {null_str:>7} {dist_str:>9} "
                  f"{min_str:>12} {max_str:>12} {mean_str:>12}")
        if self.columns_with_nulls:
            print(f"\n  ⚠️  Columns with nulls : {', '.join(self.columns_with_nulls)}")
        if self.constant_columns:
            print(f"  ⚠️  Constant columns   : {', '.join(self.constant_columns)}")
        print(f"{'='*76}\n")

    def to_dict(self) -> Dict:
        return {
            "table_name":    self.table_name,
            "profiled_at":   self.profiled_at.isoformat(),
            "total_rows":    self.total_rows,
            "total_columns": self.total_columns,
            "completeness_pct": self.completeness_pct,
            "columns_with_nulls":       self.columns_with_nulls,
            "constant_columns":         self.constant_columns,
            "high_cardinality_columns": self.high_cardinality_columns,
            "columns": [c.to_dict() for c in self.columns],
        }

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, default=str)


class DataProfiler:
    """
    Profile any Spark DataFrame — generates per-column statistics,
    top-value frequencies, and a summary report.
    """

    def __init__(self, spark: "SparkSession", top_n: int = 5):
        self.spark = spark
        self.top_n = top_n

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def profile(
        self,
        df: "DataFrame",
        table_name: str = "unknown",
        columns: Optional[List[str]] = None,
        sample_fraction: Optional[float] = None,
    ) -> DataProfile:
        """
        Profile a DataFrame and return a DataProfile.

        Parameters
        ----------
        df               : DataFrame to profile
        table_name       : Label for the report
        columns          : Subset of columns (default = all)
        sample_fraction  : Profile on a sample (e.g. 0.1 for 10 %)
        """
        from pyspark.sql import functions as F
        from pyspark.sql.types import (
            NumericType, StringType, BooleanType,
            DateType, TimestampType,
        )

        if sample_fraction:
            df = df.sample(fraction=sample_fraction, seed=42)
            log.info("Profiling on %.0f%% sample", sample_fraction * 100)

        cols     = columns or df.columns
        total    = df.count()
        profiled_at = datetime.utcnow()
        log.info("Profiling %d columns × %d rows for '%s'", len(cols), total, table_name)

        column_profiles = []
        for col_name in cols:
            try:
                cp = self._profile_column(df, col_name, total, F)
                column_profiles.append(cp)
            except Exception as exc:
                log.warning("Could not profile column '%s': %s", col_name, exc)

        return DataProfile(
            table_name=table_name,
            profiled_at=profiled_at,
            total_rows=total,
            total_columns=len(cols),
            columns=column_profiles,
        )

    # ------------------------------------------------------------------
    # Per-column profiling
    # ------------------------------------------------------------------

    def _profile_column(self, df: "DataFrame", col_name: str, total: int, F) -> ColumnProfile:
        from pyspark.sql.types import (
            NumericType, IntegralType, FractionalType,
            StringType, BooleanType, DateType, TimestampType,
        )

        dtype_obj = df.schema[col_name].dataType
        dtype_str = dtype_obj.simpleString()

        # Null count + distinct
        agg_result = df.agg(
            F.count(F.when(F.col(col_name).isNull(), 1)).alias("nulls"),
            F.countDistinct(F.col(col_name)).alias("distinct"),
        ).collect()[0]

        null_count    = int(agg_result["nulls"])
        distinct_count= int(agg_result["distinct"])
        null_pct      = round(null_count / total, 4) if total > 0 else 0.0
        distinct_pct  = round(distinct_count / total, 4) if total > 0 else 0.0

        # Top N values
        top_values = (
            df.groupBy(col_name)
            .count()
            .orderBy(F.desc("count"))
            .limit(self.top_n)
            .collect()
        )
        top_vals = [{"value": str(r[col_name]), "count": r["count"]} for r in top_values]

        cp = ColumnProfile(
            name=col_name,
            dtype=dtype_str,
            total_rows=total,
            null_count=null_count,
            null_pct=null_pct,
            distinct_count=distinct_count,
            distinct_pct=distinct_pct,
            top_values=top_vals,
        )

        # Numeric stats
        if isinstance(dtype_obj, NumericType):
            stats = df.select(
                F.min(col_name).alias("min"),
                F.max(col_name).alias("max"),
                F.mean(col_name).alias("mean"),
                F.stddev(col_name).alias("std"),
            ).collect()[0]
            percentiles = df.stat.approxQuantile(col_name, [0.25, 0.50, 0.75, 0.95, 0.99], 0.01)
            cp.min_val    = float(stats["min"])  if stats["min"]  is not None else None
            cp.max_val    = float(stats["max"])  if stats["max"]  is not None else None
            cp.mean_val   = float(stats["mean"]) if stats["mean"] is not None else None
            cp.stddev_val = float(stats["std"])  if stats["std"]  is not None else None
            if percentiles:
                cp.p25, cp.p50, cp.p75, cp.p95, cp.p99 = (
                    float(p) if p is not None else None for p in percentiles
                )

        # String stats
        if isinstance(dtype_obj, StringType):
            str_stats = df.agg(
                F.min(F.length(col_name)).alias("min_len"),
                F.max(F.length(col_name)).alias("max_len"),
                F.avg(F.length(col_name)).alias("avg_len"),
                F.count(F.when(F.trim(F.col(col_name)) == "", 1)).alias("blanks"),
            ).collect()[0]
            cp.min_length  = int(str_stats["min_len"])   if str_stats["min_len"] is not None else None
            cp.max_length  = int(str_stats["max_len"])   if str_stats["max_len"] is not None else None
            cp.avg_length  = float(str_stats["avg_len"]) if str_stats["avg_len"] is not None else None
            cp.blank_count = int(str_stats["blanks"])

        return cp

    # ------------------------------------------------------------------
    # Profile drift / comparison
    # ------------------------------------------------------------------

    def compare(self, profile_a: DataProfile, profile_b: DataProfile) -> "DataFrame":
        """
        Compare two profiles column-by-column and return a DataFrame
        showing null-rate drift, distinct-count change, and mean drift.
        """
        rows = []
        cols_a = {c.name: c for c in profile_a.columns}
        cols_b = {c.name: c for c in profile_b.columns}
        all_cols = sorted(set(cols_a) | set(cols_b))

        for col in all_cols:
            ca = cols_a.get(col)
            cb = cols_b.get(col)
            if ca is None:
                rows.append((col, "ADDED", None, None, None, None, None, None))
            elif cb is None:
                rows.append((col, "DROPPED", None, None, None, None, None, None))
            else:
                null_drift    = round(cb.null_pct   - ca.null_pct, 4)
                distinct_drift= cb.distinct_count   - ca.distinct_count
                mean_drift    = (
                    round(cb.mean_val - ca.mean_val, 4)
                    if cb.mean_val is not None and ca.mean_val is not None else None
                )
                status = "CHANGED" if abs(null_drift) > 0.05 or abs(distinct_drift) > ca.distinct_count * 0.2 else "OK"
                rows.append((
                    col, status,
                    ca.null_pct, cb.null_pct, null_drift,
                    ca.distinct_count, cb.distinct_count, distinct_drift,
                ))

        schema = (
            "column STRING, status STRING, "
            "null_pct_a DOUBLE, null_pct_b DOUBLE, null_drift DOUBLE, "
            "distinct_a LONG, distinct_b LONG, distinct_drift LONG"
        )
        return self.spark.createDataFrame(rows, schema=schema)

    # ------------------------------------------------------------------
    # Outlier detection
    # ------------------------------------------------------------------

    def detect_outliers(
        self,
        df: "DataFrame",
        columns: List[str],
        method: str = "iqr",
        threshold: float = 1.5,
    ) -> "DataFrame":
        """
        Flag rows with outlier values using IQR or Z-score method.

        Parameters
        ----------
        method    : 'iqr' (default) or 'zscore'
        threshold : IQR multiplier (default 1.5) or Z-score threshold (default 3.0)

        Returns the original DataFrame with a boolean `_is_outlier` column.
        """
        from pyspark.sql import functions as F

        result_df = df
        outlier_flags = []

        for col_name in columns:
            if method == "iqr":
                q1, q3 = df.stat.approxQuantile(col_name, [0.25, 0.75], 0.01)
                iqr    = q3 - q1
                lo, hi = q1 - threshold * iqr, q3 + threshold * iqr
                flag   = (F.col(col_name) < lo) | (F.col(col_name) > hi)

            elif method == "zscore":
                stats  = df.agg(F.mean(col_name), F.stddev(col_name)).collect()[0]
                mean, std = float(stats[0] or 0), float(stats[1] or 1)
                flag   = F.abs((F.col(col_name) - mean) / std) > threshold

            else:
                raise DataEngineeringError(f"Unknown outlier method: {method!r}")

            outlier_flags.append(flag)

        combined = outlier_flags[0]
        for f in outlier_flags[1:]:
            combined = combined | f

        return result_df.withColumn("_is_outlier", combined)

    # ------------------------------------------------------------------
    # Persist
    # ------------------------------------------------------------------

    def save(self, profile: DataProfile, table: str, mode: str = "append") -> None:
        """Persist a profile's column stats to a Hive table for trending."""
        rows = []
        for c in profile.columns:
            rows.append((
                profile.table_name,
                profile.profiled_at.isoformat(),
                profile.total_rows,
                c.name, c.dtype,
                c.null_count, c.null_pct,
                c.distinct_count, c.distinct_pct,
                c.min_val, c.max_val, c.mean_val, c.stddev_val,
                c.p50, c.p95,
                c.min_length, c.max_length,
                json.dumps(c.top_values),
            ))
        schema = (
            "table_name STRING, profiled_at STRING, total_rows LONG, "
            "col_name STRING, dtype STRING, "
            "null_count LONG, null_pct DOUBLE, "
            "distinct_count LONG, distinct_pct DOUBLE, "
            "min_val DOUBLE, max_val DOUBLE, mean_val DOUBLE, stddev_val DOUBLE, "
            "p50 DOUBLE, p95 DOUBLE, "
            "min_length INT, max_length INT, top_values STRING"
        )
        self.spark.createDataFrame(rows, schema=schema) \
            .write.mode(mode).saveAsTable(table)
        log.info("Profile for '%s' saved to '%s'", profile.table_name, table)
