"""
de_utils.quality
----------------
Declarative data quality rule engine.

Define rules once, run them against any DataFrame, get a structured report
that can be persisted to a quality log table.

Supported rule types
--------------------
  not_null          — column must have no NULL values
  unique            — column(s) must form a unique key
  min_value         — numeric column must be >= threshold
  max_value         — numeric column must be <= threshold
  between           — numeric column must be within [min, max]
  regex             — string column must match a regex pattern
  allowed_values    — column value must be in an allowed set
  referential       — every value in column must exist in a reference DataFrame
  row_count         — total row count must satisfy a condition (>, >=, ==, <=, <)
  custom_sql        — arbitrary SQL expression that must return 0 failing rows

Usage
-----
>>> from de_utils.quality import DataQualityChecker, Rule
>>> qc = DataQualityChecker(spark, table_name="orders")
>>> qc.add_rule(Rule.not_null("order_id"))
>>> qc.add_rule(Rule.unique("order_id"))
>>> qc.add_rule(Rule.between("amount", 0, 1_000_000))
>>> qc.add_rule(Rule.allowed_values("status", ["PENDING","SHIPPED","DELIVERED","CANCELLED"]))
>>> qc.add_rule(Rule.row_count(">=", 1))
>>> report = qc.run(df)
>>> report.assert_no_failures()   # raises if any rule failed
>>> report.save(spark, "gold._dq_log")
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from .utils import get_logger, DataEngineeringError

if TYPE_CHECKING:
    from pyspark.sql import SparkSession, DataFrame

log = get_logger(__name__)


class RuleType(str, Enum):
    NOT_NULL       = "not_null"
    UNIQUE         = "unique"
    MIN_VALUE      = "min_value"
    MAX_VALUE      = "max_value"
    BETWEEN        = "between"
    REGEX          = "regex"
    ALLOWED_VALUES = "allowed_values"
    REFERENTIAL    = "referential"
    ROW_COUNT      = "row_count"
    CUSTOM_SQL     = "custom_sql"


@dataclass
class Rule:
    rule_type:   RuleType
    columns:     List[str]
    params:      Dict[str, Any] = field(default_factory=dict)
    description: str = ""
    severity:    str = "ERROR"   # ERROR | WARNING

    # ── Factory helpers ────────────────────────────────────────────────

    @staticmethod
    def not_null(column: str, severity: str = "ERROR") -> "Rule":
        return Rule(RuleType.NOT_NULL, [column], severity=severity,
                    description=f"Column '{column}' must not be NULL")

    @staticmethod
    def unique(*columns: str, severity: str = "ERROR") -> "Rule":
        cols = list(columns)
        return Rule(RuleType.UNIQUE, cols, severity=severity,
                    description=f"Columns {cols} must form a unique key")

    @staticmethod
    def min_value(column: str, minimum: float, severity: str = "ERROR") -> "Rule":
        return Rule(RuleType.MIN_VALUE, [column], params={"min": minimum},
                    severity=severity,
                    description=f"Column '{column}' must be >= {minimum}")

    @staticmethod
    def max_value(column: str, maximum: float, severity: str = "ERROR") -> "Rule":
        return Rule(RuleType.MAX_VALUE, [column], params={"max": maximum},
                    severity=severity,
                    description=f"Column '{column}' must be <= {maximum}")

    @staticmethod
    def between(column: str, minimum: float, maximum: float, severity: str = "ERROR") -> "Rule":
        return Rule(RuleType.BETWEEN, [column], params={"min": minimum, "max": maximum},
                    severity=severity,
                    description=f"Column '{column}' must be between {minimum} and {maximum}")

    @staticmethod
    def regex(column: str, pattern: str, severity: str = "ERROR") -> "Rule":
        return Rule(RuleType.REGEX, [column], params={"pattern": pattern},
                    severity=severity,
                    description=f"Column '{column}' must match regex '{pattern}'")

    @staticmethod
    def allowed_values(column: str, values: List[Any], severity: str = "ERROR") -> "Rule":
        return Rule(RuleType.ALLOWED_VALUES, [column], params={"values": values},
                    severity=severity,
                    description=f"Column '{column}' must be in {values}")

    @staticmethod
    def referential(column: str, ref_df: "DataFrame", ref_column: str,
                    severity: str = "ERROR") -> "Rule":
        return Rule(RuleType.REFERENTIAL, [column],
                    params={"ref_df": ref_df, "ref_column": ref_column},
                    severity=severity,
                    description=f"Column '{column}' must exist in reference.{ref_column}")

    @staticmethod
    def row_count(operator: str, threshold: int, severity: str = "ERROR") -> "Rule":
        """operator: '>', '>=', '==', '<=', '<'"""
        return Rule(RuleType.ROW_COUNT, [],
                    params={"operator": operator, "threshold": threshold},
                    severity=severity,
                    description=f"Row count must be {operator} {threshold}")

    @staticmethod
    def custom_sql(expression: str, description: str = "", severity: str = "ERROR") -> "Rule":
        """
        SQL expression evaluated against a temp view '__dq_df__'.
        Must return 0 rows to pass.

        Example
        -------
        Rule.custom_sql("SELECT * FROM __dq_df__ WHERE amount < 0 AND status = 'DELIVERED'")
        """
        return Rule(RuleType.CUSTOM_SQL, [],
                    params={"expression": expression},
                    severity=severity,
                    description=description or f"Custom SQL: {expression[:60]}")


@dataclass
class RuleResult:
    rule:          Rule
    passed:        bool
    failing_count: int
    total_count:   int
    details:       str
    checked_at:    datetime = field(default_factory=datetime.utcnow)

    @property
    def pass_rate(self) -> float:
        return (self.total_count - self.failing_count) / max(self.total_count, 1)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "rule_type":     self.rule.rule_type.value,
            "columns":       ",".join(self.rule.columns),
            "description":   self.rule.description,
            "severity":      self.rule.severity,
            "passed":        self.passed,
            "failing_count": self.failing_count,
            "total_count":   self.total_count,
            "pass_rate":     round(self.pass_rate, 4),
            "details":       self.details,
            "checked_at":    str(self.checked_at),
        }


class DQReport:
    """Result of running a DataQualityChecker against a DataFrame."""

    def __init__(self, table_name: str, results: List[RuleResult], run_id: str = ""):
        self.table_name  = table_name
        self.results     = results
        self.run_id      = run_id or datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        self.created_at  = datetime.utcnow()

    @property
    def passed(self) -> bool:
        return all(r.passed or r.rule.severity != "ERROR" for r in self.results)

    @property
    def failures(self) -> List[RuleResult]:
        return [r for r in self.results if not r.passed]

    @property
    def warnings(self) -> List[RuleResult]:
        return [r for r in self.results if not r.passed and r.rule.severity == "WARNING"]

    @property
    def errors(self) -> List[RuleResult]:
        return [r for r in self.results if not r.passed and r.rule.severity == "ERROR"]

    def assert_no_failures(self) -> None:
        """Raise DataEngineeringError if any ERROR-severity rules failed."""
        if self.errors:
            msgs = [f"  [{r.rule.rule_type.value}] {r.rule.description}: {r.details}"
                    for r in self.errors]
            raise DataEngineeringError(
                f"Data quality check failed for '{self.table_name}':\n" + "\n".join(msgs)
            )

    def print_summary(self) -> None:
        total   = len(self.results)
        passed  = sum(1 for r in self.results if r.passed)
        print(f"\n{'='*60}")
        print(f"  DQ Report: {self.table_name}  [{self.run_id}]")
        print(f"  {passed}/{total} rules passed")
        print(f"{'='*60}")
        for r in self.results:
            icon = "✓" if r.passed else ("⚠" if r.rule.severity == "WARNING" else "✗")
            print(f"  {icon}  [{r.rule.rule_type.value:<16}] {r.rule.description}")
            if not r.passed:
                print(f"       → {r.details}")
        print()

    def save(self, spark: "SparkSession", target_table: str) -> None:
        """Persist this report to a Delta/Hive table."""
        rows = [
            (
                self.run_id,
                self.table_name,
                r.rule.rule_type.value,
                ",".join(r.rule.columns),
                r.rule.description,
                r.rule.severity,
                r.passed,
                r.failing_count,
                r.total_count,
                float(r.pass_rate),
                r.details,
                str(r.checked_at),
            )
            for r in self.results
        ]
        from pyspark.sql.types import (
            StructType, StructField, StringType, BooleanType,
            LongType, DoubleType,
        )
        schema = StructType([
            StructField("run_id",        StringType()),
            StructField("table_name",    StringType()),
            StructField("rule_type",     StringType()),
            StructField("columns",       StringType()),
            StructField("description",   StringType()),
            StructField("severity",      StringType()),
            StructField("passed",        BooleanType()),
            StructField("failing_count", LongType()),
            StructField("total_count",   LongType()),
            StructField("pass_rate",     DoubleType()),
            StructField("details",       StringType()),
            StructField("checked_at",    StringType()),
        ])
        df = spark.createDataFrame(rows, schema=schema)
        df.write.mode("append").saveAsTable(target_table)
        log.info("DQ report saved to %s (%d rows)", target_table, len(rows))


class DataQualityChecker:
    """
    Declarative data quality engine.

    Example
    -------
    >>> qc = DataQualityChecker(spark, table_name="silver.orders")
    >>> qc.add_rules([
    ...     Rule.not_null("order_id"),
    ...     Rule.unique("order_id"),
    ...     Rule.between("amount", 0, 1_000_000),
    ...     Rule.allowed_values("status", ["PENDING","SHIPPED","DELIVERED"]),
    ...     Rule.row_count(">=", 1),
    ... ])
    >>> report = qc.run(df)
    >>> report.assert_no_failures()
    """

    def __init__(self, spark: "SparkSession", table_name: str = "unknown"):
        self.spark      = spark
        self.table_name = table_name
        self._rules: List[Rule] = []

    def add_rule(self, rule: Rule) -> "DataQualityChecker":
        self._rules.append(rule)
        return self

    def add_rules(self, rules: List[Rule]) -> "DataQualityChecker":
        self._rules.extend(rules)
        return self

    def run(self, df: "DataFrame", run_id: str = "") -> DQReport:
        """Execute all rules against *df* and return a DQReport."""
        from pyspark.sql import functions as F

        total = df.count()
        df.createOrReplaceTempView("__dq_df__")
        results: List[RuleResult] = []

        for rule in self._rules:
            try:
                result = self._evaluate(rule, df, total, F)
            except Exception as e:
                result = RuleResult(rule=rule, passed=False, failing_count=-1,
                                    total_count=total,
                                    details=f"Evaluation error: {e}")
            log.info("DQ [%s] %s → %s", rule.rule_type.value,
                     rule.description, "PASS" if result.passed else "FAIL")
            results.append(result)

        return DQReport(self.table_name, results, run_id)

    # ------------------------------------------------------------------
    # Rule evaluators
    # ------------------------------------------------------------------

    def _evaluate(self, rule: Rule, df: "DataFrame", total: int, F) -> RuleResult:
        rt = rule.rule_type

        if rt == RuleType.NOT_NULL:
            col = rule.columns[0]
            null_count = df.filter(F.col(col).isNull()).count()
            return RuleResult(rule=rule, passed=null_count == 0,
                              failing_count=null_count, total_count=total,
                              details=f"{null_count} NULL values in '{col}'")

        elif rt == RuleType.UNIQUE:
            distinct = df.select(*rule.columns).distinct().count()
            dupes = total - distinct
            return RuleResult(rule=rule, passed=dupes == 0,
                              failing_count=dupes, total_count=total,
                              details=f"{dupes} duplicate key combinations on {rule.columns}")

        elif rt == RuleType.MIN_VALUE:
            col, mn = rule.columns[0], rule.params["min"]
            fail = df.filter(F.col(col) < mn).count()
            return RuleResult(rule=rule, passed=fail == 0,
                              failing_count=fail, total_count=total,
                              details=f"{fail} rows where '{col}' < {mn}")

        elif rt == RuleType.MAX_VALUE:
            col, mx = rule.columns[0], rule.params["max"]
            fail = df.filter(F.col(col) > mx).count()
            return RuleResult(rule=rule, passed=fail == 0,
                              failing_count=fail, total_count=total,
                              details=f"{fail} rows where '{col}' > {mx}")

        elif rt == RuleType.BETWEEN:
            col = rule.columns[0]
            mn, mx = rule.params["min"], rule.params["max"]
            fail = df.filter((F.col(col) < mn) | (F.col(col) > mx)).count()
            return RuleResult(rule=rule, passed=fail == 0,
                              failing_count=fail, total_count=total,
                              details=f"{fail} rows outside [{mn}, {mx}]")

        elif rt == RuleType.REGEX:
            col, pattern = rule.columns[0], rule.params["pattern"]
            fail = df.filter(~F.col(col).rlike(pattern) | F.col(col).isNull()).count()
            return RuleResult(rule=rule, passed=fail == 0,
                              failing_count=fail, total_count=total,
                              details=f"{fail} rows not matching pattern '{pattern}'")

        elif rt == RuleType.ALLOWED_VALUES:
            col, values = rule.columns[0], rule.params["values"]
            fail = df.filter(~F.col(col).isin(values)).count()
            return RuleResult(rule=rule, passed=fail == 0,
                              failing_count=fail, total_count=total,
                              details=f"{fail} rows with disallowed values in '{col}'")

        elif rt == RuleType.REFERENTIAL:
            col       = rule.columns[0]
            ref_df    = rule.params["ref_df"]
            ref_col   = rule.params["ref_column"]
            ref_vals  = ref_df.select(F.col(ref_col).alias("__ref__")).distinct()
            fail = df.join(ref_vals, df[col] == ref_vals["__ref__"], "left_anti").count()
            return RuleResult(rule=rule, passed=fail == 0,
                              failing_count=fail, total_count=total,
                              details=f"{fail} rows with no matching value in reference '{ref_col}'")

        elif rt == RuleType.ROW_COUNT:
            op, threshold = rule.params["operator"], rule.params["threshold"]
            ops = {">": lambda a, b: a > b, ">=": lambda a, b: a >= b,
                   "==": lambda a, b: a == b, "<=": lambda a, b: a <= b,
                   "<": lambda a, b: a < b}
            passed = ops[op](total, threshold)
            return RuleResult(rule=rule, passed=passed,
                              failing_count=0 if passed else 1, total_count=total,
                              details=f"Row count {total} {'satisfies' if passed else 'does not satisfy'} {op} {threshold}")

        elif rt == RuleType.CUSTOM_SQL:
            expr = rule.params["expression"]
            fail = self.spark.sql(expr).count()
            return RuleResult(rule=rule, passed=fail == 0,
                              failing_count=fail, total_count=total,
                              details=f"{fail} rows returned by custom SQL expression")

        raise DataEngineeringError(f"Unknown rule type: {rt}")
