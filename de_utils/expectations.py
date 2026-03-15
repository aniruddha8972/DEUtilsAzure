"""
de_utils.expectations
---------------------
Lightweight Great Expectations (GX) integration that auto-generates
expectation suites from Spark schemas and wires them into the
DataLoader pipeline.

If great_expectations is not installed, this module gracefully falls back
to de_utils.quality for rule-based validation so pipelines keep running.

Usage — auto-generate suite from schema
----------------------------------------
>>> from de_utils.expectations import ExpectationSuiteBuilder
>>> builder = ExpectationSuiteBuilder(spark, suite_name="orders_suite")
>>> suite = builder.from_schema(
...     schema=orders_schema,
...     not_null_keys=["order_id"],
...     unique_keys=["order_id"],
...     numeric_ranges={"amount": (0, 1_000_000)},
...     allowed_values={"status": ["PENDING", "SHIPPED", "DELIVERED", "CANCELLED"]},
... )
>>> result = builder.validate(df, suite)
>>> builder.assert_passed(result)

Usage — wire into DataLoader
-----------------------------
>>> from de_utils.expectations import ValidatingLoader
>>> vloader = ValidatingLoader(spark, database="silver", suite=suite)
>>> vloader.load(df, "orders", load_type=LoadType.PARTITION, partition_keys=["year","month"])
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

from .utils import get_logger, DataEngineeringError

if TYPE_CHECKING:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.types import StructType

log = get_logger(__name__)

# ── Check if great_expectations is available ─────────────────────────────────

def _gx_available() -> bool:
    try:
        import great_expectations  # noqa: F401
        return True
    except ImportError:
        return False


# ── Expectation definition (framework-agnostic) ───────────────────────────────

@dataclass
class Expectation:
    expectation_type: str
    kwargs: Dict[str, Any] = field(default_factory=dict)
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ExpectationSuite:
    suite_name: str
    expectations: List[Expectation] = field(default_factory=list)

    def add(self, exp: Expectation) -> "ExpectationSuite":
        self.expectations.append(exp)
        return self

    def to_ge_suite(self):
        """Convert to a great_expectations ExpectationSuite object."""
        import great_expectations as gx
        suite = gx.core.ExpectationSuite(expectation_suite_name=self.suite_name)
        for exp in self.expectations:
            suite.add_expectation(
                gx.core.ExpectationConfiguration(
                    expectation_type=exp.expectation_type,
                    kwargs=exp.kwargs,
                    meta=exp.meta,
                )
            )
        return suite

    def to_de_utils_rules(self):
        """Convert to de_utils Rule objects for fallback validation."""
        from .quality import Rule
        rules = []
        for exp in self.expectations:
            et = exp.expectation_type
            kw = exp.kwargs
            if et == "expect_column_values_to_not_be_null":
                rules.append(Rule.not_null(kw["column"]))
            elif et == "expect_column_values_to_be_unique":
                rules.append(Rule.unique(kw["column"]))
            elif et == "expect_column_values_to_be_between":
                rules.append(Rule.between(kw["column"], kw.get("min_value", 0),
                                           kw.get("max_value", float("inf"))))
            elif et == "expect_column_values_to_be_in_set":
                rules.append(Rule.allowed_values(kw["column"], kw.get("value_set", [])))
            elif et == "expect_column_values_to_match_regex":
                rules.append(Rule.regex(kw["column"], kw.get("regex", ".*")))
            elif et == "expect_table_row_count_to_be_between":
                if kw.get("min_value") is not None:
                    rules.append(Rule.row_count(">=", kw["min_value"]))
        return rules


# ── Suite Builder ─────────────────────────────────────────────────────────────

class ExpectationSuiteBuilder:
    """
    Builds ExpectationSuites from schemas and runs validation.

    Works with or without great_expectations installed.
    Falls back to de_utils.quality when GX is not available.
    """

    def __init__(self, spark: "SparkSession", suite_name: str = "de_utils_suite"):
        self.spark      = spark
        self.suite_name = suite_name
        self._gx        = _gx_available()
        if not self._gx:
            log.warning(
                "great_expectations not found — using de_utils.quality for validation. "
                "Install with: pip install great_expectations"
            )

    # ------------------------------------------------------------------
    # Suite construction
    # ------------------------------------------------------------------

    def from_schema(
        self,
        schema: "StructType",
        not_null_keys: Optional[List[str]] = None,
        unique_keys: Optional[List[str]] = None,
        numeric_ranges: Optional[Dict[str, Tuple[float, float]]] = None,
        allowed_values: Optional[Dict[str, List[Any]]] = None,
        regex_patterns: Optional[Dict[str, str]] = None,
        min_row_count: Optional[int] = None,
    ) -> ExpectationSuite:
        """
        Auto-generate an ExpectationSuite from a StructType schema plus
        additional semantic constraints.

        Example
        -------
        >>> suite = builder.from_schema(
        ...     schema=orders_schema,
        ...     not_null_keys=["order_id", "customer_id"],
        ...     unique_keys=["order_id"],
        ...     numeric_ranges={"amount": (0, 1_000_000)},
        ...     allowed_values={"status": ["PENDING","SHIPPED","DELIVERED","CANCELLED"]},
        ... )
        """
        suite = ExpectationSuite(suite_name=self.suite_name)

        # Type-based: all columns exist
        for f in schema.fields:
            suite.add(Expectation(
                "expect_column_to_exist",
                {"column": f.name},
            ))

        # Not null
        for col in (not_null_keys or []):
            suite.add(Expectation(
                "expect_column_values_to_not_be_null",
                {"column": col},
            ))

        # Unique
        for col in (unique_keys or []):
            suite.add(Expectation(
                "expect_column_values_to_be_unique",
                {"column": col},
            ))

        # Numeric ranges
        for col, (lo, hi) in (numeric_ranges or {}).items():
            suite.add(Expectation(
                "expect_column_values_to_be_between",
                {"column": col, "min_value": lo, "max_value": hi},
            ))

        # Allowed values
        for col, values in (allowed_values or {}).items():
            suite.add(Expectation(
                "expect_column_values_to_be_in_set",
                {"column": col, "value_set": values},
            ))

        # Regex patterns
        for col, pattern in (regex_patterns or {}).items():
            suite.add(Expectation(
                "expect_column_values_to_match_regex",
                {"column": col, "regex": pattern},
            ))

        # Row count
        if min_row_count is not None:
            suite.add(Expectation(
                "expect_table_row_count_to_be_between",
                {"min_value": min_row_count},
            ))

        return suite

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def validate(self, df: "DataFrame", suite: ExpectationSuite) -> Any:
        """
        Validate *df* against *suite*.
        Uses great_expectations if available, otherwise de_utils.quality.
        """
        if self._gx:
            return self._validate_gx(df, suite)
        return self._validate_fallback(df, suite)

    def assert_passed(self, result: Any) -> None:
        """Raise DataEngineeringError if validation did not pass."""
        if isinstance(result, dict):
            # Fallback result
            if not result.get("passed"):
                failed = result.get("failed_rules", [])
                raise DataEngineeringError(
                    f"Expectation suite '{self.suite_name}' failed: {failed}"
                )
        else:
            # GX result object
            if not result.success:
                raise DataEngineeringError(
                    f"Expectation suite '{self.suite_name}' failed."
                )

    def _validate_gx(self, df: "DataFrame", suite: ExpectationSuite) -> Any:
        import great_expectations as gx
        from great_expectations.dataset import SparkDFDataset

        ge_suite = suite.to_ge_suite()
        ge_df    = SparkDFDataset(df)
        results  = ge_df.validate(expectation_suite=ge_suite)
        log.info(
            "GX validation complete: %d/%d expectations passed",
            results.statistics["successful_expectations"],
            results.statistics["evaluated_expectations"],
        )
        return results

    def _validate_fallback(self, df: "DataFrame", suite: ExpectationSuite) -> Dict:
        from .quality import DataQualityChecker
        rules = suite.to_de_utils_rules()
        if not rules:
            return {"passed": True, "failed_rules": []}
        qc = DataQualityChecker(self.spark, table_name=self.suite_name)
        qc.add_rules(rules)
        report = qc.run(df)
        report.print_summary()
        return {
            "passed":       report.passed,
            "failed_rules": [r.rule.description for r in report.failures],
        }


# ── Validating Loader ─────────────────────────────────────────────────────────

class ValidatingLoader:
    """
    DataLoader wrapper that runs an ExpectationSuite before every load.
    Raises DataEngineeringError if validation fails (unless strict=False).

    Example
    -------
    >>> vloader = ValidatingLoader(spark, database="silver", suite=my_suite)
    >>> vloader.load(df, "orders", load_type=LoadType.PARTITION, partition_keys=["year","month"])
    """

    def __init__(
        self,
        spark: "SparkSession",
        database: str = "default",
        suite: Optional[ExpectationSuite] = None,
        strict: bool = True,
    ):
        from .loader import DataLoader
        self.spark   = spark
        self.loader  = DataLoader(spark, database)
        self.builder = ExpectationSuiteBuilder(spark)
        self.suite   = suite
        self.strict  = strict

    def load(self, df: "DataFrame", target_table: str, **loader_kwargs) -> Any:
        """Validate then load. Signature mirrors DataLoader.load()."""
        from .loader import LoadResult
        if self.suite:
            result = self.builder.validate(df, self.suite)
            try:
                self.builder.assert_passed(result)
            except DataEngineeringError:
                if self.strict:
                    raise
                log.warning("Validation failed but strict=False — proceeding with load.")
        return self.loader.load(df, target_table, **loader_kwargs)
