"""tests/test_quality.py — Data Quality Checker tests"""
import pytest
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType
from de_utils_v2.quality import DataQualityChecker, Rule, DQReport, RuleType
from de_utils_v2.utils import DataEngineeringError

DB = "test_quality"

@pytest.fixture(autouse=True, scope="module")
def setup_db(spark):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{DB}`")
    yield
    spark.sql(f"DROP DATABASE IF EXISTS `{DB}` CASCADE")

@pytest.fixture()
def schema():
    return StructType([
        StructField("id",     LongType()),
        StructField("name",   StringType()),
        StructField("amount", DoubleType()),
        StructField("status", StringType()),
        StructField("email",  StringType()),
    ])

@pytest.fixture()
def good_df(spark, schema):
    return spark.createDataFrame(
        [(1,"Alice",100.0,"ACTIVE","a@a.com"),
         (2,"Bob",  200.0,"ACTIVE","b@b.com"),
         (3,"Carol",300.0,"CLOSED","c@c.com")],
        schema=schema)

@pytest.fixture()
def bad_df(spark, schema):
    return spark.createDataFrame(
        [(1, None, -5.0,   "ACTIVE", "not-an-email"),
         (1, "Bob", 200.0, "UNKNOWN","b@b.com"),   # dup id, bad status
         (3, "Carol",300.0,"CLOSED","c@c.com")],
        schema=schema)

@pytest.fixture()
def qc(spark):
    return DataQualityChecker(spark, table_name="test_table")

class TestRuleFactories:
    def test_not_null_rule(self):
        r = Rule.not_null("id")
        assert r.rule_type == RuleType.NOT_NULL
        assert r.columns == ["id"]

    def test_unique_rule(self):
        r = Rule.unique("id", "name")
        assert r.rule_type == RuleType.UNIQUE
        assert len(r.columns) == 2

    def test_between_rule(self):
        r = Rule.between("amount", 0, 1000)
        assert r.params["min"] == 0
        assert r.params["max"] == 1000

    def test_allowed_values_rule(self):
        r = Rule.allowed_values("status", ["ACTIVE","CLOSED"])
        assert "ACTIVE" in r.params["values"]

    def test_regex_rule(self):
        r = Rule.regex("email", r"^[^@]+@[^@]+\.[^@]+$")
        assert "pattern" in r.params

    def test_row_count_rule(self):
        r = Rule.row_count(">=", 1)
        assert r.params["operator"] == ">="
        assert r.params["threshold"] == 1

    def test_custom_sql_rule(self):
        r = Rule.custom_sql("SELECT * FROM __dq_df__ WHERE amount < 0")
        assert r.rule_type == RuleType.CUSTOM_SQL

    def test_min_value_rule(self):
        r = Rule.min_value("amount", 0.0)
        assert r.params["min"] == 0.0

    def test_max_value_rule(self):
        r = Rule.max_value("amount", 9999.0)
        assert r.params["max"] == 9999.0

class TestNotNull:
    def test_passes_on_clean_data(self, qc, good_df):
        qc.add_rule(Rule.not_null("id"))
        report = qc.run(good_df)
        assert report.results[0].passed

    def test_fails_on_null(self, qc, bad_df):
        qc._rules.clear()
        qc.add_rule(Rule.not_null("name"))
        report = qc.run(bad_df)
        assert not report.results[0].passed
        assert report.results[0].failing_count == 1

class TestUnique:
    def test_passes_on_unique(self, qc, good_df):
        qc._rules.clear()
        qc.add_rule(Rule.unique("id"))
        assert qc.run(good_df).results[0].passed

    def test_fails_on_duplicates(self, qc, bad_df):
        qc._rules.clear()
        qc.add_rule(Rule.unique("id"))
        assert not qc.run(bad_df).results[0].passed

class TestBetween:
    def test_passes_in_range(self, qc, good_df):
        qc._rules.clear()
        qc.add_rule(Rule.between("amount", 0, 1000))
        assert qc.run(good_df).results[0].passed

    def test_fails_out_of_range(self, qc, bad_df):
        qc._rules.clear()
        qc.add_rule(Rule.between("amount", 0, 1000))
        assert not qc.run(bad_df).results[0].passed

class TestAllowedValues:
    def test_passes_with_allowed(self, qc, good_df):
        qc._rules.clear()
        qc.add_rule(Rule.allowed_values("status", ["ACTIVE","CLOSED"]))
        assert qc.run(good_df).results[0].passed

    def test_fails_with_disallowed(self, qc, bad_df):
        qc._rules.clear()
        qc.add_rule(Rule.allowed_values("status", ["ACTIVE","CLOSED"]))
        assert not qc.run(bad_df).results[0].passed

class TestRowCount:
    def test_passes_above_min(self, qc, good_df):
        qc._rules.clear()
        qc.add_rule(Rule.row_count(">=", 1))
        assert qc.run(good_df).results[0].passed

    def test_fails_below_min(self, qc, spark, schema):
        qc._rules.clear()
        qc.add_rule(Rule.row_count(">=", 1000))
        empty = spark.createDataFrame([], schema=schema)
        assert not qc.run(empty).results[0].passed

class TestReferential:
    def test_passes_with_valid_refs(self, spark, schema):
        ref_schema = StructType([StructField("valid_id", LongType())])
        ref_df = spark.createDataFrame([(1,),(2,),(3,)], schema=ref_schema)
        qc = DataQualityChecker(spark)
        qc.add_rule(Rule.referential("id", ref_df, "valid_id"))
        good = spark.createDataFrame([(1,"A",1.0,"ACTIVE","a@a.com")], schema=schema)
        assert qc.run(good).results[0].passed

    def test_fails_with_missing_refs(self, spark, schema):
        ref_schema = StructType([StructField("valid_id", LongType())])
        ref_df = spark.createDataFrame([(99,)], schema=ref_schema)
        qc = DataQualityChecker(spark)
        qc.add_rule(Rule.referential("id", ref_df, "valid_id"))
        df = spark.createDataFrame([(1,"A",1.0,"ACTIVE","a@a.com")], schema=schema)
        assert not qc.run(df).results[0].passed

class TestDQReport:
    def test_assert_no_failures_passes(self, qc, good_df):
        qc._rules.clear()
        qc.add_rule(Rule.not_null("id"))
        report = qc.run(good_df)
        report.assert_no_failures()  # should not raise

    def test_assert_no_failures_raises(self, qc, bad_df):
        qc._rules.clear()
        qc.add_rule(Rule.not_null("name"))
        report = qc.run(bad_df)
        with pytest.raises(DataEngineeringError):
            report.assert_no_failures()

    def test_warning_severity_does_not_raise(self, spark, schema):
        qc = DataQualityChecker(spark)
        qc.add_rule(Rule.not_null("name", severity="WARNING"))
        df = spark.createDataFrame([(1,None,1.0,"X","x@x.com")], schema=schema)
        report = qc.run(df)
        report.assert_no_failures()  # WARNING should not raise

    def test_save_writes_to_table(self, spark, good_df):
        spark.sql(f"DROP TABLE IF EXISTS `{DB}`.`dq_log`")
        qc = DataQualityChecker(spark, table_name="orders")
        qc.add_rule(Rule.not_null("id"))
        report = qc.run(good_df)
        report.save(spark, f"`{DB}`.`dq_log`")
        assert spark.table(f"`{DB}`.`dq_log`").count() >= 1

    def test_pass_rate_is_one_on_clean(self, qc, good_df):
        qc._rules.clear()
        qc.add_rule(Rule.not_null("id"))
        report = qc.run(good_df)
        assert report.results[0].pass_rate == 1.0

    def test_failures_property(self, qc, bad_df):
        qc._rules.clear()
        qc.add_rule(Rule.not_null("name"))
        report = qc.run(bad_df)
        assert len(report.failures) == 1

    def test_add_rules_chaining(self, spark):
        qc = DataQualityChecker(spark)
        result = qc.add_rule(Rule.row_count(">=",0)).add_rule(Rule.row_count(">=",0))
        assert result is qc
        assert len(qc._rules) == 2
