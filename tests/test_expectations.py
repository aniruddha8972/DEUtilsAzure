"""tests/test_expectations.py"""
import pytest
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType
from de_utils_v2.expectations import (
    Expectation, ExpectationSuite, ExpectationSuiteBuilder, ValidatingLoader
)
from de_utils_v2.utils import DataEngineeringError

DB = "test_expectations"

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
    ])

@pytest.fixture()
def good_df(spark, schema):
    return spark.createDataFrame(
        [(1,"Alice",100.0,"ACTIVE"),(2,"Bob",200.0,"CLOSED")],
        schema=schema)

@pytest.fixture()
def bad_df(spark, schema):
    return spark.createDataFrame(
        [(1,None,-5.0,"UNKNOWN")],
        schema=schema)

@pytest.fixture()
def builder(spark):
    return ExpectationSuiteBuilder(spark, suite_name="test_suite")

class TestExpectationSuite:
    def test_add_expectation(self):
        suite = ExpectationSuite("my_suite")
        exp = Expectation("expect_column_to_exist", {"column": "id"})
        suite.add(exp)
        assert len(suite.expectations) == 1

    def test_to_de_utils_rules_not_null(self):
        suite = ExpectationSuite("s")
        suite.add(Expectation("expect_column_values_to_not_be_null", {"column": "id"}))
        rules = suite.to_de_utils_rules()
        assert len(rules) == 1
        assert rules[0].columns == ["id"]

    def test_to_de_utils_rules_unique(self):
        suite = ExpectationSuite("s")
        suite.add(Expectation("expect_column_values_to_be_unique", {"column": "id"}))
        rules = suite.to_de_utils_rules()
        assert len(rules) == 1

    def test_to_de_utils_rules_between(self):
        suite = ExpectationSuite("s")
        suite.add(Expectation("expect_column_values_to_be_between",
                              {"column":"amount","min_value":0,"max_value":1000}))
        rules = suite.to_de_utils_rules()
        assert rules[0].params["min"] == 0

    def test_to_de_utils_rules_allowed_values(self):
        suite = ExpectationSuite("s")
        suite.add(Expectation("expect_column_values_to_be_in_set",
                              {"column":"status","value_set":["ACTIVE","CLOSED"]}))
        rules = suite.to_de_utils_rules()
        assert "ACTIVE" in rules[0].params["values"]

    def test_to_de_utils_rules_row_count(self):
        suite = ExpectationSuite("s")
        suite.add(Expectation("expect_table_row_count_to_be_between", {"min_value": 1}))
        rules = suite.to_de_utils_rules()
        assert len(rules) == 1

class TestExpectationSuiteBuilder:
    def test_from_schema_creates_suite(self, builder, schema):
        suite = builder.from_schema(schema)
        assert isinstance(suite, ExpectationSuite)
        # Should have one expect_column_to_exist per field
        col_exists = [e for e in suite.expectations
                      if e.expectation_type == "expect_column_to_exist"]
        assert len(col_exists) == 4

    def test_from_schema_not_null(self, builder, schema):
        suite = builder.from_schema(schema, not_null_keys=["id"])
        not_null = [e for e in suite.expectations
                    if e.expectation_type == "expect_column_values_to_not_be_null"]
        assert len(not_null) == 1

    def test_from_schema_unique(self, builder, schema):
        suite = builder.from_schema(schema, unique_keys=["id"])
        unique = [e for e in suite.expectations
                  if e.expectation_type == "expect_column_values_to_be_unique"]
        assert len(unique) == 1

    def test_from_schema_numeric_range(self, builder, schema):
        suite = builder.from_schema(schema, numeric_ranges={"amount": (0, 9999)})
        between = [e for e in suite.expectations
                   if e.expectation_type == "expect_column_values_to_be_between"]
        assert len(between) == 1

    def test_from_schema_allowed_values(self, builder, schema):
        suite = builder.from_schema(schema, allowed_values={"status": ["ACTIVE","CLOSED"]})
        av = [e for e in suite.expectations
              if e.expectation_type == "expect_column_values_to_be_in_set"]
        assert len(av) == 1

    def test_validate_passes_on_clean(self, builder, schema, good_df):
        suite = builder.from_schema(schema, not_null_keys=["id"],
                                    allowed_values={"status":["ACTIVE","CLOSED"]})
        result = builder.validate(good_df, suite)
        builder.assert_passed(result)   # should not raise

    def test_validate_fails_on_bad(self, builder, schema, bad_df):
        suite = builder.from_schema(schema, not_null_keys=["name"],
                                    allowed_values={"status":["ACTIVE","CLOSED"]})
        result = builder.validate(bad_df, suite)
        with pytest.raises(DataEngineeringError):
            builder.assert_passed(result)

class TestValidatingLoader:
    def test_load_with_passing_suite(self, spark, schema, good_df):
        from de_utils_v2.loader import LoadType
        builder = ExpectationSuiteBuilder(spark)
        suite = builder.from_schema(schema, not_null_keys=["id"], row_count_min=1)
        # row_count_min not a param — just test it doesn't crash
        suite2 = builder.from_schema(schema, not_null_keys=["id"])
        vl = ValidatingLoader(spark, database=DB, suite=suite2)
        spark.sql(f"DROP TABLE IF EXISTS `{DB}`.`vl_orders`")
        good_df.write.mode("overwrite").saveAsTable(f"`{DB}`.`vl_orders`")
        vl.load(good_df, "vl_orders", load_type=LoadType.FULL)

    def test_load_raises_on_failed_strict(self, spark, schema, bad_df):
        from de_utils_v2.loader import LoadType
        b = ExpectationSuiteBuilder(spark)
        suite = b.from_schema(schema, not_null_keys=["name"])
        vl = ValidatingLoader(spark, database=DB, suite=suite, strict=True)
        with pytest.raises(DataEngineeringError):
            vl.load(bad_df, "vl_strict", load_type=LoadType.FULL)

    def test_load_continues_on_failed_non_strict(self, spark, schema, bad_df):
        from de_utils_v2.loader import LoadType
        b = ExpectationSuiteBuilder(spark)
        suite = b.from_schema(schema, not_null_keys=["name"])
        vl = ValidatingLoader(spark, database=DB, suite=suite, strict=False)
        spark.sql(f"DROP TABLE IF EXISTS `{DB}`.`vl_nonstrict`")
        bad_df.write.mode("overwrite").saveAsTable(f"`{DB}`.`vl_nonstrict`")
        vl.load(bad_df, "vl_nonstrict", load_type=LoadType.FULL)  # should not raise

    def test_load_without_suite_skips_validation(self, spark, good_df):
        from de_utils_v2.loader import LoadType
        spark.sql(f"DROP TABLE IF EXISTS `{DB}`.`vl_nosuite`")
        good_df.write.mode("overwrite").saveAsTable(f"`{DB}`.`vl_nosuite`")
        vl = ValidatingLoader(spark, database=DB, suite=None)
        vl.load(good_df, "vl_nosuite", load_type=LoadType.FULL)  # no error
