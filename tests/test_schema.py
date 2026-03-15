"""
tests/test_schema.py
--------------------
Unit tests for SchemaManager — diff, align, cast, validate.
"""

import pytest
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, IntegerType, DoubleType,
)
from de_utils.schema import SchemaManager
from de_utils.utils import DataEngineeringError


@pytest.fixture()
def sm(spark):
    return SchemaManager(spark)


@pytest.fixture()
def schema_v1():
    return StructType([
        StructField("id",     LongType()),
        StructField("name",   StringType()),
        StructField("amount", DoubleType()),
    ])


@pytest.fixture()
def schema_v2():
    """v2 adds 'region', drops nothing, changes 'amount' to StringType."""
    return StructType([
        StructField("id",     LongType()),
        StructField("name",   StringType()),
        StructField("amount", StringType()),   # type changed
        StructField("region", StringType()),   # added
    ])


class TestDiffSchemas:

    def test_added_columns_detected(self, sm, schema_v1, schema_v2):
        diff = sm.diff_schemas(schema_v1, schema_v2)
        assert "region" in diff["added"]

    def test_dropped_columns_detected(self, sm, schema_v1, schema_v2):
        diff = sm.diff_schemas(schema_v2, schema_v1)
        assert "region" in diff["dropped"]

    def test_type_change_detected(self, sm, schema_v1, schema_v2):
        diff = sm.diff_schemas(schema_v1, schema_v2)
        assert "amount" in diff["type_changed"]

    def test_no_diff_identical_schemas(self, sm, schema_v1):
        diff = sm.diff_schemas(schema_v1, schema_v1)
        assert diff == {"added": [], "dropped": [], "type_changed": []}


class TestAssertSchemaCompatible:

    def test_compatible_schemas_pass(self, sm, spark, schema_v1):
        df = spark.createDataFrame([], schema_v1)
        sm.assert_schema_compatible(df, df)   # should not raise

    def test_incompatible_raises(self, sm, spark, schema_v1, schema_v2):
        src = spark.createDataFrame([], schema_v2)
        tgt = spark.createDataFrame([], schema_v1)
        # source has 'amount' as String, target as Double → type_changed
        with pytest.raises(DataEngineeringError):
            sm.assert_schema_compatible(src, tgt)

    def test_missing_columns_raise(self, sm, spark, schema_v1):
        small_schema = StructType([StructField("id", LongType())])
        src = spark.createDataFrame([], small_schema)
        tgt = spark.createDataFrame([], schema_v1)
        with pytest.raises(DataEngineeringError, match="missing required columns"):
            sm.assert_schema_compatible(src, tgt)


class TestAddMissingColumns:

    def test_adds_absent_column(self, sm, spark, schema_v1):
        partial = StructType([
            StructField("id",   LongType()),
            StructField("name", StringType()),
        ])
        df = spark.createDataFrame([(1, "Alice")], schema=partial)
        result = sm.add_missing_columns(df, schema_v1)
        assert "amount" in result.columns

    def test_null_fill_value(self, sm, spark, schema_v1):
        partial = StructType([StructField("id", LongType()), StructField("name", StringType())])
        df = spark.createDataFrame([(1, "X")], schema=partial)
        result = sm.add_missing_columns(df, schema_v1)
        row = result.collect()[0]
        assert row["amount"] is None

    def test_existing_columns_untouched(self, sm, spark, schema_v1):
        df = spark.createDataFrame([(1, "Alice", 99.9)], schema=schema_v1)
        result = sm.add_missing_columns(df, schema_v1)
        assert result.collect()[0]["amount"] == 99.9


class TestAlignToSchema:

    def test_reorders_columns(self, sm, spark, schema_v1):
        shuffled = StructType([
            StructField("amount", DoubleType()),
            StructField("name",   StringType()),
            StructField("id",     LongType()),
        ])
        df = spark.createDataFrame([(1.5, "X", 1)], schema=shuffled)
        result = sm.align_to_schema(df, schema_v1)
        assert result.columns == ["id", "name", "amount"]

    def test_drops_extra_columns(self, sm, spark, schema_v1):
        wide = StructType([
            StructField("id",     LongType()),
            StructField("name",   StringType()),
            StructField("amount", DoubleType()),
            StructField("extra",  StringType()),
        ])
        df = spark.createDataFrame([(1, "A", 1.0, "X")], schema=wide)
        result = sm.align_to_schema(df, schema_v1)
        assert "extra" not in result.columns


class TestCastColumns:

    def test_cast_applied(self, sm, spark):
        schema = StructType([
            StructField("val", StringType()),
        ])
        df = spark.createDataFrame([("42",)], schema=schema)
        result = sm.cast_columns(df, {"val": "integer"})
        assert result.schema["val"].dataType == IntegerType()


class TestValidateNotNull:

    def test_no_nulls_returns_true(self, sm, spark, schema_v1):
        df = spark.createDataFrame([(1, "A", 1.0)], schema=schema_v1)
        assert sm.validate_not_null(df, ["id", "name"]) is True

    def test_nulls_returns_false(self, sm, spark, schema_v1):
        df = spark.createDataFrame([(1, None, 1.0)], schema=schema_v1)
        assert sm.validate_not_null(df, ["name"]) is False


class TestValidateUnique:

    def test_unique_keys_returns_true(self, sm, spark, schema_v1):
        df = spark.createDataFrame([(1, "A", 1.0), (2, "B", 2.0)], schema=schema_v1)
        assert sm.validate_unique(df, ["id"]) is True

    def test_duplicate_keys_returns_false(self, sm, spark, schema_v1):
        df = spark.createDataFrame([(1, "A", 1.0), (1, "B", 2.0)], schema=schema_v1)
        assert sm.validate_unique(df, ["id"]) is False
