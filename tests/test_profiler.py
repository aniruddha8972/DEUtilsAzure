"""tests/test_profiler.py — Tests for DataProfiler."""
import pytest
from pyspark.sql.types import (
    StructType, StructField, LongType, StringType, DoubleType, IntegerType,
)
from de_utils.profiler import DataProfiler, DataProfile, ColumnProfile


@pytest.fixture()
def profiler(spark):
    return DataProfiler(spark, top_n=3)


@pytest.fixture()
def sample_df(spark):
    schema = StructType([
        StructField("order_id", LongType()),
        StructField("status",   StringType()),
        StructField("amount",   DoubleType()),
        StructField("year",     IntegerType()),
    ])
    return spark.createDataFrame(
        [
            (1,    "PENDING",   50.0,  2024),
            (2,    "SHIPPED",  150.0,  2024),
            (3,    "DELIVERED",200.0,  2024),
            (4,    "PENDING",   75.0,  2024),
            (5,    None,        99.9,  2023),  # null status
        ],
        schema=schema,
    )


@pytest.fixture()
def nulls_df(spark):
    schema = StructType([
        StructField("id",  LongType()),
        StructField("val", DoubleType()),
    ])
    return spark.createDataFrame([(1, None), (2, None), (3, 5.0)], schema=schema)


class TestProfileBasics:

    def test_returns_data_profile(self, profiler, sample_df):
        p = profiler.profile(sample_df, table_name="test")
        assert isinstance(p, DataProfile)

    def test_total_rows_correct(self, profiler, sample_df):
        p = profiler.profile(sample_df)
        assert p.total_rows == 5

    def test_total_columns_correct(self, profiler, sample_df):
        p = profiler.profile(sample_df)
        assert p.total_columns == 4

    def test_column_profiles_created_for_all_cols(self, profiler, sample_df):
        p = profiler.profile(sample_df)
        names = [c.name for c in p.columns]
        assert "order_id" in names
        assert "status"   in names
        assert "amount"   in names

    def test_subset_columns(self, profiler, sample_df):
        p = profiler.profile(sample_df, columns=["order_id", "amount"])
        assert p.total_columns == 2
        assert all(c.name in ["order_id", "amount"] for c in p.columns)

    def test_table_name_stored(self, profiler, sample_df):
        p = profiler.profile(sample_df, table_name="silver.orders")
        assert p.table_name == "silver.orders"


class TestColumnProfileNulls:

    def test_null_count_detected(self, profiler, sample_df):
        p  = profiler.profile(sample_df)
        cp = p.get_column("status")
        assert cp.null_count == 1

    def test_null_pct_calculated(self, profiler, sample_df):
        p  = profiler.profile(sample_df)
        cp = p.get_column("status")
        assert cp.null_pct == pytest.approx(0.2, abs=0.01)

    def test_no_nulls_complete(self, profiler, sample_df):
        p  = profiler.profile(sample_df)
        cp = p.get_column("order_id")
        assert cp.is_complete is True

    def test_columns_with_nulls_list(self, profiler, sample_df):
        p = profiler.profile(sample_df)
        assert "status" in p.columns_with_nulls

    def test_high_null_rate(self, profiler, nulls_df):
        p  = profiler.profile(nulls_df)
        cp = p.get_column("val")
        assert cp.null_pct == pytest.approx(2/3, abs=0.01)


class TestColumnProfileNumeric:

    def test_min_max_computed(self, profiler, sample_df):
        p  = profiler.profile(sample_df)
        cp = p.get_column("amount")
        assert cp.min_val == pytest.approx(50.0)
        assert cp.max_val == pytest.approx(200.0)

    def test_mean_computed(self, profiler, sample_df):
        p  = profiler.profile(sample_df)
        cp = p.get_column("amount")
        assert cp.mean_val is not None
        assert cp.mean_val > 0

    def test_percentiles_computed(self, profiler, sample_df):
        p  = profiler.profile(sample_df)
        cp = p.get_column("amount")
        assert cp.p50 is not None

    def test_non_numeric_has_no_mean(self, profiler, sample_df):
        p  = profiler.profile(sample_df)
        cp = p.get_column("status")
        assert cp.mean_val is None


class TestColumnProfileString:

    def test_string_lengths_computed(self, profiler, sample_df):
        p  = profiler.profile(sample_df)
        cp = p.get_column("status")
        assert cp.min_length is not None
        assert cp.max_length is not None
        assert cp.max_length >= cp.min_length

    def test_blank_count_detected(self, spark, profiler):
        from pyspark.sql.types import StructType, StructField, StringType
        schema = StructType([StructField("s", StringType())])
        df = spark.createDataFrame([("hello",), ("",), ("  ",), ("world",)], schema=schema)
        p  = profiler.profile(df)
        cp = p.get_column("s")
        assert cp.blank_count >= 2


class TestDistinct:

    def test_distinct_count(self, profiler, sample_df):
        p  = profiler.profile(sample_df)
        cp = p.get_column("order_id")
        assert cp.distinct_count == 5
        assert cp.is_unique is True

    def test_constant_column_detected(self, spark, profiler):
        from pyspark.sql.types import StructType, StructField, StringType
        schema = StructType([StructField("x", StringType())])
        df = spark.createDataFrame([("A",), ("A",), ("A",)], schema=schema)
        p  = profiler.profile(df)
        assert "x" in p.constant_columns


class TestTopValues:

    def test_top_values_populated(self, profiler, sample_df):
        p  = profiler.profile(sample_df)
        cp = p.get_column("status")
        assert len(cp.top_values) > 0
        assert "value" in cp.top_values[0]
        assert "count" in cp.top_values[0]


class TestReport:

    def test_completeness_pct_all_complete(self, profiler, sample_df):
        p  = profiler.profile(sample_df, columns=["order_id", "amount"])
        assert p.completeness_pct == 100.0

    def test_show_runs_without_error(self, profiler, sample_df, capsys):
        p = profiler.profile(sample_df, table_name="test_show")
        p.show()
        out = capsys.readouterr().out
        assert "test_show" in out

    def test_to_dict_structure(self, profiler, sample_df):
        p = profiler.profile(sample_df)
        d = p.to_dict()
        assert "table_name"    in d
        assert "total_rows"    in d
        assert "columns"       in d
        assert isinstance(d["columns"], list)

    def test_to_json_valid(self, profiler, sample_df):
        import json
        p    = profiler.profile(sample_df)
        data = json.loads(p.to_json())
        assert data["total_rows"] == 5


class TestOutlierDetection:

    def test_iqr_outliers_flagged(self, spark, profiler):
        from pyspark.sql.types import StructType, StructField, DoubleType
        schema = StructType([StructField("v", DoubleType())])
        # Most values clustered around 10, one obvious outlier at 1000
        data = [(10.0,), (10.5,), (9.8,), (10.2,), (1000.0,)]
        df   = spark.createDataFrame(data, schema=schema)
        result = profiler.detect_outliers(df, columns=["v"], method="iqr")
        assert "_is_outlier" in result.columns
        outliers = result.filter("`_is_outlier` = true").count()
        assert outliers >= 1

    def test_zscore_outliers_flagged(self, spark, profiler):
        from pyspark.sql.types import StructType, StructField, DoubleType
        schema = StructType([StructField("v", DoubleType())])
        data   = [(1.0,), (1.1,), (0.9,), (1.05,), (100.0,)]
        df     = spark.createDataFrame(data, schema=schema)
        result = profiler.detect_outliers(df, columns=["v"], method="zscore", threshold=2.0)
        assert result.filter("`_is_outlier` = true").count() >= 1

    def test_unknown_method_raises(self, spark, profiler):
        from de_utils.utils import DataEngineeringError
        from pyspark.sql.types import StructType, StructField, DoubleType
        schema = StructType([StructField("v", DoubleType())])
        df = spark.createDataFrame([(1.0,)], schema=schema)
        with pytest.raises(DataEngineeringError):
            profiler.detect_outliers(df, columns=["v"], method="invalid")


class TestDriftComparison:

    def test_compare_returns_dataframe(self, spark, profiler, sample_df):
        p_a = profiler.profile(sample_df, table_name="a")
        p_b = profiler.profile(sample_df, table_name="b")
        drift = profiler.compare(p_a, p_b)
        assert drift is not None
        assert "column" in drift.columns
        assert "_diff_type" in drift.columns or "status" in drift.columns
