"""
tests/conftest.py
-----------------
Shared pytest fixtures for de_utils test suite.
Uses a local Spark session (no real Azure / Hive required).
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, IntegerType, DoubleType, TimestampType, BooleanType,
)


# ── Spark session ────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def spark():
    """Local SparkSession shared across the entire test session."""
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("de_utils_tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .enableHiveSupport()
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# ── Common schemas ───────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def orders_schema():
    return StructType([
        StructField("order_id",    LongType(),    nullable=False),
        StructField("customer_id", LongType(),    nullable=True),
        StructField("status",      StringType(),  nullable=True),
        StructField("amount",      DoubleType(),  nullable=True),
        StructField("year",        IntegerType(), nullable=True),
        StructField("month",       IntegerType(), nullable=True),
    ])


@pytest.fixture(scope="session")
def customer_schema():
    return StructType([
        StructField("customer_id", LongType(),   nullable=False),
        StructField("name",        StringType(), nullable=True),
        StructField("email",       StringType(), nullable=True),
        StructField("address",     StringType(), nullable=True),
    ])


# ── Common DataFrames ────────────────────────────────────────────────────────

@pytest.fixture()
def orders_df(spark, orders_schema):
    data = [
        (1, 100, "PENDING",  99.99,  2024, 1),
        (2, 101, "SHIPPED",  149.50, 2024, 1),
        (3, 102, "DELIVERED",200.00, 2024, 2),
        (4, 103, "PENDING",  50.00,  2024, 2),
        (5, 104, "CANCELLED",75.00,  2024, 3),
    ]
    return spark.createDataFrame(data, schema=orders_schema)


@pytest.fixture()
def customers_df(spark, customer_schema):
    data = [
        (1, "Alice",   "alice@example.com",   "123 Main St"),
        (2, "Bob",     "bob@example.com",     "456 Oak Ave"),
        (3, "Charlie", "charlie@example.com", "789 Pine Rd"),
    ]
    return spark.createDataFrame(data, schema=customer_schema)


@pytest.fixture()
def updated_customers_df(spark, customer_schema):
    """Bob's email changed, Dave is new, Alice unchanged."""
    data = [
        (1, "Alice",   "alice@example.com",      "123 Main St"),   # unchanged
        (2, "Bob",     "bob_new@example.com",     "456 Oak Ave"),   # email changed
        (4, "Dave",    "dave@example.com",        "999 Elm St"),    # new
    ]
    return spark.createDataFrame(data, schema=customer_schema)
