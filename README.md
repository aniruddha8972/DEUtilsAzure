# de_utils — Data Engineering Utility Library

> A batteries-included Python library for Azure ADLS Gen2, Hive Metastore, and Spark ETL patterns.

---

## Installation

```bash
pip install de_utils               # core (requires pyspark)
pip install de_utils[azure]        # + Azure SDK for file-system operations
pip install de_utils[azure,dev]    # + dev/test tooling
```

---

## 1. Configure ADLS & Spark

```python
from de_utils import ADLSConfig, HiveConfig, SparkConfig

adls_cfg = ADLSConfig(
    account_name="myadlsaccount",
    container="raw",
    # Service Principal auth (recommended)
    client_id="<app-client-id>",
    client_secret="<app-secret>",
    tenant_id="<aad-tenant-id>",
    root_path="/data",
)

hive_cfg = HiveConfig(
    metastore_uri="thrift://hive-metastore.example.com:9083",
    database="silver",
    warehouse_path=adls_cfg.path("warehouse"),
)

spark_cfg = SparkConfig(
    app_name="MyETLJob",
    adls=adls_cfg,
    hive=hive_cfg,
)

spark = spark_cfg.build_session()
```

---

## 2. ADLS Connector — read & write files

```python
from de_utils import ADLSConnector

conn = ADLSConnector(adls_cfg, spark)

# Read
raw_df   = conn.read_parquet("ingest/orders/2024/")
delta_df = conn.read_delta("ingest/events/", version=5)
csv_df   = conn.read_csv("landing/customers.csv")

# Write
conn.write_parquet(raw_df, "staging/orders/", partition_by=["year", "month"])
conn.write_delta(raw_df, "silver/orders/", mode="append")

# File-system helpers (requires azure-storage-file-datalake)
conn.create_directory("archive/2024/")
paths = conn.list_paths("staging/", recursive=True)
exists = conn.path_exists("staging/orders/year=2024/")
conn.delete_path("tmp/scratch/")
```

---

## 3. Hive Metastore

```python
from de_utils import HiveMetastore, HiveConfig
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType

meta = HiveMetastore(HiveConfig(database="silver"), spark)

# Create database
meta.create_database_if_not_exists(location=adls_cfg.path("silver"))

# Register an external table
schema = StructType([
    StructField("order_id",   LongType()),
    StructField("customer_id",LongType()),
    StructField("status",     StringType()),
    StructField("year",       IntegerType()),
    StructField("month",      IntegerType()),
])
meta.create_external_table(
    table_name="orders",
    schema=schema,
    location=adls_cfg.path("silver/orders"),
    partition_cols=["year", "month"],
    file_format="PARQUET",
    comment="Silver-layer orders table",
)

# Partition management
meta.add_partition("orders", {"year": "2024", "month": "06"})
meta.repair_table("orders")                 # MSCK REPAIR
meta.list_partitions("orders")
meta.drop_partition("orders", {"year": "2020", "month": "01"})

# Inspect
meta.list_tables()
meta.describe_table("orders").show()
meta.set_table_property("orders", "de_utils.owner", "data-team@example.com")
```

---

## 4. Schema Manager

```python
from de_utils import SchemaManager

sm = SchemaManager(spark)

# Diff two schemas
diff = sm.diff_schemas(old_schema, new_schema)
# {"added": [...], "dropped": [...], "type_changed": [...]}

sm.print_schema_diff(old_schema, new_schema)

# Evolve a DataFrame to match a target schema (add nulls for new cols, reorder)
aligned_df = sm.align_to_schema(source_df, target_schema)

# Cast specific columns
df = sm.cast_columns(df, {"amount": "double", "event_date": "date"})

# Validate
sm.assert_schema_compatible(source_df, target_df)
sm.validate_not_null(df, ["order_id", "customer_id"])
sm.validate_unique(df, ["order_id"])
```

---

## 5. Table Operations — CREATE / INSERT / UPDATE / MERGE

```python
from de_utils import TableOperations

ops = TableOperations(spark, database="silver")

# CREATE
ops.create_or_replace("orders", df, partition_by=["year", "month"])
ops.create_if_not_exists("customers", dim_df)

# INSERT
ops.insert_append("orders", new_rows_df)
ops.insert_overwrite("orders", df, partition_by=["year", "month"])  # dynamic partition

# UPDATE / DELETE (Delta)
ops.update("orders", condition="status = 'PENDING'", set_clause={"status": "'SHIPPED'"})
ops.delete("orders", condition="created_date < '2020-01-01'")

# MERGE / UPSERT
ops.merge(
    target_table="orders",
    source_df=updates_df,
    match_keys=["order_id"],
    update_columns=["status", "updated_at"],
    insert_when_not_matched=True,
    delete_when_not_matched_by_source=False,
)
ops.upsert("orders", cdc_df, match_keys=["order_id"])  # alias for merge
```

---

## 6. Data Loader — load strategies

```python
from de_utils import DataLoader
from de_utils.loader import LoadType

loader = DataLoader(spark, database="silver")

# ── Full load (truncate & reload) ───────────────────────────────────────
result = loader.full_load(raw_df, "orders", partition_by=["year", "month"])

# ── Incremental (watermark-based append) ────────────────────────────────
result = loader.incremental_load(
    daily_df, "orders",
    watermark_column="updated_at",
    last_watermark="2024-05-31 23:59:59",
)

# ── Partition overwrite (only touch the supplied partition values) ───────
result = loader.partition_load(
    df_june, "orders",
    partition_keys=["year", "month"],
)

# ── Merge / CDC upsert ───────────────────────────────────────────────────
result = loader.merge_load(cdc_df, "orders", match_keys=["order_id"])

# ── Snapshot (append with snapshot_date column) ─────────────────────────
result = loader.load(
    df, "orders_snapshot",
    load_type=LoadType.SNAPSHOT,
    snapshot_date="2024-06-30",
)

print(result)
# LoadResult(table='silver.orders', type=FULL, elapsed=4.2s, completed_at=...)
```

---

## 7. History Table

```python
from de_utils import HistoryTable

hist = HistoryTable(spark, database="gold")

# First run — creates orders_hist table
hist.init_history_table(
    base_table="orders",
    source_df=df,
    key_columns=["order_id"],
    tracked_columns=["status", "amount", "address"],
    file_format="PARQUET",
    location=adls_cfg.path("gold/orders_hist"),
)

# Subsequent runs — capture changes
result = hist.capture(
    source_df=daily_df,
    base_table="orders",
    key_columns=["order_id"],
)
print(result)
# HistoryCaptureResult(table='orders_hist', inserts=120, updates=45, deletes=3, ...)

# Point-in-time query
snapshot = hist.get_version_at("orders", as_of="2024-03-01 00:00:00")

# Full audit trail
all_versions = hist.get_full_history("orders")
```

---

## 8. SCD Functions

### SCD Type 1 — Overwrite
```python
from de_utils import SCDHandler

scd = SCDHandler(spark, database="gold")

scd.apply_scd1(
    target_table="dim_customer",
    source_df=staging_df,
    key_columns=["customer_id"],
    update_columns=["email", "phone", "address"],
)
```

### SCD Type 2 — Full History
```python
result = scd.apply_scd2(
    target_table="dim_customer",
    source_df=staging_df,
    key_columns=["customer_id"],
    tracked_columns=["email", "address"],
    effective_date="2024-06-01 00:00:00",   # optional, defaults to now
)
# SCD2Result(table='dim_customer', new_inserts=30, updates=12, effective_date='2024-06-01 ...')
```

### SCD Type 3 — Previous-Value Column
```python
# Target table must have: email, prev_email, address, prev_address
scd.apply_scd3(
    target_table="dim_customer",
    source_df=staging_df,
    key_columns=["customer_id"],
    tracked_columns=["email", "address"],
)
```

### SCD Type 4 — Mini-Dimension (separate history table)
```python
scd.apply_scd4(
    target_table="dim_customer",
    history_table="dim_customer_hist",
    source_df=staging_df,
    key_columns=["customer_id"],
    tracked_columns=["email", "address"],
)
```

### SCD Type 6 — Hybrid (1+2+3)
```python
# Target must have: current columns + prev_ columns + effective_from/to + is_current
scd.apply_scd6(
    target_table="dim_customer",
    source_df=staging_df,
    key_columns=["customer_id"],
    tracked_columns=["email", "address"],
)
```

### Diff utility
```python
diff_df = scd.diff(
    df_a=current_df,
    df_b=incoming_df,
    key_columns=["customer_id"],
    compare_columns=["email", "address"],
)
diff_df.show()
# +------------+-----------+
# |customer_id |_diff_type |
# +------------+-----------+
# |1001        |UNCHANGED  |
# |1002        |CHANGED    |
# |1099        |NEW        |
# |2050        |DELETED    |
# +------------+-----------+
```

---

## Quick-start end-to-end example

```python
from de_utils import (
    ADLSConfig, HiveConfig, SparkConfig,
    ADLSConnector, HiveMetastore,
    DataLoader, HistoryTable, SCDHandler,
)
from de_utils.loader import LoadType

# 1. Build Spark session with ADLS + Hive
spark = SparkConfig(
    app_name="DailyOrdersETL",
    adls=ADLSConfig("myadls", "raw", client_id="...", client_secret="...", tenant_id="..."),
    hive=HiveConfig(metastore_uri="thrift://meta:9083", database="silver"),
).build_session()

# 2. Read from ADLS
conn   = ADLSConnector(ADLSConfig("myadls", "raw"), spark)
raw_df = conn.read_parquet("ingest/orders/2024/06/")

# 3. Partition load to silver
loader = DataLoader(spark, database="silver")
loader.partition_load(raw_df, "orders", partition_keys=["year", "month"])

# 4. Capture history
hist = HistoryTable(spark, database="gold")
hist.capture(raw_df, "orders", key_columns=["order_id"])

# 5. Upsert to gold dimension
scd = SCDHandler(spark, database="gold")
scd.apply_scd2(
    target_table="dim_orders",
    source_df=raw_df,
    key_columns=["order_id"],
    tracked_columns=["status", "total_amount"],
)
```
