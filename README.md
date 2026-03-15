# de_utils v2.1 — Data Engineering Utility Library

> A batteries-included Python library for Azure ADLS Gen2, Hive Metastore, Spark ETL,
> Data Quality, Data Profiling, Lineage Tracking, Delta Lake, Job Checkpointing,
> Medallion Architecture, Alerting, Secrets Management, Retry/Resilience,
> and Pipeline Orchestration.

---

## Table of Contents

1. [Installation](#installation)
2. [What's in v2.1](#whats-in-v21)
3. [Module Overview](#module-overview)
4. [Configuration](#1-configuration)
5. [ADLS Connector](#2-adls-connector)
6. [Hive Metastore](#3-hive-metastore)
7. [Schema Manager](#4-schema-manager)
8. [Table Operations](#5-table-operations)
9. [Data Loader](#6-data-loader)
10. [History Table](#7-history-table)
11. [SCD Functions](#8-scd-functions)
12. [Data Quality](#9-data-quality)
13. [Data Profiler](#10-data-profiler)
14. [Lineage Tracking](#11-lineage-tracking)
15. [Job Checkpointing](#12-job-checkpointing)
16. [Delta Lake Utilities](#13-delta-lake-utilities)
17. [Audit Log](#14-audit-log)
18. [Medallion Architecture](#15-medallion-architecture)
19. [Great Expectations Integration](#16-great-expectations-integration)
20. [Alerting](#17-alerting)
21. [Secrets Management](#18-secrets-management)
22. [Retry & Resilience](#19-retry--resilience)
23. [Pipeline Orchestration](#20-pipeline-orchestration)
24. [CLI Reference](#21-cli-reference)
25. [End-to-End Example](#end-to-end-example)
26. [Running Tests](#running-tests)

---

## Installation

```bash
# Core (requires PySpark)
pip install de_utils

# + Azure Data Lake Storage SDK
pip install "de_utils[azure]"

# + Azure Key Vault secrets
pip install "de_utils[keyvault]"

# + YAML config support
pip install "de_utils[yaml]"

# + Delta Lake utilities
pip install "de_utils[delta]"

# + Great Expectations integration
pip install "de_utils[gx]"

# Everything
pip install "de_utils[all]"

# Development + testing
pip install "de_utils[dev]"
```

---

## What's in v2.1

| Module | Feature | Description |
|---|---|---|
| `config.py` | Configuration | `ADLSConfig`, `HiveConfig`, `SparkConfig` dataclasses |
| `config_loader.py` | YAML / Env Config | Load configs from YAML files or env vars — no hardcoded credentials |
| `storage.py` | ADLS Connector | Read/write Parquet, Delta, CSV, JSON via `abfss://` |
| `metastore.py` | Hive Metastore | DDL, partitions, table properties |
| `schema.py` | Schema Manager | Diff, align, cast, validate schemas |
| `table_ops.py` | Table Operations | CREATE, INSERT, UPDATE, DELETE, MERGE |
| `loader.py` | Data Loader | FULL, INCREMENTAL, PARTITION, MERGE, SNAPSHOT load strategies |
| `history.py` | History Table | Row-level change capture + point-in-time queries |
| `scd.py` | SCD Functions | SCD1, SCD2, SCD3, SCD4, SCD6 + `diff()` |
| `quality.py` | Data Quality | Declarative rule engine — null, unique, range, regex, referential, custom SQL |
| `profiler.py` | **Data Profiler** | Auto statistical summaries, distributions, outlier detection, drift comparison |
| `lineage.py` | Lineage Tracking | Auto-capture source→target lineage per run, queryable graph |
| `checkpoint.py` | Job Checkpointing | Persistent watermarks + partition-done state for restartable ETL |
| `delta.py` | Delta Lake Utils | OPTIMIZE, VACUUM, Z-ORDER, time travel, clone, restore, CDF |
| `audit.py` | Audit Log | Central structured log — timing, row counts, errors, context manager |
| `medallion.py` | Medallion Arch | `BronzeLoader`, `SilverLoader`, `GoldLoader` with per-layer defaults |
| `expectations.py` | GX Integration | Auto-generate Great Expectations suites from schemas |
| `alerts.py` | **Alerting** | Teams, Slack, Email, PagerDuty — fan-out router + `@alert_on_failure` |
| `secrets.py` | **Secrets Mgmt** | Azure Key Vault, env vars, chain resolution, `${SECRET:name}` placeholders |
| `retry.py` | **Retry / Resilience** | `@retry` decorator, `RetryPolicy`, `CircuitBreaker` with back-off + jitter |
| `pipeline.py` | **Pipeline Orchestration** | DAG runner — topological sort, parallel execution, retries, dry-run |
| `cli.py` | **CLI** | `de_utils` command — profile, audit, lineage, delta, checkpoint, pipeline |

---

## Module Overview

```
de_utils/
├── config.py          ADLSConfig, HiveConfig, SparkConfig dataclasses
├── config_loader.py   Load configs from YAML files or environment variables
├── storage.py         ADLSConnector — read/write Parquet, Delta, CSV, JSON
├── metastore.py       HiveMetastore — DDL, partitions, table properties
├── schema.py          SchemaManager — diff, align, cast, validate
├── table_ops.py       TableOperations — CREATE, INSERT, UPDATE, DELETE, MERGE
├── loader.py          DataLoader — FULL, INCREMENTAL, PARTITION, MERGE, SNAPSHOT
├── history.py         HistoryTable — row-level change capture + point-in-time
├── scd.py             SCDHandler — SCD1, SCD2, SCD3, SCD4, SCD6 + diff()
├── quality.py         DataQualityChecker — declarative rule engine + DQReport
├── profiler.py        DataProfiler — stats, distributions, outliers, drift
├── lineage.py         LineageTracker — source→target lineage capture
├── checkpoint.py      JobCheckpoint — watermarks + idempotent run detection
├── delta.py           DeltaUtils — optimize, vacuum, z-order, time travel
├── audit.py           AuditLog — structured operation log with context manager
├── medallion.py       BronzeLoader, SilverLoader, GoldLoader
├── expectations.py    ExpectationSuiteBuilder, ValidatingLoader (GX integration)
├── alerts.py          AlertRouter, TeamsAlerter, SlackAlerter, EmailAlerter, PagerDutyAlerter
├── secrets.py         SecretsChain, AzureKeyVaultSecrets, EnvSecrets
├── retry.py           @retry, RetryPolicy, CircuitBreaker, with_retry
├── pipeline.py        Pipeline, task decorator, DAG execution, PipelineResult
├── cli.py             de_utils CLI entry point
└── utils.py           Shared helpers, logger, DataEngineeringError

tests/
├── conftest.py                    Shared Spark session + fixtures
├── test_config.py                 ADLSConfig, HiveConfig, SparkConfig (15 tests)
├── test_config_loader.py          YAML/env loading (18 tests)
├── test_utils.py                  Helpers + exceptions (16 tests)
├── test_schema.py                 Schema diff, align, cast, validate (17 tests)
├── test_table_ops.py              CREATE, INSERT, MERGE (11 tests)
├── test_loader.py                 All 5 load strategies (13 tests)
├── test_history.py                Init, capture, point-in-time (14 tests)
├── test_scd.py                    SCD1/2/3/6 + diff (15 tests)
├── test_quality.py                All DQ rule types + report (28 tests)
├── test_profiler.py               Stats, nulls, outliers, drift (34 tests)
├── test_lineage.py                Record, context manager, query (7 tests)
├── test_checkpoint.py             Watermarks, partition-done (11 tests)
├── test_delta.py                  Optimize, vacuum, history (11 tests)
├── test_audit.py                  Log, context manager, queries (11 tests)
├── test_medallion.py              Bronze/Silver/Gold loaders (16 tests)
├── test_expectations.py           Suite from schema, validate (17 tests)
├── test_storage.py                Read/write + mocked SDK (12 tests)
├── test_metastore.py              Database, DDL, partitions (11 tests)
├── test_alerts.py                 Router, channels, decorator (18 tests)
├── test_retry.py                  Retry, back-off, CircuitBreaker (22 tests)
└── test_secrets_and_pipeline.py   Secrets + pipeline DAG (44 tests)
                                   ─────────────────────────
                                   343 tests total
```

---

## 1. Configuration

### From code

```python
from de_utils import ADLSConfig, HiveConfig, SparkConfig

adls_cfg = ADLSConfig(
    account_name="myadlsaccount",
    container="raw",
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

spark = SparkConfig(
    app_name="DailyOrdersETL",
    adls=adls_cfg,
    hive=hive_cfg,
).build_session()
```

### From YAML

```yaml
# pipeline.yml
adls:
  account_name: myadlsaccount
  container: raw
  client_id: ${AZURE_CLIENT_ID}         # env var interpolation
  client_secret: ${AZURE_CLIENT_SECRET}
  tenant_id: ${AZURE_TENANT_ID}
  root_path: /data

hive:
  metastore_uri: thrift://meta:9083
  database: silver
  warehouse_path: abfss://wh@acct.dfs.core.windows.net/

spark:
  app_name: DailyOrdersETL
  master: local[*]
  enable_hive_support: true
  extra_conf:
    spark.sql.shuffle.partitions: "200"
    spark.sql.adaptive.enabled: "true"
```

```python
from de_utils import ConfigLoader

spark    = ConfigLoader.from_yaml("pipeline.yml").build_session()
adls_cfg = ConfigLoader.adls_from_yaml("pipeline.yml")
hive_cfg = ConfigLoader.hive_from_yaml("pipeline.yml")
```

### From environment variables

```bash
export DE_ADLS_ACCOUNT_NAME=myadlsaccount
export DE_ADLS_CONTAINER=raw
export DE_ADLS_CLIENT_ID=...
export DE_ADLS_CLIENT_SECRET=...
export DE_ADLS_TENANT_ID=...
export DE_HIVE_METASTORE_URI=thrift://meta:9083
export DE_HIVE_DATABASE=silver
export DE_SPARK_APP_NAME=DailyOrdersETL
```

```python
adls_cfg = ConfigLoader.adls_from_env()
hive_cfg = ConfigLoader.hive_from_env()
spark    = ConfigLoader.spark_from_env().build_session()
```

---

## 2. ADLS Connector

```python
from de_utils import ADLSConnector

conn = ADLSConnector(adls_cfg, spark)

# ── Read ──────────────────────────────────────────────────────────────────────
raw_df   = conn.read_parquet("ingest/orders/2024/")
delta_df = conn.read_delta("ingest/events/", version=5)    # time travel
csv_df   = conn.read_csv("landing/customers.csv")
json_df  = conn.read_json("landing/events/", multiline=True)

# ── Write ─────────────────────────────────────────────────────────────────────
conn.write_parquet(raw_df, "staging/orders/", partition_by=["year", "month"])
conn.write_delta(raw_df,   "silver/orders/",  mode="append")
conn.write_csv(raw_df,     "exports/orders.csv")

# ── File-system helpers (requires azure-storage-file-datalake) ───────────────
conn.create_directory("archive/2024/")
paths  = conn.list_paths("staging/", recursive=True)
exists = conn.path_exists("staging/orders/year=2024/")
conn.delete_path("tmp/scratch/")

# ── Path builder ──────────────────────────────────────────────────────────────
full_path = conn.abfss("silver", "orders", "year=2024")
# → abfss://raw@myadlsaccount.dfs.core.windows.net/data/silver/orders/year=2024
```

---

## 3. Hive Metastore

```python
from de_utils import HiveMetastore, HiveConfig
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType

meta = HiveMetastore(HiveConfig(database="silver"), spark)

# ── Database ──────────────────────────────────────────────────────────────────
meta.create_database_if_not_exists(location=adls_cfg.path("silver"))
meta.list_databases()

# ── External table ────────────────────────────────────────────────────────────
schema = StructType([
    StructField("order_id",    LongType()),
    StructField("customer_id", LongType()),
    StructField("status",      StringType()),
    StructField("year",        IntegerType()),
    StructField("month",       IntegerType()),
])
meta.create_external_table(
    table_name="orders",
    schema=schema,
    location=adls_cfg.path("silver/orders"),
    partition_cols=["year", "month"],
    file_format="PARQUET",
    comment="Silver-layer orders table",
    table_properties={"de_utils.owner": "data-team@example.com"},
)

# ── Partition management ──────────────────────────────────────────────────────
meta.add_partition("orders", {"year": "2024", "month": "06"})
meta.repair_table("orders")           # MSCK REPAIR TABLE
meta.list_partitions("orders")
meta.drop_partition("orders", {"year": "2020", "month": "01"})

# ── Inspect ───────────────────────────────────────────────────────────────────
meta.list_tables()
meta.table_exists("orders")
meta.describe_table("orders").show()
meta.set_table_property("orders", "de_utils.version", "2")
meta.drop_table("old_table", purge=True)
```

---

## 4. Schema Manager

```python
from de_utils import SchemaManager

sm = SchemaManager(spark)

# ── Diff ──────────────────────────────────────────────────────────────────────
diff = sm.diff_schemas(old_schema, new_schema)
# {"added": ["region"], "dropped": [], "type_changed": ["amount"]}
sm.print_schema_diff(old_schema, new_schema)

# ── Evolve ────────────────────────────────────────────────────────────────────
aligned_df = sm.align_to_schema(source_df, target_schema)
df = sm.cast_columns(df, {"amount": "double", "event_date": "date"})

# ── Validate ──────────────────────────────────────────────────────────────────
sm.assert_schema_compatible(source_df, target_df)
sm.validate_not_null(df, ["order_id", "customer_id"])
sm.validate_unique(df, ["order_id"])

# ── DDL ───────────────────────────────────────────────────────────────────────
ddl = sm.schema_to_ddl(schema, exclude=["year", "month"])
```

---

## 5. Table Operations

```python
from de_utils import TableOperations

ops = TableOperations(spark, database="silver")

# ── CREATE ────────────────────────────────────────────────────────────────────
ops.create_or_replace("orders", df, partition_by=["year", "month"])
ops.create_if_not_exists("customers", dim_df)

# ── INSERT ────────────────────────────────────────────────────────────────────
ops.insert_append("orders", new_rows_df)
ops.insert_overwrite("orders", df, partition_by=["year", "month"])

# ── UPDATE / DELETE (Delta) ───────────────────────────────────────────────────
ops.update("orders", condition="status = 'PENDING'", set_clause={"status": "'SHIPPED'"})
ops.delete("orders", condition="created_date < '2020-01-01'")

# ── MERGE / UPSERT ────────────────────────────────────────────────────────────
ops.merge(
    target_table="orders",
    source_df=updates_df,
    match_keys=["order_id"],
    update_columns=["status", "updated_at"],
    insert_when_not_matched=True,
    delete_when_not_matched_by_source=False,
)
ops.upsert("orders", cdc_df, match_keys=["order_id"])
```

---

## 6. Data Loader

```python
from de_utils import DataLoader, LoadType

loader = DataLoader(spark, database="silver")

result = loader.full_load(raw_df, "orders", partition_by=["year", "month"])

result = loader.incremental_load(
    daily_df, "orders",
    watermark_column="updated_at",
    last_watermark="2024-05-31 23:59:59",
)

result = loader.partition_load(df_june, "orders", partition_keys=["year", "month"])

result = loader.merge_load(cdc_df, "orders", match_keys=["order_id"])

result = loader.load(df, "orders_snapshot", load_type=LoadType.SNAPSHOT,
                     snapshot_date="2024-06-30")

print(result)
# LoadResult(table='silver.orders', type=FULL, elapsed=4.2s, completed_at=...)
```

---

## 7. History Table

```python
from de_utils import HistoryTable

hist = HistoryTable(spark, database="gold")

hist.init_history_table(
    base_table="orders",
    source_df=df,
    key_columns=["order_id"],
    tracked_columns=["status", "amount", "address"],
    location=adls_cfg.path("gold/orders_hist"),
)

result = hist.capture(daily_df, "orders", key_columns=["order_id"])
# HistoryCaptureResult(inserts=120, updates=45, deletes=3)

snapshot     = hist.get_version_at("orders", as_of="2024-03-01 00:00:00")
all_versions = hist.get_full_history("orders")
```

Metadata columns added automatically:

| Column | Type | Description |
|---|---|---|
| `_valid_from` | TIMESTAMP | When this version became active |
| `_valid_to` | TIMESTAMP | When superseded (`9999-12-31` = still current) |
| `_is_current` | BOOLEAN | Convenience flag for the current row |
| `_row_hash` | STRING | MD5 hash of tracked columns |
| `_load_timestamp` | TIMESTAMP | When written to history table |
| `_operation` | STRING | `INSERT` / `UPDATE` / `DELETE` |

---

## 8. SCD Functions

```python
from de_utils import SCDHandler

scd = SCDHandler(spark, database="gold")

# SCD Type 1 — Overwrite (no history)
scd.apply_scd1("dim_customer", staging_df, key_columns=["customer_id"],
               update_columns=["email", "phone", "address"])

# SCD Type 2 — Full history (new row per change)
result = scd.apply_scd2(
    target_table="dim_customer",
    source_df=staging_df,
    key_columns=["customer_id"],
    tracked_columns=["email", "address"],
    effective_date="2024-06-01 00:00:00",
)
# SCD2Result(new_inserts=30, updates=12)

# SCD Type 3 — Previous-value columns
# Target table must have: email, prev_email, address, prev_address
scd.apply_scd3("dim_customer", staging_df,
               key_columns=["customer_id"],
               tracked_columns=["email", "address"])

# SCD Type 4 — Separate history table
scd.apply_scd4("dim_customer", "dim_customer_hist", staging_df,
               key_columns=["customer_id"])

# SCD Type 6 — Hybrid (1+2+3)
scd.apply_scd6("dim_customer", staging_df,
               key_columns=["customer_id"],
               tracked_columns=["email", "address"])

# Diff utility
diff_df = scd.diff(current_df, incoming_df, key_columns=["customer_id"],
                   compare_columns=["email", "address"])
# Returns _diff_type: UNCHANGED | CHANGED | NEW | DELETED
```

---

## 9. Data Quality

```python
from de_utils import DataQualityChecker, Rule, Severity

qc = DataQualityChecker(spark, table_name="silver.orders")

qc.add_rule(Rule.not_null("order_id"))
qc.add_rule(Rule.unique(["order_id"]))
qc.add_rule(Rule.min_value("amount", 0.0))
qc.add_rule(Rule.max_value("amount", 1_000_000.0))
qc.add_rule(Rule.between("year", 2020, 2030))
qc.add_rule(Rule.allowed_values("status", ["PENDING","SHIPPED","DELIVERED","CANCELLED"]))
qc.add_rule(Rule.regex("email", r"^[^@]+@[^@]+\.[^@]+$"))
qc.add_rule(Rule.row_count(">=", 1))
qc.add_rule(Rule.referential("customer_id", "gold.dim_customer", "customer_id"))
qc.add_rule(Rule.custom_sql(
    "SELECT * FROM {table} WHERE amount IS NOT NULL AND amount < 0",
    description="No negative amounts",
))
qc.add_rule(Rule.not_null("notes", severity=Severity.WARNING))  # warning only

report = qc.run(df)
report.show()
report.assert_no_failures()       # raises if any ERROR rule fails
report.to_json()                  # structured JSON
qc.save_report(report, "gold._dq_log")
```

---

## 10. Data Profiler

```python
from de_utils import DataProfiler

profiler = DataProfiler(spark, top_n=5)

# Profile a full DataFrame
profile = profiler.profile(orders_df, table_name="silver.orders")
profile.show()

# Profile on a sample (faster for large tables)
profile = profiler.profile(df, sample_fraction=0.1)

# Summary properties
profile.completeness_pct          # 99.2 — average non-null rate
profile.columns_with_nulls        # ["status", "notes"]
profile.constant_columns          # columns with only 1 distinct value
profile.high_cardinality_columns  # distinct > 50% of rows

# Per-column stats
col = profile.get_column("amount")
col.min_val, col.max_val, col.mean_val, col.stddev_val
col.p25, col.p50, col.p75, col.p95, col.p99
col.top_values     # [{"value": "99.99", "count": 1200}, ...]
col.is_complete    # no nulls
col.is_unique      # all distinct

# Outlier detection
flagged_df = profiler.detect_outliers(df, columns=["amount"], method="iqr", threshold=1.5)
flagged_df = profiler.detect_outliers(df, columns=["amount"], method="zscore", threshold=3.0)
flagged_df.filter("`_is_outlier` = true").show()

# Drift comparison between two profiles
drift_df = profiler.compare(profile_today, profile_yesterday)
drift_df.show()
# column   status   null_pct_a  null_pct_b  null_drift  distinct_a  distinct_b
# status   CHANGED  0.008       0.035       0.027       4           4
# amount   OK       0.000       0.000       0.000       12543       12891

# Persist for trending
profiler.save(profile, table="gold._profiles")
profile.to_json()
```

---

## 11. Lineage Tracking

```python
from de_utils import LineageTracker

tracker = LineageTracker(spark, lineage_table="gold._lineage", job_name="daily_orders_etl")

# Context manager — auto-captures timing
with tracker.track(source="bronze.orders_raw", target="silver.orders") as rec:
    transformed_df = transform(raw_df)
    transformed_df.write.mode("append").saveAsTable("silver.orders")
    rec.rows_read    = raw_df.count()
    rec.rows_written = transformed_df.count()

# Manual record
tracker.record(source="bronze.raw", target="silver.orders",
               transformation_type="INCREMENTAL", rows_read=50000, rows_written=49800)
tracker.save()

# Query
tracker.get_upstream("silver.orders").show()
tracker.get_downstream("bronze.orders_raw").show()
tracker.get_job_runs("daily_orders_etl", last_n=10).show()
```

---

## 12. Job Checkpointing

```python
from de_utils import JobCheckpoint

cp = JobCheckpoint(spark, state_table="gold._job_state", job_name="orders_incremental")

# Watermark management
last_wm = cp.get_watermark("updated_at")      # None on first run
df = spark.table("bronze.orders").filter(f"updated_at > '{last_wm or '1970-01-01'}'")
# ... process ...
cp.set_watermark("updated_at", str(df.agg({"updated_at": "max"}).collect()[0][0]))
cp.commit()

# Idempotent partition detection
if cp.is_already_run("2024-06-01"):
    print("Skipping — already processed")
else:
    process("2024-06-01")
    cp.mark_partition_done("2024-06-01")

# Arbitrary key-value state
cp.set("last_row_count", "50000")
count = cp.get("last_row_count")
cp.get_all_state().show()
cp.clear_state()
```

---

## 13. Delta Lake Utilities

```python
from de_utils import DeltaUtils

du = DeltaUtils(spark)

# Maintenance
du.optimize("silver.orders")
du.optimize("silver.orders", zorder_by=["customer_id", "order_date"])
du.vacuum("silver.orders", retention_hours=168)

# Time travel
du.show_history("silver.orders").show()
v5_df = du.read_version("silver.orders", version=5)
ts_df = du.read_timestamp("silver.orders", timestamp="2024-06-01 00:00:00")
du.restore("silver.orders", version=5)

# Clone
du.clone("silver.orders", "gold.orders_backup", shallow=True)
du.clone("silver.orders", "gold.orders_backup", shallow=False)

# Change data feed
changes_df = du.read_changes("silver.orders", start_version=10, end_version=20)

# Info
du.detail("silver.orders").show()
du.is_delta_table("silver.orders")
du.convert_to_delta("silver.old_parquet")
```

---

## 14. Audit Log

```python
from de_utils import AuditLog

audit = AuditLog(spark, audit_table="gold._audit_log", job_name="daily_etl")

# Manual entry
audit.log(table="silver.orders", operation="FULL_LOAD", rows_inserted=50000, status="SUCCESS")

# Context manager — auto-captures timing + errors
with audit.capture("silver.orders", "MERGE") as entry:
    ops.merge("silver.orders", updates_df, match_keys=["order_id"])
    entry.rows_updated  = 1200
    entry.rows_inserted = 300

audit.flush()
audit.get_table_history("silver.orders").show()
audit.get_failed_jobs(since="2024-01-01").show()
audit.get_run_summary(run_id="abc-123").show()
```

Audit log schema:

| Column | Description |
|---|---|
| `run_id` | UUID per job invocation |
| `job_name` | Passed to `AuditLog(job_name=...)` |
| `table_name` | Target table |
| `operation` | FULL_LOAD / MERGE / SCD2 / DQ_CHECK / etc. |
| `rows_inserted` / `rows_updated` / `rows_deleted` | Change counts |
| `status` | SUCCESS / FAILED / SKIPPED |
| `error_message` | Exception message if FAILED |
| `started_at` / `ended_at` / `duration_seconds` | Timing |

---

## 15. Medallion Architecture

```python
from de_utils import BronzeLoader, SilverLoader, GoldLoader

# Bronze — land raw data as-is
bronze = BronzeLoader(spark, adls_config=adls_cfg, database="bronze")
bronze.ingest(source_df=raw_df, target_table="orders_raw",
              source_tag="abfss://landing@acct.dfs.core.windows.net/orders/2024/06/")
# Auto-adds: _ingest_ts, _source_file, _run_id, _batch_date

# Silver — cleanse, deduplicate, enforce schema
silver = SilverLoader(spark, database="silver")
silver.load(
    source_df=bronze_df, target_table="orders",
    key_columns=["order_id"], partition_keys=["year", "month"],
    not_null_columns=["order_id", "customer_id"],
    cast_map={"amount": "double", "order_date": "date"},
    dedup=True, target_schema=orders_schema,
)

# Gold — SCD2 dimensions or facts with DQ
gold = GoldLoader(spark, database="gold")
gold.load_dimension(source_df=silver_df, target_table="dim_orders",
                    key_columns=["order_id"], tracked_columns=["status", "amount"])
gold.load_fact(source_df=silver_df, target_table="fact_daily_orders",
               partition_keys=["year", "month", "day"],
               dq_rules=[Rule.not_null("order_id"), Rule.min_value("amount", 0)])
```

---

## 16. Great Expectations Integration

```python
from de_utils import ExpectationSuiteBuilder, ValidatingLoader, LoadType

builder = ExpectationSuiteBuilder(spark, suite_name="orders_suite")

suite = builder.from_schema(
    schema=orders_schema,
    not_null_keys=["order_id", "customer_id"],
    unique_keys=["order_id"],
    numeric_ranges={"amount": (0, 1_000_000)},
    allowed_values={"status": ["PENDING","SHIPPED","DELIVERED","CANCELLED"]},
)

result = builder.validate(df, suite)
builder.assert_passed(result)

# Wire into DataLoader — validates BEFORE writing
vloader = ValidatingLoader(spark, database="silver", suite=suite)
vloader.load(df, "orders", load_type=LoadType.PARTITION, partition_keys=["year","month"])

builder.save_suite(suite, path="abfss://meta@acct.dfs.core.windows.net/suites/")
suite = builder.load_suite(path="abfss://meta@acct.dfs.core.windows.net/suites/orders_suite.json")
```

> If `great_expectations` is not installed, `ExpectationSuiteBuilder` automatically falls back to `de_utils.quality`.

---

## 17. Alerting

```python
from de_utils import AlertRouter, TeamsAlerter, SlackAlerter, EmailAlerter, alert_on_failure

router = AlertRouter(job_name="daily_orders_etl")
router.add(TeamsAlerter(webhook_url="https://outlook.office.com/webhook/..."))
router.add(SlackAlerter(webhook_url="https://hooks.slack.com/services/..."))
router.add(EmailAlerter(
    smtp_host="smtp.office365.com", smtp_port=587,
    sender="etl-alerts@example.com",
    recipients=["data-team@example.com"],
    username="etl-alerts@example.com", password="...",
))

# Manual alerts
router.send_failure("silver.orders", "Row count dropped to 0")
router.send_warning("Null rate exceeded threshold", table="silver.orders")
router.send_success("silver.orders", rows_written=50000, elapsed=12.4)

# DQ integration
report = qc.run(df)
if not report.passed:
    router.send_dq_failure(report)

# Auto-fire on unhandled exception — then re-raises
@alert_on_failure(router, table="silver.orders")
def run_silver_load():
    loader.full_load(raw_df, "orders")

# PagerDuty for critical incidents
from de_utils import PagerDutyAlerter
router.add(PagerDutyAlerter(integration_key="YOUR-PD-KEY", severity="critical"))
```

All channels are tried even if one fails. `AlertRouter.send()` returns `{channel_name: bool}` and never raises.

---

## 18. Secrets Management

```python
from de_utils import SecretsChain, AzureKeyVaultSecrets, EnvSecrets, StaticSecrets

# Build a chain: Key Vault → env vars → static fallback
secrets = SecretsChain([
    AzureKeyVaultSecrets(vault_url="https://my-vault.vault.azure.net/"),
    EnvSecrets(prefix="DE_"),
    StaticSecrets({"dev-only-key": "dev-value"}),
])

# Fetch secrets
db_password = secrets.get("db-password")           # Key Vault → DE_DB_PASSWORD → static
token       = secrets.get_required("api-token")    # raises DataEngineeringError if not found

# Resolve ${SECRET:name} placeholders in any config dict
raw_cfg = {
    "host":     "mydb.azure.com",
    "password": "${SECRET:db-password}",
    "nested":   {"token": "${SECRET:api-token}"},
}
resolved = secrets.resolve_dict(raw_cfg)
# resolved["password"] → actual password from Key Vault

# Key Vault helpers
kv = AzureKeyVaultSecrets("https://my-vault.vault.azure.net/")
kv.set("new-secret", "value")
kv.list_secrets()
kv.delete("old-secret")
kv.clear_cache()   # force re-fetch
```

`EnvSecrets` maps `my-secret` → `DE_MY_SECRET` (hyphens to underscores, uppercased).
`AzureKeyVaultSecrets` uses `DefaultAzureCredential` — works with Managed Identity, service principal, VS Code, and Azure CLI automatically.

---

## 19. Retry & Resilience

```python
from de_utils import retry, RetryPolicy, CircuitBreaker, with_retry

# @retry decorator with exponential back-off
@retry(max_attempts=4, wait_seconds=2.0, backoff=2.0, jitter=True)
def read_from_adls(path):
    return conn.read_parquet(path)

# Reusable RetryPolicy
policy = RetryPolicy(
    max_attempts=5, wait_seconds=1.0, backoff=2.0,
    max_wait_seconds=30.0, jitter=True,
    retryable_on=(ConnectionError, TimeoutError),
)
result = policy.execute(lambda: spark.table("silver.orders").count())

# Functional form — no decorator needed
count = with_retry(
    lambda: spark.table("silver.orders").count(),
    RetryPolicy(max_attempts=3, wait_seconds=2),
)

# CircuitBreaker — stop hammering a failing service
cb = CircuitBreaker(failure_threshold=3, recovery_timeout=60, name="adls_writer")

@cb.protect
def write_to_delta(df, path):
    conn.write_delta(df, path)

cb.state    # CircuitState.CLOSED / OPEN / HALF_OPEN
cb.reset()  # manually reset to CLOSED
print(cb)   # CircuitBreaker(name='adls_writer', state=CLOSED, failures=0/3)
```

Back-off schedule with `wait_seconds=1, backoff=2, jitter=True`:

| Attempt | Base wait | With jitter range |
|---|---|---|
| 1st retry | 1.0s | 0.75s – 1.25s |
| 2nd retry | 2.0s | 1.5s – 2.5s |
| 3rd retry | 4.0s | 3.0s – 5.0s |
| 4th retry | 8.0s | 6.0s – 10.0s |

Automatically retries on `ConnectionError`, `TimeoutError`, `OSError`, and any message containing keywords like `"timeout"`, `"throttl"`, `"connection reset"`, `"service unavailable"`.

---

## 20. Pipeline Orchestration

```python
from de_utils import Pipeline

pipeline = Pipeline(
    name="daily_orders_etl",
    spark=spark,
    max_workers=4,        # tasks with no inter-dependencies run in parallel
    audit_log=audit,      # auto-log every task result
    alert_router=router,  # auto-alert on pipeline failure
)

@pipeline.task()
def ingest_bronze():
    BronzeLoader(spark, adls_cfg, "bronze").ingest(raw_df, "orders_raw")

@pipeline.task(depends_on=["ingest_bronze"])
def validate_bronze():
    qc = DataQualityChecker(spark, "bronze.orders_raw")
    qc.add_rule(Rule.not_null("order_id")).add_rule(Rule.row_count(">=", 1))
    qc.run(spark.table("bronze.orders_raw")).assert_no_failures()

@pipeline.task(depends_on=["ingest_bronze"])
def profile_bronze():
    DataProfiler(spark).profile(spark.table("bronze.orders_raw")).show()

# validate_bronze and profile_bronze both depend on ingest_bronze only
# → they run in parallel at Level 2

@pipeline.task(depends_on=["validate_bronze", "profile_bronze"], retries=2, retry_delay=10)
def load_silver():
    SilverLoader(spark, "silver").load(
        spark.table("bronze.orders_raw"), "orders",
        key_columns=["order_id"], partition_keys=["year","month"], dedup=True,
    )

@pipeline.task(depends_on=["load_silver"])
def load_gold():
    GoldLoader(spark, "gold").load_dimension(
        spark.table("silver.orders"), "dim_orders",
        key_columns=["order_id"], tracked_columns=["status","amount"],
    )

# Preview the execution plan
print(pipeline.visualize())
# Pipeline: daily_orders_etl
#   Level 1:  [ingest_bronze]
#   Level 2:  [validate_bronze]  →  [profile_bronze]   (parallel)
#   Level 3:  [load_silver]
#   Level 4:  [load_gold]

# Dry-run — prints plan, executes nothing
pipeline.run(dry_run=True)

# Execute
result = pipeline.run()
result.show()
# ══════════════════════════════════════════════════════════════════
#   Pipeline: daily_orders_etl  ✅  (38.2s total)
# ══════════════════════════════════════════════════════════════════
#   ✅ ingest_bronze    SUCCESS    4.1s
#   ✅ validate_bronze  SUCCESS    2.8s
#   ✅ profile_bronze   SUCCESS    5.3s
#   ✅ load_silver      SUCCESS   18.4s
#   ✅ load_gold        SUCCESS   12.9s
# ══════════════════════════════════════════════════════════════════

result.succeeded       # True / False
result.failed_tasks    # ["load_gold"]
result.elapsed_sec     # 38.2
result.task_results    # {"ingest_bronze": TaskResult(status=SUCCESS, ...), ...}

# Tag-based partial runs
@pipeline.task(depends_on=["load_silver"], tags=["daily", "critical"])
def send_report(): ...

pipeline.run(tags=["daily"])   # only tasks tagged "daily"
```

---

## 21. CLI Reference

After `pip install de_utils`, the `de_utils` command is available everywhere:

```bash
# Profile a table
de_utils profile silver.orders
de_utils profile silver.orders --sample 0.1 --top-n 10
de_utils profile silver.orders --save gold._profiles

# Audit log
de_utils audit silver.orders
de_utils audit silver.orders --since 2024-01-01 --limit 50

# Lineage
de_utils lineage silver.orders
de_utils lineage silver.orders --lineage-table gold._lineage

# Job checkpoint state
de_utils checkpoint daily_orders_etl
de_utils checkpoint daily_orders_etl --clear      # wipe watermarks

# Delta Lake
de_utils delta optimize silver.orders
de_utils delta optimize silver.orders --zorder customer_id,order_date
de_utils delta vacuum   silver.orders --retention 168
de_utils delta history  silver.orders
de_utils delta detail   silver.orders

# Run a pipeline defined in a Python module
de_utils pipeline run jobs.daily_orders:pipeline
de_utils pipeline run jobs.daily_orders:pipeline --dry-run
de_utils pipeline run jobs.daily_orders:pipeline --tags daily

# Version
de_utils version
# de_utils v2.1.0
```

---

## End-to-End Example

A complete, production-grade Bronze → Silver → Gold pipeline using every major feature:

```python
from de_utils import (
    ConfigLoader, ADLSConnector,
    BronzeLoader, SilverLoader, GoldLoader,
    DataQualityChecker, Rule, DataProfiler,
    LineageTracker, JobCheckpoint, AuditLog, HistoryTable,
    AlertRouter, TeamsAlerter, alert_on_failure,
    SecretsChain, AzureKeyVaultSecrets, EnvSecrets,
    RetryPolicy, Pipeline,
)

# ── Bootstrap ─────────────────────────────────────────────────────────────────
secrets  = SecretsChain([AzureKeyVaultSecrets("https://my-vault.vault.azure.net/"), EnvSecrets("DE_")])
spark    = ConfigLoader.from_yaml("pipeline.yml", secrets=secrets).build_session()
adls_cfg = ConfigLoader.adls_from_yaml("pipeline.yml", secrets=secrets)
conn     = ADLSConnector(adls_cfg, spark)

router  = AlertRouter(job_name="daily_orders").add(TeamsAlerter(webhook_url=secrets.get_required("teams-webhook")))
audit   = AuditLog(spark,    audit_table="gold._audit_log",  job_name="daily_orders")
tracker = LineageTracker(spark, lineage_table="gold._lineage", job_name="daily_orders")
cp      = JobCheckpoint(spark, state_table="gold._job_state", job_name="daily_orders")
policy  = RetryPolicy(max_attempts=3, wait_seconds=5, backoff=2)

# ── Define pipeline ───────────────────────────────────────────────────────────
pipeline = Pipeline("daily_orders", spark=spark, max_workers=2,
                    audit_log=audit, alert_router=router)

@pipeline.task()
@alert_on_failure(router, table="bronze.orders_raw")
def ingest_bronze():
    raw_df = policy.execute(lambda: conn.read_parquet("landing/orders/2024/06/"))
    with audit.capture("bronze.orders_raw", "INGEST") as e:
        BronzeLoader(spark, adls_cfg, "bronze").ingest(raw_df, "orders_raw")
        e.rows_inserted = raw_df.count()

@pipeline.task(depends_on=["ingest_bronze"])
def validate_and_profile():
    df = spark.table("bronze.orders_raw")
    DataProfiler(spark).profile(df, "bronze.orders_raw").show()
    qc = DataQualityChecker(spark, "bronze.orders_raw")
    qc.add_rule(Rule.not_null("order_id")).add_rule(Rule.row_count(">=", 1))
    report = qc.run(df)
    if not report.passed:
        router.send_dq_failure(report)
    report.assert_no_failures()

@pipeline.task(depends_on=["validate_and_profile"])
def load_silver():
    last_wm   = cp.get_watermark("updated_at")
    bronze_df = spark.table("bronze.orders_raw").filter(f"updated_at > '{last_wm or '1970-01-01'}'")
    with tracker.track("bronze.orders_raw", "silver.orders"):
        SilverLoader(spark, "silver").load(
            bronze_df, "orders", key_columns=["order_id"],
            partition_keys=["year","month"], cast_map={"amount": "double"}, dedup=True,
        )
    cp.set_watermark("updated_at", str(bronze_df.agg({"updated_at": "max"}).collect()[0][0]))
    cp.commit()

@pipeline.task(depends_on=["load_silver"], retries=2, retry_delay=10)
def load_gold():
    silver_df = spark.table("silver.orders")
    with tracker.track("silver.orders", "gold.dim_orders"):
        GoldLoader(spark, "gold").load_dimension(
            silver_df, "dim_orders", key_columns=["order_id"], tracked_columns=["status","amount"],
        )
        HistoryTable(spark, "gold").capture(silver_df, "orders", key_columns=["order_id"])

# ── Run ───────────────────────────────────────────────────────────────────────
result = pipeline.run()
result.show()

audit.flush()
tracker.save()
router.send_success("daily_orders", rows_written=50000, elapsed=result.elapsed_sec)
```

---

## Running Tests

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run all 343 tests
pytest tests/ -v

# With coverage report
pytest tests/ -v --cov=de_utils --cov-report=term-missing

# Run a specific module
pytest tests/test_quality.py -v
pytest tests/test_profiler.py -v
pytest tests/test_retry.py -v
pytest tests/test_secrets_and_pipeline.py -v
```

| Test file | Tests | What's covered |
|---|---|---|
| `test_config.py` | 15 | ADLSConfig, HiveConfig, SparkConfig |
| `test_config_loader.py` | 18 | YAML/env loading, interpolation |
| `test_utils.py` | 16 | Logger, validators, helpers |
| `test_schema.py` | 17 | Diff, align, cast, validate |
| `test_table_ops.py` | 11 | CREATE, INSERT, MERGE, UPSERT |
| `test_loader.py` | 13 | All 5 load strategies |
| `test_history.py` | 14 | Init, capture, point-in-time |
| `test_scd.py` | 15 | SCD1/2/3/6 + diff |
| `test_quality.py` | 28 | All DQ rule types + report |
| `test_profiler.py` | 34 | Stats, nulls, outliers, drift |
| `test_lineage.py` | 7 | Record, context manager, query |
| `test_checkpoint.py` | 11 | Watermarks, partition-done |
| `test_delta.py` | 11 | Optimize, vacuum, time travel |
| `test_audit.py` | 11 | Log, context manager, queries |
| `test_medallion.py` | 16 | Bronze/Silver/Gold loaders |
| `test_expectations.py` | 17 | Suite from schema, validate |
| `test_storage.py` | 12 | Read/write, mocked Azure SDK |
| `test_metastore.py` | 11 | Database, DDL, partitions |
| `test_alerts.py` | 18 | Router, all channels, decorator |
| `test_retry.py` | 22 | Retry, back-off, CircuitBreaker |
| `test_secrets_and_pipeline.py` | 44 | Secrets chain + pipeline DAG |
| **Total** | **343** | |

---

## License

MIT — see [LICENSE](LICENSE) for details.
