# Databricks Notebook — de_utils Quickstart
# ==========================================
# Copy-paste each cell into your Databricks notebook.
# Works on Community Edition and standard workspaces.

# ── Cell 1: Install ───────────────────────────────────────────────────────────
# %pip install de_utils
# (restart kernel after install — Databricks will prompt you)

# ── Cell 2: Imports ───────────────────────────────────────────────────────────
from de_utils.databricks import (
    DatabricksConfig,
    DatabricksSession,
    DatabricksSecrets,
    DBFSConnector,
    MountConnector,
    DatabricksTableOps,
    DatabricksLoader,
    DatabricksAuditLog,
    DatabricksCheckpoint,
    NotebookUtils,
    is_databricks,
    is_community_edition,
)
from de_utils import DataQualityChecker, Rule, DataProfiler, SCDHandler

print(f"Running in Databricks: {is_databricks()}")
print(f"Community Edition    : {is_community_edition()}")

# ── Cell 3: Config ────────────────────────────────────────────────────────────
cfg = DatabricksConfig(
    dbfs_root="dbfs:/de_utils",      # all data stored under here on DBFS
    default_database="silver",
)
print(cfg)
# DatabricksConfig(dbfs_root='dbfs:/de_utils', database='silver', community=True, ...)

# ── Cell 4: Spark session (safe — uses the pre-existing one) ─────────────────
session = DatabricksSession.get()
DatabricksSession.set_shuffle_partitions(session, n=8)  # important for CE!
DatabricksSession.configure_for_delta(session)
print(f"Spark version: {session.version}")

# ── Cell 5a: Read/Write with DBFS (Community Edition — no credentials) ───────
conn = DBFSConnector(session)

# Create sample data
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType
schema = StructType([
    StructField("order_id",   LongType()),
    StructField("customer_id",LongType()),
    StructField("status",     StringType()),
    StructField("amount",     DoubleType()),
])
sample_df = session.createDataFrame(
    [(1, 100, "PENDING", 99.9), (2, 101, "SHIPPED", 150.0), (3, 102, "DELIVERED", 200.0)],
    schema=schema,
)

# Write to DBFS as Delta
conn.write_delta(sample_df, cfg.path("raw", "orders"), mode="overwrite")
print("Written to DBFS ✅")

# Read back
orders_df = conn.read_delta(cfg.path("raw", "orders"))
orders_df.show()

# ── Cell 5b: Mount ADLS (Community Edition — account key) ────────────────────
# Only needed if your data is on ADLS. Skip this on CE if using DBFS only.
#
# mnt = MountConnector(session, dbutils=dbutils)
# mnt.mount_adls(
#     account_name="myadlsaccount",
#     container="raw",
#     mount_name="raw",
#     account_key="YOUR_ACCOUNT_KEY",   # get from DatabricksSecrets below
# )
# mounted_df = mnt.read_parquet("orders/2024/")

# ── Cell 6: Secrets (Databricks Secret Scope) ─────────────────────────────────
# First create a scope via Databricks CLI:
#   databricks secrets create-scope de-utils-scope
#   databricks secrets put --scope de-utils-scope --key adls-account-key
#
# Then read it:
# secrets = DatabricksSecrets(scope="de-utils-scope", dbutils=dbutils)
# key = secrets.get_required("adls-account-key")
# print("Secret loaded ✅")

# ── Cell 7: Create database and tables ────────────────────────────────────────
ops = DatabricksTableOps(session, database="silver")
ops.create_database(location=cfg.path("silver"))

ops.create_or_replace(
    "orders",
    orders_df,
    partition_by=["status"],
    location=cfg.path("silver", "orders"),
)
print(ops.list_tables())

# ── Cell 8: Data Quality ──────────────────────────────────────────────────────
qc = DataQualityChecker(session, table_name="silver.orders")
qc.add_rule(Rule.not_null("order_id"))
qc.add_rule(Rule.unique(["order_id"]))
qc.add_rule(Rule.min_value("amount", 0.0))
qc.add_rule(Rule.allowed_values("status", ["PENDING","SHIPPED","DELIVERED","CANCELLED"]))

report = qc.run(orders_df)
report.show()
report.assert_no_failures()   # stops the notebook if any ERROR rule fails

# ── Cell 9: Data Profiling ────────────────────────────────────────────────────
profiler = DataProfiler(session, top_n=5)
profile  = profiler.profile(orders_df, table_name="silver.orders")
profile.show()

# Detect outliers
flagged = profiler.detect_outliers(orders_df, columns=["amount"], method="iqr")
flagged.filter("`_is_outlier` = true").show()

# ── Cell 10: Load strategies ──────────────────────────────────────────────────
loader = DatabricksLoader(session, database="silver")

# Full load
result = loader.full_load(orders_df, "orders_full", partition_by=["status"])
print(result)

# Incremental
updates_df = session.createDataFrame(
    [(4, 103, "PENDING", 55.0)], schema=schema
)
result = loader.incremental_load(
    updates_df, "orders_full",
    watermark_col="order_id", last_watermark="3",
)
print(result)

# Merge / upsert
cdc_df = session.createDataFrame(
    [(1, 100, "SHIPPED", 99.9), (5, 104, "PENDING", 30.0)], schema=schema
)
result = loader.merge_load(cdc_df, "orders_full", match_keys=["order_id"])
print(result)

# ── Cell 11: SCD Type 2 ───────────────────────────────────────────────────────
from pyspark.sql import functions as F

# Prepare SCD2 table (needs effective_from, effective_to, is_current columns)
scd2_df = (
    orders_df
    .withColumn("effective_from", F.lit("2024-01-01 00:00:00").cast("timestamp"))
    .withColumn("effective_to",   F.lit("9999-12-31 23:59:59").cast("timestamp"))
    .withColumn("is_current",     F.lit(True))
)
ops.create_or_replace("dim_orders", scd2_df,
                       location=cfg.path("silver", "dim_orders"))

scd = SCDHandler(session, database="silver")
incoming = session.createDataFrame(
    [(1, 100, "SHIPPED", 99.9)], schema=schema  # order_id=1 status changed
)
incoming_scd2 = (
    incoming
    .withColumn("effective_from", F.lit("2024-06-01 00:00:00").cast("timestamp"))
    .withColumn("effective_to",   F.lit("9999-12-31 23:59:59").cast("timestamp"))
    .withColumn("is_current",     F.lit(True))
)
result = scd.apply_scd2(
    target_table="dim_orders",
    source_df=incoming_scd2,
    key_columns=["order_id"],
    tracked_columns=["status", "amount"],
)
print(result)

# ── Cell 12: Checkpointing ────────────────────────────────────────────────────
cp = DatabricksCheckpoint(
    session,
    table="silver._job_state",
    job_name="daily_orders_etl",
)

last_wm = cp.get_watermark("order_id")
print(f"Last watermark: {last_wm}")   # None on first run

# Load only new rows
new_rows = orders_df.filter(
    f"order_id > {last_wm or 0}"
)
print(f"New rows: {new_rows.count()}")

# Save new watermark after successful load
cp.set_watermark("order_id", "3")
cp.commit()

# Idempotent partition check
today = "2024-06-01"
if cp.is_already_run(today):
    print(f"Partition {today} already loaded — skipping")
else:
    print(f"Loading partition {today}")
    cp.mark_partition_done(today)

# ── Cell 13: Audit Log ────────────────────────────────────────────────────────
audit = DatabricksAuditLog(
    session,
    table="silver._audit_log",
    job_name="daily_orders_etl",
)

with audit.capture("silver.orders", "FULL_LOAD") as entry:
    loader.full_load(orders_df, "orders_full")
    entry.rows_inserted = orders_df.count()

session.table("silver._audit_log").show()

# ── Cell 14: Notebook widgets (optional) ─────────────────────────────────────
# nu = NotebookUtils(dbutils)
# nu.widget_dropdown("env", "dev", ["dev", "staging", "prod"], "Environment")
# nu.widget_text("start_date", "2024-01-01", "Start Date")
# env        = nu.get_widget("env")
# start_date = nu.get_widget("start_date")
# print(f"Running for env={env}, start={start_date}")

# ── Cell 15: Delta maintenance ────────────────────────────────────────────────
# OPTIMIZE with Z-ORDER (improves query speed on CE)
ops.optimize("orders_full", zorder_by=["order_id"])
# VACUUM old snapshots (keep 7 days)
ops.vacuum("orders_full", retention_hours=168)

print("Pipeline complete ✅")
# nu.exit("SUCCESS")   # if running as a Databricks Job
