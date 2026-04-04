# de_utils v2.1 — Data Engineering Utility Library

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python Version](https://img.shields.io/badge/python-3.8%2B-blue.svg)](https://www.python.org/)
[![Spark Version](https://img.shields.io/badge/spark-3.3.0%2B-orange.svg)](https://spark.apache.org/)

`de_utils` is a production-grade, "batteries-included" utility library designed to accelerate Data Engineering workflows on **Azure** and **Databricks**. It provides a standardized framework for building robust, scalable, and observable ETL pipelines using PySpark and Delta Lake.

---

## 📑 Table of Contents

1.  [Installation](#-installation)
2.  [What's New in v2.1](#-whats-new-in-v21)
3.  [Module Overview](#-module-overview)
4.  [Core Components](#-core-components)
    *   [Configuration & Secrets](#1-configuration--secrets)
    *   [Storage & Metastore](#2-storage--metastore)
    *   [Data Loading & SCD](#3-data-loading--scd)
    *   [Data Quality & Profiling](#4-data-quality--profiling)
    *   [Observability (Lineage & Audit)](#5-observability-lineage--audit)
    *   [Resilience (Retry & Circuit Breaker)](#6-resilience-retry--circuit-breaker)
5.  [Databricks Compatibility](#-databricks-compatibility)
6.  [Pipeline Orchestration](#-pipeline-orchestration)
7.  [CLI Reference](#-cli-reference)
8.  [End-to-End Example](#-end-to-end-example)
9.  [License](#-license)

---

## 🚀 Installation

Install the core library:
```bash
pip install de_utils
```

Install with specific extras:
```bash
# For Azure ADLS and Key Vault support
pip install de_utils[azure,keyvault]

# For Delta Lake and Great Expectations
pip install de_utils[delta,gx]

# Install everything
pip install de_utils[all]
```

---

## ✨ What's New in v2.1

*   **Native Databricks Support**: Seamless integration with DBFS, Mount points, and Notebook utilities.
*   **Pipeline Orchestration**: A lightweight DAG-based task runner for complex ETL flows.
*   **Circuit Breaker Pattern**: Prevent cascading failures in distributed environments.
*   **Medallion Architecture Loaders**: Standardized classes for Bronze, Silver, and Gold layers.
*   **Great Expectations Integration**: Build and run expectation suites directly within your loaders.

---

## 🧩 Module Overview

| Module | Description |
| :--- | :--- |
| `config` | Pydantic-based configuration for ADLS, Hive, and Spark. |
| `storage` | High-level ADLS Gen2 connector with file-system operations. |
| `loader` | Unified data loader supporting Full, Append, and Overwrite modes. |
| `scd` | Type 1 and Type 2 Slowly Changing Dimension (SCD) handlers. |
| `quality` | Rule-based data quality engine with severity levels. |
| `profiler` | Automated data profiling and statistics generation. |
| `lineage` | Column-level and table-level lineage tracking. |
| `checkpoint` | Reliable job checkpointing to prevent redundant processing. |
| `delta` | Advanced Delta Lake utilities (Optimize, Vacuum, Time Travel). |
| `medallion` | Specialized loaders for the Medallion architecture. |
| `retry` | Advanced retry logic with exponential backoff and circuit breakers. |
| `pipeline` | Task-based pipeline orchestration with dependency management. |

---

## 🛠 Core Components

### 1. Configuration & Secrets
Manage environment-specific settings and sensitive credentials securely.

```python
from de_utils.secrets import SecretsChain, AzureKeyVaultSecrets, EnvSecrets

# Chain multiple secret providers
secrets = SecretsChain([
    AzureKeyVaultSecrets(vault_url="https://my-vault.vault.azure.net/"),
    EnvSecrets()
])

db_password = secrets.get("DB_PASSWORD")
```

### 2. Storage & Metastore
Interact with ADLS Gen2 and Hive Metastore effortlessly.

```python
from de_utils.storage import ADLSConnector

adls = ADLSConnector(account_name="mystorage", container="raw")
adls.upload_file("local_path.csv", "landing/data.csv")
```

### 3. Data Loading & SCD
Handle complex loading patterns including SCD Type 2.

```python
from de_utils.scd import SCDHandler, SCDType

handler = SCDHandler(spark, target_table="dim_customers")
handler.process(df_updates, scd_type=SCDType.TYPE_2, key_columns=["customer_id"])
```

### 4. Data Quality & Profiling
Ensure data integrity with built-in validation and profiling.

```python
from de_utils.quality import DataQualityChecker, Rule

checker = DataQualityChecker(df)
checker.add_rule(Rule("age", "not_null"))
checker.add_rule(Rule("salary", "min_value", value=0))

report = checker.run()
if not report.passed:
    print(report.summary)
```

### 5. Observability (Lineage & Audit)
Track data movement and audit every job execution.

```python
from de_utils.audit import AuditLog

with AuditLog(spark, job_name="daily_sales_load") as logger:
    # ETL Logic here
    logger.log_success(records_processed=1000)
```

### 6. Resilience (Retry & Circuit Breaker)
Protect your pipelines from transient failures.

```python
from de_utils.retry import retry, RetryPolicy

@retry(policy=RetryPolicy(max_attempts=3, backoff_factor=2))
def fetch_api_data():
    # Unreliable network call
    pass
```

---

## ☁️ Databricks Compatibility

The library now detects if it's running on Databricks and provides specialized utilities:

*   **`DBFSConnector`**: Optimized file operations for `dbfs:/` paths.
*   **`MountConnector`**: Manage ADLS mount points programmatically.
*   **`NotebookUtils`**: Access `dbutils` functionality in a type-safe way.
*   **`DatabricksSecrets`**: Direct integration with Databricks Secret Scopes.

---

## ⛓ Pipeline Orchestration

Build complex ETL workflows with dependency management:

```python
from de_utils.pipeline import Pipeline

pipeline = Pipeline("Customer_360")

@pipeline.task(name="extract")
def extract():
    return spark.read.csv(...)

@pipeline.task(name="transform", depends_on=["extract"])
def transform(df):
    return df.filter(...)

result = pipeline.run()
```

---

## 💻 CLI Reference

The `de_utils` CLI provides powerful tools for data management:

```bash
# Profile a Delta table
de_utils profile --table sales.orders

# Check job lineage
de_utils lineage --job daily_load

# Run a pipeline
de_utils pipeline run --config config.yaml

# Optimize a Delta table
de_utils delta optimize --table sales.orders --zorder customer_id
```

---

## 📝 End-to-End Example

```python
from de_utils import ADLSConnector, DataLoader, DataQualityChecker, AuditLog

def run_etl():
    with AuditLog(spark, "Bronze_to_Silver") as audit:
        # 1. Connect to ADLS
        adls = ADLSConnector(account_name="mystorage", container="bronze")
        
        # 2. Load Data
        loader = DataLoader(spark)
        df = loader.load("abfss://bronze@mystorage.dfs.core.windows.net/raw_data")
        
        # 3. Quality Check
        dq = DataQualityChecker(df)
        dq.add_rule("id", "unique")
        if dq.run().passed:
            # 4. Save to Silver (Delta)
            df.write.format("delta").mode("overwrite").saveAsTable("silver.customers")
            audit.log_success(records=df.count())

if __name__ == "__main__":
    run_etl()
```

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
