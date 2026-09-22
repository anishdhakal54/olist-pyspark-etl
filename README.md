# Olist E-commerce Data Engineering Pipeline

## Project Overview

This project implements an end-to-end data engineering pipeline using the Olist Brazilian e-commerce dataset.

The project began as a local modular PySpark ETL pipeline and was subsequently extended to Azure to demonstrate a cloud-based lakehouse architecture using Azure Data Lake Storage Gen2, Azure Databricks, PySpark, and Delta Lake.

The pipeline ingests multiple related Olist datasets, performs data-quality and referential-integrity checks, transforms and aggregates the data, and organizes the cloud implementation into Bronze, Silver, and Gold layers.

The Azure implementation also introduces Delta Lake capabilities including MERGE-based upserts, schema enforcement and evolution, table history, time travel, restore, Change Data Feed (CDF), and incremental processing.

---

## Architecture

The current cloud pipeline follows a Medallion Architecture:

```text
Olist CSV Dataset
        |
        v
Azure Data Lake Storage Gen2
        |
        v
+-----------------------+
|        BRONZE         |
| Raw Data              |
| Ingestion Metadata    |
+-----------+-----------+
            |
            | PySpark
            v
+-----------------------+
|        SILVER         |
| Cleaned Data          |
| Validated Data        |
| Curated Delta Tables  |
+-----------+-----------+
            |
            | Delta Change Data Feed
            | Incremental Processing
            v
+-----------------------+
|         GOLD          |
| Business Metrics      |
| Analytical Aggregates |
+-----------------------+
```

The project retains the original local PySpark implementation alongside the Azure implementation, demonstrating the progression from a local ETL application to a cloud-based data engineering pipeline.

---

## Technologies Used

### Data Processing

* Python
* PySpark
* Spark SQL
* Spark DataFrame API

### Azure

* Microsoft Azure
* Azure Data Lake Storage Gen2
* Azure Databricks
* Managed Identity

### Data Storage

* Delta Lake
* Parquet

### Development

* Git
* GitHub
* Jupyter Notebook
* Linux

---

## Dataset

The project uses the Olist Brazilian e-commerce dataset.

The pipeline works with nine related datasets:

* Orders
* Customers
* Order Items
* Order Payments
* Order Reviews
* Products
* Sellers
* Geolocation
* Product Category Name Translation

These datasets represent different parts of an e-commerce system and provide an opportunity to work with multiple grains, one-to-many relationships, data-quality problems, joins, aggregations, and incremental processing.

---

# Local PySpark Pipeline

The first implementation of the project was built locally as a modular PySpark ETL application.

The local pipeline follows:

```text
Raw CSV Files
      |
      v
Ingestion
      |
      v
Data Quality Validation
      |
      v
Transformations
      |
      v
Aggregations
      |
      v
Curation
      |
      v
Output Validation
      |
      v
Curated Parquet Files
```

The local implementation separates ingestion, validation, transformation, curation, writing, configuration, and orchestration into reusable Python modules.

---

## Local Project Structure

```text
olist-pyspark-etl/
│
├── data/
│   ├── raw/
│   └── curated/
│       ├── orders/
│       └── order_items/
│
├── notebooks/
│   └── 01_data_ingestion.ipynb
│
├── src/
│   ├── config.py
│   ├── ingestion.py
│   ├── validation.py
│   ├── transformation.py
│   ├── curation.py
│   ├── writer.py
│   └── main.py
│
├── .gitignore
└── README.md
```

---

# Pipeline Components

## 1. Data Ingestion

The ingestion layer reads the Olist CSV datasets using explicitly defined Spark schemas.

Reusable ingestion logic keeps data loading separate from dataset-specific transformations and validation.

The cloud version stores source data in Azure Data Lake Storage Gen2 before processing it with Azure Databricks.

The Bronze layer preserves the ingested data while also recording ingestion metadata to improve traceability.

---

## 2. Data Quality Validation

Reusable PySpark validation logic is used to identify structural problems and source-data anomalies.

Validation includes:

* Duplicate business-key detection
* Null analysis
* Date consistency validation
* Allowed-value validation
* Referential-integrity validation using left anti joins

Examples of referential-integrity relationships include:

```text
orders.customer_id       → customers.customer_id
order_items.order_id     → orders.order_id
order_items.product_id   → products.product_id
order_items.seller_id    → sellers.seller_id
```

Critical structural problems can stop pipeline processing rather than allowing invalid records to silently contaminate downstream datasets.

Non-critical source anomalies can instead be retained and logged for investigation.

For example, the Olist source data contains records where the customer delivery timestamp occurs before the carrier delivery timestamp. These records are identified as data-quality warnings rather than silently modified.

---

## 3. Transformations

PySpark transformations enrich the source data with attributes useful for downstream analytics.

Transformations include:

### Delivery Duration

Calculates the number of days between purchase and customer delivery.

```text
order_purchase_timestamp
        +
order_delivered_customer_date
        |
        v
delivery_days
```

### Purchase Date Attributes

Time-based attributes are extracted from purchase timestamps to support downstream aggregation and analysis.

Examples include:

* Purchase year
* Purchase month
* Order date

### Late Delivery Flag

An `is_late` indicator compares the actual customer delivery date against the estimated delivery date.

---

## 4. Aggregations

Lower-grain datasets are aggregated before being joined to order-level data.

This prevents one-to-many relationships from unintentionally multiplying order records.

### Order Item Aggregation

Order items are grouped by `order_id` to calculate:

* Item count
* Total item price
* Total freight value

### Payment Aggregation

Payments are grouped by `order_id` to calculate:

* Total payment value
* Payment count

The aggregated datasets can then safely be joined to orders while preserving the intended order-level grain.

---

# Medallion Architecture

## Bronze Layer

The Bronze layer represents the raw ingestion layer.

Its purpose is to preserve source data with minimal transformation while maintaining information about how and when the data entered the pipeline.

Bronze ingestion includes metadata to support traceability and future pipeline operations.

```text
ADLS Raw Data
      |
      v
Bronze
Raw + Metadata
```

The Bronze layer provides the starting point for downstream validation and transformation.

---

## Silver Layer

The Silver layer contains cleaned, validated, enriched, and joined data suitable for downstream processing.

The primary Silver orders dataset combines:

* Order information
* Customer information
* Order-item aggregates
* Payment aggregates

Its grain is:

```text
one row per order
```

Before data reaches downstream analytical processing, the pipeline performs data-quality checks to verify assumptions such as uniqueness, valid status values, timestamp consistency, and referential integrity.

Silver data is stored using Delta Lake.

---

# Delta Lake

Delta Lake provides reliability and transactional capabilities on top of the data lake.

The project implements several Delta Lake features.

## MERGE / Upserts

Instead of rewriting the entire Silver dataset whenever records change, Delta `MERGE` operations support:

* Updating existing records
* Inserting new records

Conceptually:

```text
Incoming Data
      |
      v
Compare with Silver
      |
      +---- existing order ----> UPDATE
      |
      +---- new order ---------> INSERT
```

This provides a foundation for incremental data processing.

---

## Schema Enforcement

Delta Lake validates incoming data against the target table schema.

Incompatible writes can be rejected rather than silently introducing unexpected structural changes.

This protects downstream consumers from accidental schema changes.

---

## Schema Evolution

When a legitimate schema change is required, Delta Lake can evolve the table schema.

Schema evolution was tested by introducing an additional column and allowing the Delta table to update its schema explicitly.

This demonstrates the difference between:

```text
Unexpected schema change
        ↓
Rejected

Approved schema evolution
        ↓
Accepted
```

---

## Delta Table History

Delta Lake maintains transaction history for table operations.

The project uses Delta history to inspect previous table versions and operations such as:

* WRITE
* MERGE
* RESTORE

This provides visibility into how a table has changed over time.

---

## Time Travel

Historical Delta table versions can be queried without manually maintaining separate copies of every dataset.

Historical data can be accessed using:

* Version numbers
* Timestamps

Conceptually:

```text
Current Silver Table
       |
       +---- Version N
       +---- Version N-1
       +---- Version N-2
```

This is useful for debugging, auditing, and investigating data changes.

---

## Restore

Delta tables can be restored to a previous valid version.

The project demonstrates restoring Silver data after modifying a record and confirming that its previous value is recovered.

This provides an additional recovery mechanism when incorrect data reaches a Delta table.

---

# Change Data Feed (CDF)

Delta Lake Change Data Feed is used to identify records that changed between table versions.

Instead of repeatedly processing the complete Silver dataset, CDF exposes relevant changes such as:

```text
insert

update_preimage

update_postimage

delete
```

This allows downstream processing to focus on changed records.

Conceptually:

```text
Silver Delta Table
       |
       | Change Data Feed
       v
Changed Records Only
       |
       v
Incremental Processing
```

---

# Incremental Processing

The pipeline implements incremental processing so downstream calculations do not require a complete rebuild every time source data changes.

The general flow is:

```text
Bronze
   |
   v
Silver Delta
   |
   | CDF
   v
Changed Records
   |
   v
Calculate Metric Deltas
   |
   v
MERGE
   |
   v
Gold
```

This demonstrates an important production data-engineering pattern:

> Process the data that changed rather than unnecessarily reprocessing the complete historical dataset.

---

# Gold Layer

The Gold layer contains business-ready analytical aggregates derived from Silver data.

Gold models are designed for downstream reporting and analytical consumption rather than raw operational processing.

One implemented Gold dataset provides daily delivered-order metrics.

Its grain is:

```text
one row per order_date
```

Metrics include:

* Total delivered orders
* Total delivered value
* Average delivered order value

Conceptually:

```text
Silver Orders
      |
      | CDF
      v
Changed Orders
      |
      v
Daily Metric Changes
      |
      v
Gold Daily Metrics
```

Incremental Gold processing applies metric changes to existing Gold records using Delta `MERGE`.

This allows inserts, updates, and relevant source changes to be reflected without rebuilding the complete Gold table.

---

# Data Modelling and Grain

Maintaining the correct dataset grain is an important design decision throughout the project.

Examples:

```text
orders       → one row per order

order_items  → one row per order item

Gold daily metrics → one row per order date
```

One-to-many datasets such as payments and order items are aggregated before being joined to order-level datasets.

This prevents accidental row multiplication and incorrect business metrics.

---

# Error Handling and Logging

The local PySpark implementation uses structured logging to record:

* Ingestion activity
* Data-quality warnings
* Pipeline failures
* Successful completion

The main pipeline uses exception handling:

```text
try
   |
   v
Execute pipeline

except
   |
   v
Log failure
   |
   v
Re-raise exception

finally
   |
   v
Stop Spark safely
```

This ensures Spark resources are cleaned up even when pipeline execution fails.

---

# Engineering Decisions

## Explicit Schemas

Spark schemas are explicitly defined instead of relying entirely on automatic schema inference.

This provides greater control over expected source data types.

---

## Separation of Concerns

The local implementation separates responsibilities across reusable modules:

```text
ingestion.py       → reading data
validation.py      → data-quality checks
transformation.py  → row-level transformations
curation.py        → aggregation and curated datasets
writer.py          → output writing
config.py          → configuration and paths
main.py            → orchestration
```

This makes the codebase easier to maintain, test, and extend.

---

## Preserve Raw Data

Bronze data is kept close to its source representation rather than performing destructive transformations immediately.

Cleaning and business logic are applied downstream.

This makes source data easier to trace and reprocess.

---

## Fail Fast on Critical Problems

Structural issues such as unexpected duplicate business keys and broken referential relationships can invalidate downstream metrics.

These problems are therefore treated differently from non-critical source anomalies.

---

## Delta Instead of Full Rewrites

Delta Lake provides transactional tables and supports incremental updates through `MERGE`.

This avoids unnecessary full-table rewrites when only a subset of records has changed.

---

## Incremental Gold Processing

Gold aggregates are updated from changes in Silver rather than rebuilding the complete analytical table after every source modification.

This architecture is more representative of production data-engineering workloads where historical datasets can become too large to reprocess unnecessarily.

---

# Local Pipeline Execution

The original local implementation can be run independently.

## 1. Clone the repository

```bash
git clone git@github.com:anishdhakal54/olist-pyspark-etl.git
cd olist-pyspark-etl
```

## 2. Create a virtual environment

```bash
python -m venv .venv
source .venv/bin/activate
```

## 3. Install Dependencies

Install PySpark and the required Python dependencies.

## 4. Add Olist Data

Place the source CSV files under:

```text
data/raw/
```

## 5. Run the Pipeline

```bash
python src/main.py
```

The local curated outputs are written under:

```text
data/curated/
```

---

# Project Evolution

The project has progressed through several stages.

```text
Stage 1
Local PySpark ETL
        |
        v
Stage 2
Modular Validation + Transformation
        |
        v
Stage 3
Azure Data Lake Storage
        |
        v
Stage 4
Azure Databricks
        |
        v
Stage 5
Bronze / Silver / Gold
        |
        v
Stage 6
Delta Lake
        |
        v
Stage 7
MERGE + Schema Management
        |
        v
Stage 8
CDF + Incremental Processing
        |
        v
Stage 9
Incremental Gold Aggregations
```

This progression demonstrates the evolution of the project from a local ETL application into a cloud-based lakehouse data pipeline.

---

# Key Learning Outcomes

This project demonstrates practical experience with:

* Building end-to-end PySpark data pipelines
* Working with multiple related datasets
* Designing reusable ingestion and transformation logic
* Defining explicit Spark schemas
* Implementing data-quality validation
* Performing referential-integrity checks
* Understanding dataset grain
* Handling one-to-many relationships safely
* Performing Spark joins and aggregations
* Using Azure Data Lake Storage Gen2
* Processing cloud data with Azure Databricks
* Designing Bronze, Silver, and Gold layers
* Working with Delta Lake
* Performing Delta `MERGE` operations
* Understanding schema enforcement
* Implementing controlled schema evolution
* Inspecting Delta transaction history
* Using Delta time travel
* Restoring previous Delta versions
* Working with Change Data Feed
* Designing incremental pipelines
* Incrementally maintaining Gold aggregates
* Using Git and GitHub throughout project development

---

# Current Development

The core Azure Databricks pipeline is now implemented through the Gold layer.

The next stage of the project focuses on improving transformation management, orchestration, testing, security, and deployment practices.

---

# Planned Improvements

The next development stages include:

### dbt

Introduce dbt for structured SQL-based transformation modelling, including:

* Models
* Sources
* `source()`
* `ref()`
* Tests
* Documentation
* A small dbt implementation using the Olist pipeline

### Apache Airflow

Introduce orchestration to manage pipeline execution and dependencies.

### Automated Testing and Data Quality

Expand automated validation and testing across pipeline layers.

### Security and Secrets Management

Strengthen cloud security practices using:

* Azure Managed Identity
* Azure Key Vault / secret management

### CI/CD

Introduce automated validation and deployment workflows using Git and CI/CD practices.

### Monitoring

Add improved operational monitoring, pipeline metrics, and alerting.

---

# Author

**Anish Dhakal**

Data Engineering Portfolio Project
