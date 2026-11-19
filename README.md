# Olist E-commerce Data Engineering Pipeline

An end-to-end data engineering portfolio project built with **Python, PySpark, Apache Airflow, Azure Databricks, ADLS Gen2, Delta Lake, Docker, pytest, GitHub Actions, and Databricks Asset Bundles**.

The project started as a local PySpark ETL application and evolved into an Azure lakehouse pipeline designed around incremental processing, safe reprocessing, late-arriving data, corrected records, data-quality controls, failure recovery, CI/CD, and operational observability.

---

## Project Overview

The pipeline processes the Brazilian Olist e-commerce dataset through a Bronze → Silver → Gold architecture.

The final implementation focuses on a practical engineering problem:

> How can a batch data pipeline be safely rerun, backfilled, monitored, and recovered when source data arrives late, records are corrected, or individual processing stages fail?

The project includes:

- Azure Data Lake Storage Gen2
- Azure Databricks
- PySpark
- Delta Lake
- Bronze / Silver / Gold architecture
- Incremental daily processing
- Late-arriving data handling
- Corrected-record handling
- Idempotent reprocessing
- Historical backfills
- Delta `MERGE`
- Change Data Feed
- Data-quality validation
- Quarantine handling
- Data-quality failure thresholds
- Task-level retries
- Selective failure recovery
- Apache Airflow orchestration
- Pipeline run metrics
- GitHub Actions CI/CD
- Databricks Asset Bundles
- Manual production approval
- Managed Identity / Unity Catalog access

A local PySpark implementation is also retained in the repository for modular transformation development and automated testing.

---

# Architecture

```text
                         Apache Airflow
                              |
                              | processing_start
                              | processing_end
                              v
                    Azure Databricks Job
                              |
              +---------------+---------------+
              |                               |
              v                               |
     Azure Data Lake Storage                  |
         raw / raw_simulated                  |
              |                               |
              v                               |
        +-----------+                         |
        |  BRONZE   |                         |
        |-----------|                         |
        | Raw data  |                         |
        | Metadata  |                         |
        | Processing|                         |
        | windows   |                         |
        +-----+-----+                         |
              |                               |
              v                               |
        +-----------+                         |
        |  SILVER   |                         |
        |-----------|                         |
        | Validate  |                         |
        | Aggregate |                         |
        | Enrich    |                         |
        | Deduplicate                         |
        | MERGE     |                         |
        +-----+-----+                         |
              |                               |
       +------+-------+                       |
       |              |                       |
       v              v                       |
 Accepted Silver   Quarantine                 |
       |          invalid records             |
       |                                      |
       v                                      |
 +-----------+                                |
 |   GOLD    |                                |
 |-----------|                                |
 | Daily     |                                |
 | business  |                                |
 | metrics   |                                |
 +-----------+                                |
                                              |
                       +----------------------+
                       |
                       v
                Run Metrics Delta Table
```

The orchestration and processing responsibilities are deliberately separated:

- **Airflow** controls scheduling and the processing window.
- **Databricks Jobs** execute the Bronze → Silver → Gold workflow.
- **Delta Lake** provides transactional writes, merges, history, and Change Data Feed.
- **GitHub Actions + Databricks Asset Bundles** deploy tested code to Databricks.

---

# Technology Stack

| Category | Technologies |
|---|---|
| Programming | Python, SQL |
| Processing | PySpark, Spark SQL |
| Cloud | Microsoft Azure |
| Compute | Azure Databricks |
| Storage | ADLS Gen2 |
| Table Format | Delta Lake |
| Orchestration | Apache Airflow |
| Containers | Docker, Docker Compose |
| Testing | pytest |
| CI/CD | GitHub Actions |
| Deployment | Databricks Asset Bundles |
| Security | Managed Identity, Unity Catalog |
| Version Control | Git, GitHub |

---

# Dataset

The project uses the **Olist Brazilian E-Commerce Public Dataset**.

The source contains information about:

- Orders
- Customers
- Order items
- Payments
- Products
- Sellers
- Reviews
- Geolocation
- Product-category translations

The dataset contains realistic one-to-many relationships and data-quality anomalies, making it useful for practising:

- Dataset grain management
- Referential integrity
- Timestamp validation
- Joins and aggregations
- Incremental processing
- Late-arriving records
- Corrected records

The complete orders dataset contains approximately **99,000 orders**.

For controlled incremental-pipeline validation, a smaller historical period is replayed as daily source arrivals.

---

# Repository Structure

```text
olist-pyspark-etl/
│
├── .github/
│   └── workflows/
│       ├── ci.yml
│       └── cd.yml
│
├── airflow/
│   ├── dags/
│   │   ├── olist_local_pipeline_dag.py
│   │   ├── olist_azure_pipeline_dag.py
│   │   └── experiments/
│   ├── Dockerfile
│   └── docker-compose.yaml
│
├── databricks/
│   ├── 00_prepare_simulated_source.py
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_processing.py
│   ├── 03_gold.py
│   ├── pipeline.py
│   └── experiments/
│       └── validate_incremental_pipeline.py
│
├── pipeline/
│   ├── bronze.py
│   ├── silver.py
│   └── gold.py
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
├── tests/
│   ├── test_bronze_metadata.py
│   ├── test_curation.py
│   ├── test_olist_azure_dag.py
│   ├── test_olist_dag.py
│   ├── test_pipeline_integration.py
│   ├── test_transformation.py
│   └── test_validation.py
│
├── databricks.yml
├── requirements.txt
└── README.md
```

---

# Bronze Layer

Bronze ingests source data from ADLS Gen2 and persists it as Delta.

The orders pipeline records ingestion and processing metadata including:

```text
_ingestion_timestamp
_source_file
_processing_start
_processing_end
```

The processing-window metadata is important because the business timestamp of an order is not necessarily the date on which the pipeline receives it.

This allows the pipeline to distinguish:

```text
When the event happened
        ≠
When the pipeline received it
```

For orders, daily source batches are read using the requested processing date.

Writes are constrained to the processing window using Delta `replaceWhere`, allowing a historical window to be safely reprocessed without appending duplicate copies of the same batch.

---

# Silver Layer

Silver creates an order-level analytical dataset by combining:

- Orders
- Customers
- Aggregated order items
- Aggregated payments

The intended grain is:

```text
One row per order
```

One-to-many datasets are aggregated before joining to avoid accidental row multiplication.

Example metrics include:

- Item count
- Total item price
- Freight value
- Total payment value
- Payment count

---

## Corrected Records

Incremental systems cannot assume that the first version of a record is always final.

Bronze therefore retains processing metadata and Silver resolves multiple versions of the same `order_id` using a Spark window ordered by `_processing_start`.

Conceptually:

```text
order_id = X
│
├── version received Nov 26
│
└── corrected version received Nov 28
             |
             v
      Keep latest version
             |
             v
         Silver
```

A controlled test order was processed twice with a corrected estimated-delivery date.

Validation confirmed:

```text
Bronze versions: 2
Silver versions: 1
```

The Silver version contained the corrected value from the latest processing window.

---

# Late-Arriving Data

The simulated daily source includes records whose business event date is earlier than their actual arrival date.

Five orders originally associated with November 25 were deliberately delivered to the pipeline on November 27.

The pipeline successfully processed them when they arrived rather than assuming that source arrival always matches the business date.

Validation confirmed:

```text
Bronze late arrivals: 5
Silver late arrivals: 5
```

This separates:

```text
Event time
```

from:

```text
Processing / arrival time
```

which is an important requirement for reliable incremental pipelines.

---

# Historical Backfills

Airflow supplies explicit processing windows to the Databricks job:

```text
processing_start
processing_end
```

Historical dates can therefore be replayed individually.

A controlled backfill processed:

```text
2017-11-24
2017-11-25
2017-11-26
2017-11-27
2017-11-28
```

Each window was processed independently through:

```text
Bronze → Silver → Gold
```

The same processing contract is used for both scheduled execution and historical reprocessing.

---

# Idempotent Reprocessing

A production pipeline must tolerate rerunning the same processing window.

The November 28 window was deliberately executed again after the original successful run.

Before rerun:

```text
Nov 28 Bronze rows:          381
Nov 28 distinct order IDs:   381
Total Bronze rows:         2,850
```

After rerun:

```text
Nov 28 Bronze rows:          381
Nov 28 distinct order IDs:   381
Total Bronze rows:         2,850
```

The rerun therefore did not duplicate the daily batch.

Idempotency is achieved through processing-window-aware Bronze replacement and business-key-aware Silver processing.

---

# Data Quality

Silver performs several validation checks before accepted records are written.

Checks include:

- Duplicate order IDs
- Orphan customer references
- Invalid approval timestamps
- Invalid delivery chronology
- Invalid order statuses

Not every data-quality problem is treated in the same way.

Structural/integrity problems such as unexpected duplicates or broken customer references cause the pipeline to fail.

Known source-quality problems that can be isolated safely can instead be quarantined.

---

# Quarantine Handling

Orders with impossible delivery chronology:

```text
order_delivered_customer_date
<
order_delivered_carrier_date
```

are excluded from accepted Silver data.

They receive a reason:

```text
_quarantine_reason =
delivery_before_carrier_timestamp
```

and are persisted separately in a Delta quarantine table.

This prevents invalid business records from contaminating the accepted analytical dataset while preserving them for investigation.

If a previously valid order later receives a corrected version that is invalid, the existing accepted version is removed from Silver so stale data is not retained.

---

## Data-Quality Threshold

The pipeline calculates the proportion of records requiring quarantine.

A configurable threshold is currently:

```text
5%
```

Behaviour:

```text
quarantine rate <= 5%
        |
        v
Pipeline may continue

quarantine rate > 5%
        |
        v
Pipeline fails
```

The quarantine data is persisted before the threshold failure so rejected records remain available for investigation.

Critical structural validation failures continue to fail independently of the quarantine threshold.

---

# Quarantine Validation

The quarantine implementation was tested using a controlled Silver record.

A valid delivered order was modified in an isolated experiment so that:

```text
customer delivery timestamp
<
carrier delivery timestamp
```

The test confirmed:

```text
Quarantined records: 1
Quarantine reason:
delivery_before_carrier_timestamp
```

The quarantine record was then written to an isolated Delta path and merged again using the same `order_id`.

After rerun:

```text
Rows:               1
Distinct order IDs: 1
```

This verified that quarantine persistence is idempotent.

The experiment cleans up its temporary Delta data after validation.

---

# Gold Layer

Gold contains business-facing analytical aggregates.

The implemented dataset contains daily delivered-order metrics including:

- Delivered order count
- Total delivered value
- Average delivered order value

The intended grain is:

```text
One row per order date
```

Delta Lake is used for persistent Gold storage.

---

# Delta Lake

The project uses Delta Lake for transactional lakehouse processing.

Implemented or explored features include:

### MERGE

Used to update existing business keys and insert new records.

```text
Incoming record
      |
      v
Does key exist?
   /       \
 YES       NO
  |         |
UPDATE    INSERT
```

### Change Data Feed

CDF is enabled on Silver to expose row-level changes for potential downstream incremental consumers. The current Gold layer does not consume CDF; it rebuilds from the complete accepted Silver state so corrections, status changes, and removed aggregate keys are reflected deterministically.

### Table History

Delta transaction history can be inspected to understand previous writes and merges.

### Time Travel

Historical table versions can be queried for debugging and investigation.

### Restore

The project tested modifying Delta data and restoring an earlier table version.

### Schema Enforcement

Delta protects target datasets against incompatible writes.

---

# Apache Airflow Orchestration

Airflow runs locally in Docker and acts as the high-level orchestrator for the Azure pipeline.

The Azure DAG triggers the Databricks production job rather than running Spark processing itself.

Conceptually:

```text
Airflow
   |
   | daily processing window
   v
Databricks Job
   |
   +--> Bronze
   |
   +--> Silver
   |
   +--> Gold
```

This keeps orchestration responsibility separate from Spark execution.

Airflow owns:

- Scheduling
- Processing-window generation
- Databricks job triggering
- High-level execution visibility

Databricks owns:

- Spark execution
- Bronze → Silver → Gold dependencies
- Task retries
- Delta processing

---

# Retry and Failure Recovery

Databricks tasks are configured with task-level retries.

The pipeline was deliberately tested with a controlled Silver failure.

Observed execution:

```text
Bronze
  |
  +--> SUCCESS
        |
        v
Silver
  |
  +--> initial attempt FAILED
  +--> retry 1 FAILED
  +--> retry 2 FAILED
        |
        v
Gold SKIPPED
```

After correcting the problem, Databricks repair execution was used.

The repaired run:

```text
Bronze  → not rerun
Silver  → rerun successfully
Gold    → executed successfully
```

This demonstrated selective recovery rather than unnecessarily repeating successful upstream work.

---

# Pipeline Observability

Successful Silver executions persist operational metrics to a Delta table:

```text
silver/run_metrics
```

Each execution records:

| Metric | Description |
|---|---|
| `processing_start` | Start of processing window |
| `processing_end` | End of processing window |
| `input_orders` | Orders received for the run |
| `silver_orders` | Accepted Silver orders for the run |
| `quarantined_orders` | Orders rejected to quarantine |
| `quarantine_rate` | Proportion of input orders quarantined |
| `recorded_at` | Metrics recording timestamp |

For the validated November 28 execution:

```text
Input orders:          381
Accepted Silver:       381
Quarantined:             0
Quarantine rate:         0%
```

Metrics are append-only so repeated executions of the same processing window remain visible as separate operational events.

Databricks Jobs and Airflow provide failure/retry visibility, while the Delta metrics table provides persistent data-processing metrics.

---

# CI/CD

The repository uses GitHub Actions for Continuous Integration and Continuous Delivery.

## Continuous Integration

Changes are developed through feature branches and Pull Requests.

CI runs automated checks including:

- PySpark tests
- Transformation tests
- Validation tests
- Airflow DAG tests

Conceptually:

```text
Feature branch
      |
      v
Pull Request
      |
      v
GitHub Actions CI
      |
   +--+--+
   |     |
pytest  Airflow tests
   |     |
   +--+--+
      |
      v
Merge to main
```

The protected `main` branch requires the configured checks to pass before integration.

---

# Production Deployment

After successful CI on `main`, the CD workflow prepares the exact tested commit for production deployment.

The workflow includes:

1. Verify successful CI
2. Verify repository origin
3. Verify `main` branch
4. Check out the tested commit SHA
5. Verify commit integrity
6. Wait for manual production approval
7. Validate the Databricks Asset Bundle
8. Deploy the bundle to the production Databricks workspace

Production deployment is therefore **real rather than simulated**.

Databricks Asset Bundles define the deployed jobs, notebooks, task dependencies, compute configuration, parameters, and retry behaviour.

---

# Deployment Safety

The CD process deliberately separates testing from production release.

```text
Pull Request
     |
     v
    CI
     |
     v
Merge to main
     |
     v
CD preparation
     |
     v
Verify tested SHA
     |
     v
Production approval
     |
     v
Databricks bundle validation
     |
     v
Production deployment
```

The production GitHub environment requires manual approval before deployment proceeds.

This reduces the chance of deploying unreviewed or untested code.

---

# Security

ADLS access is configured using Azure identity rather than embedding storage credentials in pipeline code.

The Azure implementation uses:

- Azure Databricks
- Unity Catalog
- Managed Identity / Access Connector
- ADLS Gen2 permissions
- Unity Catalog storage credential
- External location configuration

The Databricks environment can therefore access the required ADLS paths without hard-coded storage-account keys in the processing notebooks.

Deployment credentials are stored outside source code using GitHub environment secrets.

---

# Automated Testing

The project uses `pytest` for automated pipeline testing.

Tests cover areas including:

- Transformation functions
- Delivery-duration calculations
- Date attributes
- Late-delivery classification
- Validation logic
- Bronze metadata
- Pipeline integration
- Airflow DAG structure
- Azure Airflow DAG configuration

Transformation tests generally follow:

```text
ARRANGE
Create controlled input
      |
      v
ACT
Execute transformation
      |
      v
ASSERT
Compare actual and expected output
```

Controlled Databricks validation notebooks are also used for integration behaviour that cannot be meaningfully proven through local unit tests alone.

---

# Engineering Decisions

## Spark on a Small Dataset

Olist is not large enough to economically require Spark.

A simpler technology such as SQL, pandas, or DuckDB would be sufficient for this data volume.

Spark was intentionally used because the purpose of the project is to practise the architecture and operational patterns used in larger distributed data platforms:

- Incremental processing
- Delta Lake
- Distributed transformations
- Backfills
- Idempotency
- Orchestration
- Failure recovery
- Data-quality controls

The project therefore demonstrates engineering patterns rather than claiming that Spark is necessary for a ~100 MB dataset.

---

## Processing Time vs Event Time

Business timestamps cannot reliably identify when data became available to the pipeline.

The design therefore stores explicit processing-window metadata.

This enables:

- Late-arriving data
- Historical backfills
- Corrected records
- Deterministic reruns

---

## Preserve Bronze History

Bronze retains multiple versions of corrected business records.

Silver is responsible for resolving those versions into the current accepted state.

This preserves traceability instead of silently destroying earlier source versions.

---

## Aggregate Before Joining

Order items and payments are one-to-many relationships.

They are aggregated to order grain before being joined to orders.

This prevents accidental row multiplication and incorrect business metrics.

---

## Fail vs Quarantine

Not all bad data should be treated equally.

The pipeline distinguishes between:

**Pipeline/integrity failures**

Examples:

- Unexpected duplicate business keys
- Broken customer references
- Invalid statuses

These can indicate an engineering or structural problem and cause the pipeline to fail.

**Isolatable source-quality issues**

Example:

- Impossible delivery chronology

These records can be quarantined while valid records continue through the pipeline, provided the quarantine rate remains within the configured threshold.

---

## Append Operational Metrics

Run metrics are intentionally append-only.

A rerun is itself an operational event and should remain visible rather than replacing evidence of the previous execution.

---

## Retry Ownership

Airflow is kept as the high-level orchestrator while Databricks owns retries for individual Spark tasks.

This avoids duplicating retry behaviour across orchestration layers.

---

# Problems Encountered

The final architecture was influenced by failures discovered while implementing real backfills and reruns.

Examples included:

- Expired Airflow Databricks authentication
- Missing processing-date variables
- Incorrect Delta `replaceWhere` conditions
- Processing windows where start and end were accidentally equal
- Duplicate processing metadata after Silver joins
- Confusion between manually edited workspace notebooks and bundle-deployed production code

These failures exposed assumptions that were not obvious during simple one-off execution.

The resulting fixes introduced:

- Explicit processing-window contracts
- Deterministic daily source selection
- Idempotent window replacement
- Metadata-aware Silver processing
- Latest-record resolution
- Controlled deployment through Databricks Asset Bundles
- Retry and repair validation

A major lesson from the project was that making a pipeline **safely reprocessable** required more engineering than making it run successfully once.

---

# Incremental Pipeline Validation

The final controlled validation demonstrates three important behaviours.

### Late arrivals

```text
Bronze late arrivals: 5
Silver late arrivals: 5
PASS
```

### Corrected record

```text
Bronze versions: 2
Silver versions: 1
Corrected version selected
PASS
```

### Idempotent rerun

```text
Nov 28 rows:              381
Nov 28 distinct IDs:      381
Total Bronze rows:      2,850
PASS
```

Additional controlled validation proves quarantine classification and idempotent Delta quarantine persistence.

---

# Local PySpark Implementation

The repository also contains the original local PySpark implementation.

It demonstrates:

- Explicit schemas
- Modular ingestion
- Validation
- Reusable transformations
- Order-level curation
- Parquet output
- pytest
- Local Airflow orchestration
- Docker-based Airflow infrastructure

The local implementation remains useful for fast development and unit testing, while the Azure implementation represents the final cloud pipeline architecture.

---

# Running Tests

From the repository root:

```bash
pytest
```

Individual test groups can also be executed during development.

CI runs the required automated checks for Pull Requests and changes targeting `main`.

---

# Deployment Model

Normal production deployment follows:

```text
Code change
   |
   v
Feature branch
   |
   v
Pull Request
   |
   v
CI
   |
   v
Merge to main
   |
   v
CD
   |
   v
Manual production approval
   |
   v
Databricks Asset Bundle deployment
```

Direct CLI execution is primarily used for controlled backfills, validation, and debugging rather than as the normal deployment mechanism.

---

# Project Status

| Capability | Status |
|---|---|
| Local PySpark ETL | Completed |
| Automated PySpark tests | Completed |
| Docker Airflow environment | Completed |
| Azure Data Lake Storage | Completed |
| Azure Databricks | Completed |
| Bronze / Silver / Gold | Completed |
| Delta Lake | Completed |
| Unity Catalog / Managed Identity | Completed |
| Airflow → Databricks orchestration | Completed |
| Processing-window parameterisation | Completed |
| Historical backfills | Completed |
| Late-arriving data handling | Completed |
| Corrected-record handling | Completed |
| Idempotent reprocessing | Completed |
| Delta MERGE | Completed |
| Change Data Feed | Completed |
| Task retries | Completed |
| Selective repair / recovery | Completed |
| Data-quality quarantine | Completed |
| Quarantine-rate threshold | Completed |
| Pipeline run metrics | Completed |
| GitHub Actions CI | Completed |
| Production approval gate | Completed |
| Databricks Asset Bundle deployment | Completed |

---

# Key Learning Outcomes

This project provided hands-on experience with:

- Designing an Azure lakehouse pipeline
- Building PySpark ETL workloads
- Managing Bronze, Silver, and Gold layers
- Working with Delta Lake
- Designing incremental processing windows
- Separating event time from processing time
- Handling late-arriving records
- Resolving corrected records
- Building idempotent pipelines
- Running historical backfills
- Using Delta `MERGE`
- Using Change Data Feed
- Managing dataset grain
- Implementing referential-integrity checks
- Designing fail-vs-quarantine data-quality behaviour
- Building quarantine thresholds
- Testing failure and recovery scenarios
- Using Databricks task retries and repair runs
- Orchestrating Databricks through Airflow
- Persisting operational pipeline metrics
- Testing PySpark code with pytest
- Building CI/CD with GitHub Actions
- Deploying with Databricks Asset Bundles
- Protecting production deployment with manual approval
- Accessing ADLS through Managed Identity and Unity Catalog

The most important lesson was that a reliable pipeline is not defined only by whether it can process data successfully.

It also needs to answer:

```text
Can it be rerun safely?
Can it process historical windows?
Can it handle late data?
Can it handle corrected data?
Can it isolate bad records?
Can it recover without rerunning successful work?
Can we see what each run processed?
Can the exact tested code be deployed consistently?
```

Those questions drove the final design of the project.

---

# Future Improvements

The project is intentionally considered complete at its current scope.

Possible future extensions, rather than requirements for completion, include:

- Alerting on failed runs or quality thresholds
- Execution-duration metrics
- Dashboarding over pipeline run metrics
- dbt-based SQL modelling
- Larger synthetic datasets for performance testing
- Dedicated dev/staging/prod environments
- Streaming ingestion

These are deliberately left outside the current project to avoid adding complexity without demonstrating substantially new engineering concepts.

---

# Author

**Anish Dhakal**

Data Engineering Portfolio Project

[GitHub](https://github.com/anishdhakal54) | [LinkedIn](https://www.linkedin.com/in/anish-dhakal54)

**Repository:** [olist-pyspark-etl](https://github.com/anishdhakal54/olist-pyspark-etl)