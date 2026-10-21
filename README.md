
# Olist E-commerce Data Engineering Pipeline

An end-to-end data engineering portfolio project built using **Python, PySpark, Apache Airflow, Docker, Azure Databricks, Delta Lake, pytest, and GitHub Actions**.

The project demonstrates how to design, build, orchestrate, test, and prepare data pipelines for deployment using modern data engineering practices.

## Project Overview

This project processes the Olist Brazilian e-commerce dataset to build reliable, reusable, and analytics-ready data pipelines.

The project includes two complementary implementations:

**1. Local PySpark Pipeline**

A modular ETL application that ingests, validates, transforms, aggregates, and writes e-commerce data using PySpark and Parquet.

The local implementation includes:

- Modular Python pipeline components
- Explicit Spark schemas
- Data quality and referential integrity validation
- Reusable PySpark transformations
- Bronze-layer ingestion
- Curated analytical datasets
- Apache Airflow orchestration
- Docker-based Airflow environment
- Automated tests using pytest
- GitHub Actions CI/CD

**2. Azure Lakehouse Pipeline**

A cloud-based data engineering implementation using:

- Azure Data Lake Storage Gen2
- Azure Databricks
- PySpark and Spark SQL
- Delta Lake
- Bronze, Silver, and Gold architecture
- Delta MERGE operations
- Schema enforcement and evolution
- Delta table history and time travel
- Restore operations
- Change Data Feed (CDF)
- Incremental Gold processing

The Azure implementation demonstrates lakehouse processing and incremental data engineering patterns. The local implementation focuses on modularity, orchestration, testing, and deployment engineering.

**Current deployment status:** GitHub Actions CI is operational, and CD includes manual production approval and commit verification. The production deployment action is currently simulated; automated deployment to Azure has not yet been implemented.

---

## Architecture

### Local Pipeline Architecture

```text
Olist CSV Dataset
        |
        v
+--------------------------+
|     Data Ingestion       |
|     PySpark              |
+------------+-------------+
             |
             v
+--------------------------+
|      Bronze Layer        |
|      Parquet Storage     |
+------------+-------------+
             |
             v
+--------------------------+
|    Data Validation       |
|    Quality Checks        |
+------------+-------------+
             |
             v
+--------------------------+
|    Transformations       |
|    Joins / Enrichment    |
+------------+-------------+
             |
             v
+--------------------------+
|    Aggregation           |
|    Curated Datasets      |
+------------+-------------+
             |
             v
+--------------------------+
|    Parquet Outputs       |
+--------------------------+

Orchestration: Apache Airflow
Execution: Docker / Local PySpark
Testing: pytest
CI/CD: GitHub Actions
```

### Azure Lakehouse Architecture

```text
Olist CSV Dataset
        |
        v
Azure Data Lake Storage Gen2
        |
        v
+--------------------------+
|          BRONZE          |
| Raw Data                 |
| Ingestion Metadata       |
+------------+-------------+
             |
             | PySpark
             v
+--------------------------+
|          SILVER          |
| Cleaned Data             |
| Validated Data           |
| Delta Tables             |
+------------+-------------+
             |
             | Change Data Feed
             | Incremental Processing
             v
+--------------------------+
|           GOLD           |
| Business Metrics         |
| Analytical Aggregates    |
| Delta MERGE              |
+--------------------------+
```

The two implementations demonstrate both local pipeline engineering and cloud lakehouse development.

---

## Technology Stack

| Category | Technologies |
|---|---|
| Programming | Python, SQL |
| Data Processing | PySpark, Spark SQL, Spark DataFrame API |
| Cloud | Microsoft Azure, Azure Databricks, ADLS Gen2 |
| Storage | Parquet, Delta Lake |
| Orchestration | Apache Airflow |
| Containers | Docker, Docker Compose |
| Testing | pytest, PySpark testing |
| CI/CD | GitHub Actions |
| Version Control | Git, GitHub |
| Development | Ubuntu Linux, Jupyter Notebook |
| Airflow Infrastructure | PostgreSQL, Redis |

---

## Dataset

The project uses the **Olist Brazilian E-Commerce Public Dataset**, which contains information about orders, customers, products, sellers, payments, reviews, and deliveries.

Nine datasets are used:

| Dataset | Records |
|---|---:|
| Orders | 99,441 |
| Customers | 99,441 |
| Order Items | 112,650 |
| Order Payments | 103,886 |
| Order Reviews | 104,162 |
| Products | 32,951 |
| Sellers | 3,095 |
| Geolocation | 1,000,163 |
| Product Category Translation | 71 |

The datasets provide realistic examples of:

- One-to-many relationships
- Referential integrity
- Data quality anomalies
- Date and timestamp transformations
- Joins and aggregations
- Dataset grain management
- Incremental processing

---

## Project Structure

The repository contains modular PySpark code, Airflow configuration, automated tests, and GitHub Actions workflows.

```text
olist-pyspark-etl/
|
├── .github/
│   └── workflows/
│       ├── ci.yml
│       └── cd.yml
|
├── airflow/
│   └── dags/
│       ├── olist_local_pipeline_dag.py
│       └── olist_pipeline_experiment_dag.py
|
├── data/
│   ├── raw/
│   ├── bronze/
│   └── curated/
|
├── notebooks/
│   └── 01_data_ingestion.ipynb
|
├── src/
│   ├── config.py
│   ├── ingestion.py
│   ├── bronze.py
│   ├── validation.py
│   ├── transformation.py
│   ├── curation.py
│   ├── writer.py
│   └── main.py
|
├── tests/
├── requirements.txt
├── .gitignore
└── README.md
```

The tree is a simplified representation of the project. Docker configuration, supporting files, and test modules are maintained in their respective directories.

---

# Local PySpark Pipeline

## 1. Data Ingestion

The ingestion layer reads source CSV files using PySpark.

Explicit schemas are used to control data types and reduce unexpected behaviour caused by automatic schema inference.

The ingestion logic is separated from transformations and validation so that components can be reused and tested independently.

## 2. Bronze Layer

The local Bronze layer preserves ingested data in Parquet format.

Bronze processing is implemented as a reusable pipeline component.

The purpose of Bronze is to maintain source-level data that can be used for downstream transformations and validation.

## 3. Data Quality Validation

Data quality is an important part of the pipeline.

Implemented validation concepts include:

- Duplicate business-key checks
- Null analysis
- Timestamp consistency
- Referential integrity
- Invalid business values
- Structural validation

### Referential Integrity

PySpark left anti joins are used to identify missing relationships between datasets.

Examples:

```text
orders.customer_id
        |
        v
customers.customer_id

order_items.order_id
        |
        v
orders.order_id
```

During validation, the following relationships were checked:

- Orders without matching customers: 0
- Order items without matching orders: 0

### Source Data Anomalies

The dataset includes real-world inconsistencies.

Examples identified during analysis include:

- Missing approval timestamps
- Missing delivery timestamps
- Delivery timestamps occurring before carrier timestamps
- Purchase timestamps occurring after approval timestamps

The pipeline distinguishes between critical structural problems and source-data anomalies that should be investigated.

---

## 4. Data Transformations

The transformation layer uses reusable PySpark functions.

### Delivery Duration

Calculates the number of days between order purchase and customer delivery.

```python
delivery_days = datediff(
    col("order_delivered_customer_date"),
    col("order_purchase_timestamp")
)
```

### Date Attributes

Date-based attributes are extracted for downstream analytics.

Examples include:

- Purchase year
- Purchase month
- Order date

### Late Delivery Indicator

The `is_late` column identifies orders delivered after their estimated delivery date.

Conceptually:

```text
Actual delivery > Estimated delivery
                  |
                  v
              is_late = 1

Actual delivery <= Estimated delivery
                  |
                  v
              is_late = 0
```

The transformation functions are independently testable.

---

## 5. Aggregations and Curation

Order items and payments contain multiple records for some orders.

Joining these datasets directly to orders can multiply rows and produce incorrect business metrics.

To prevent this, related datasets are aggregated to the intended grain before joining.

### Order Item Aggregation

Metrics include:

- Item count
- Total item price
- Total freight value

### Payment Aggregation

Metrics include:

- Total payment value
- Payment count

The curated output maintains the intended order-level structure.

---

# Apache Airflow Orchestration

Apache Airflow is used to orchestrate the local PySpark pipeline.

Airflow runs inside a Docker-based environment with supporting services including PostgreSQL and Redis.

The implementation includes:

- Airflow DAG definitions
- Task execution using BashOperator
- Local PySpark pipeline invocation
- Task execution monitoring
- DAG validation tests

## Pipeline DAG

The local pipeline DAG executes the modular PySpark application.

Example task execution:

```bash
cd /opt/olist && python src/main.py
```

This allows Airflow to coordinate pipeline execution instead of requiring the application to be started manually.

### Why Airflow?

Airflow introduces orchestration concepts used in production data engineering systems:

- Scheduling
- Task dependencies
- Execution monitoring
- Failure visibility
- Workflow automation

The current implementation demonstrates local orchestration rather than managed cloud Airflow deployment.

---

# Automated Testing

Automated testing is implemented using **pytest** and PySpark.

The project tests transformation behaviour using small, controlled input datasets.

Examples include:

- Delivery duration calculations
- Purchase date attributes
- Late delivery classification
- Edge cases in transformation logic
- Airflow DAG validation

## Testing Approach

Tests follow the **Arrange → Act → Assert** pattern.

```text
ARRANGE
Create sample Spark DataFrame
        |
        v
ACT
Execute transformation function
        |
        v
ASSERT
Compare actual and expected values
```

This helps ensure that changes to transformation code do not silently introduce incorrect results.

## Running Tests

From the project root:

```bash
pytest
```

The project currently includes automated PySpark and Airflow-related tests executed through GitHub Actions.

---

# CI/CD with GitHub Actions

The project includes two GitHub Actions workflows:

```text
.github/workflows/ci.yml
.github/workflows/cd.yml
```

## Continuous Integration (CI)

The `Olist CI` workflow runs automated checks when code is pushed or a Pull Request targets `main`.

It includes two jobs:

### PySpark Tests

```text
pytest
```

Executes automated tests for pipeline transformations and related Python functionality.

### Airflow Tests

```text
airflow-tests
```

Validates Airflow-related functionality, including DAG tests.

### CI Flow

```text
Developer creates feature branch
              |
              v
       Code changes
              |
              v
        Push to GitHub
              |
              v
        Pull Request
              |
              v
       GitHub Actions CI
              |
       +------+------+
       |             |
       v             v
    pytest      airflow-tests
       |             |
       +------+------+
              |
              v
        Checks pass
              |
              v
         Merge to main
```

This provides automated validation before changes are integrated into the main branch.

---

## Branch Protection

The `main` branch is protected using GitHub repository rules.

The project uses:

- Pull Requests for changes to `main`
- Required CI status checks
- Up-to-date branch requirements
- Restrictions on force pushes

Required checks include:

```text
pytest
airflow-tests
```

This reduces the risk of untested code being merged into the main branch.

---

## Continuous Delivery (CD)

The `Olist CD` workflow is triggered after the `Olist CI` workflow completes successfully on `main`.

The CD workflow includes:

1. CI success verification
2. Repository-origin verification
3. Main-branch verification
4. Deployment preparation
5. Manual production approval
6. Checkout of the tested commit
7. Commit SHA verification
8. Simulated production deployment

### CD Architecture

```text
       Code merged into main
                 |
                 v
             Olist CI
                 |
                 v
          CI successful?
                 |
          +------+------+
          |             |
         NO            YES
          |             |
          v             v
        STOP       Olist CD
                        |
                        v
               Validate CI source
                        |
                        v
               Prepare deployment
                        |
                        v
               Production approval
                        |
                        v
               Checkout tested SHA
                        |
                        v
               Verify commit SHA
                        |
                        v
               Simulated deployment
```

---

## Deployment Security

The CD workflow checks that the triggering CI run:

- Completed successfully
- Originated from the expected GitHub repository
- Ran against the `main` branch

Example condition:

```yaml
if: >-
  ${{
    github.event.workflow_run.conclusion == 'success' &&
    github.event.workflow_run.head_repository.full_name == github.repository &&
    github.event.workflow_run.head_branch == 'main'
  }}
```

All three conditions must be true before deployment preparation can proceed.

---

## Manual Production Approval

GitHub Environments are used to protect the deployment stage.

The `production` environment requires manual approval before the deployment job starts.

This provides a controlled deployment gate.

```text
CI successful
      |
      v
CD preparation
      |
      v
Waiting for approval
      |
      v
Manual approval
      |
      v
Deployment job
```

---

## Exact Commit Verification

A key feature of the CD workflow is deploying the exact commit associated with the successful CI run.

The deployment job checks out:

```yaml
- name: Checkout tested commit
  uses: actions/checkout@v4
  with:
    ref: ${{ github.event.workflow_run.head_sha }}
```

It then compares the checked-out commit SHA with the expected CI commit SHA.

```bash
actual=$(git rev-parse HEAD)
expected="$EXPECTED_SHA"

if [[ "$actual" != "$expected" ]]; then
  echo "ERROR: Commit SHA mismatch!"
  exit 1
fi

echo "Commit verification passed!"
```

### Verified Execution

A successful GitHub Actions run produced:

```text
Actual commit:
e7affc6cdd68b43789861d80b70eb27ca51673de

Expected commit:
e7affc6cdd68b43789861d80b70eb27ca51673de

Commit verification passed!
```

This confirms that the deployment job checked out the commit associated with the triggering successful CI run.

### Current CD Limitation

The deployment currently executes a simulation:

```bash
echo "Deploying Olist pipeline to production."
```

Actual deployment to Azure Databricks or another production environment is planned.

The workflow therefore demonstrates deployment controls and commit verification, but not a completed cloud release process.

---

# Azure Databricks Lakehouse Implementation

The Azure implementation uses a Medallion Architecture consisting of Bronze, Silver, and Gold layers.

## Bronze Layer

The Bronze layer ingests source data from Azure Data Lake Storage Gen2.

Ingestion metadata includes:

- `_ingestion_timestamp`
- `_source_file`

The layer preserves raw source information for traceability and downstream processing.

## Silver Layer

The Silver layer performs data cleaning, validation, enrichment, and joins.

The main Silver orders dataset combines:

- Order information
- Customer information
- Order item aggregates
- Payment aggregates

The intended grain is:

```text
One row per order
```

The Silver implementation uses Delta Lake and supports Change Data Feed.

## Gold Layer

The Gold layer provides analytical aggregates for downstream reporting.

One implemented Gold dataset contains daily delivered-order metrics.

Metrics include:

- Total delivered orders
- Total delivered value
- Average delivered order value

The intended grain is:

```text
One row per order date
```

Incremental Gold processing uses changes from Silver to update analytical metrics.

---

# Delta Lake Features

The Azure implementation explores Delta Lake functionality commonly used in lakehouse architectures.

## MERGE Operations

Delta `MERGE` supports updating existing records and inserting new records.

```text
Incoming records
        |
        v
Compare with Delta table
        |
        +---- Existing key ---> UPDATE
        |
        +---- New key --------> INSERT
```

This provides a foundation for incremental processing.

## Schema Enforcement

Delta Lake validates incoming data against the target table schema.

Incompatible changes can be rejected to protect downstream consumers.

## Schema Evolution

Controlled schema evolution allows approved structural changes.

The project tested schema evolution by introducing an additional column.

## Delta Table History

Delta Lake records transaction history for operations such as:

- WRITE
- MERGE
- RESTORE

This makes it possible to inspect changes across table versions.

## Time Travel

Historical Delta table versions can be queried using versions or timestamps.

This supports debugging, auditing, and data investigation.

## Restore

Delta tables can be restored to earlier versions.

The project tested modifying a record and restoring a previous table version.

## Change Data Feed

Change Data Feed identifies records modified between Delta table versions.

Change types include:

```text
insert
update_preimage
update_postimage
delete
```

This allows downstream processing to focus on changed records rather than repeatedly processing the full dataset.

---

# Incremental Processing

The Azure implementation uses Delta Change Data Feed and MERGE operations to demonstrate incremental processing.

```text
Silver Delta Table
         |
         v
   Change Data Feed
         |
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
   Gold Delta Table
```

This approach reduces unnecessary full-table processing when only a subset of records has changed.

---

# Data Modelling and Dataset Grain

Maintaining the correct grain is an important design principle throughout the project.

Examples:

| Dataset | Grain |
|---|---|
| Orders | One row per order |
| Order Items | One row per order item |
| Payments | One row per payment record |
| Gold Daily Metrics | One row per order date |

One-to-many datasets are aggregated before joining to order-level data to prevent accidental row multiplication.

---

# Error Handling and Logging

The local PySpark application includes logging and exception handling.

Logging covers:

- Pipeline execution
- Data ingestion
- Validation warnings
- Processing failures
- Successful completion

Spark resources are stopped safely when execution finishes or fails.

The application separates critical failures from non-critical source anomalies.

---

# Engineering Decisions

## Modular Design

The pipeline separates ingestion, transformation, validation, curation, and writing into reusable modules.

This improves maintainability and testability.

## Explicit Spark Schemas

Schemas are defined explicitly to improve control over input data types.

## Referential Integrity Checks

Left anti joins are used to detect missing relationships between datasets.

## Preserve Source Data

Bronze layers preserve source-level data to support traceability and reprocessing.

## Prevent Row Multiplication

One-to-many relationships are aggregated before joining to the intended analytical grain.

## Automated Testing

Transformation functions are tested independently using controlled input datasets.

## Orchestration

Apache Airflow manages pipeline execution instead of relying exclusively on manual commands.

## Protected Main Branch

GitHub repository rules and required CI checks reduce the risk of untested changes entering `main`.

## Controlled Deployment

Production approval and commit SHA verification add safeguards to the delivery process.

---

# Running the Project Locally

## Prerequisites

The local implementation requires:

- Python
- Java compatible with the installed PySpark version
- PySpark
- Git
- Docker and Docker Compose for Airflow
- The Olist source CSV files

## 1. Clone the Repository

```bash
git clone https://github.com/anishdhakal54/olist-pyspark-etl.git
cd olist-pyspark-etl
```

## 2. Create a Virtual Environment

```bash
python -m venv .venv
source .venv/bin/activate
```

## 3. Install Dependencies

```bash
pip install -r requirements.txt
```

## 4. Prepare Source Data

Place the required Olist CSV datasets in:

```text
data/raw/
```

## 5. Run the PySpark Pipeline

```bash
python src/main.py
```

## 6. Run Automated Tests

```bash
pytest
```

## 7. Run Through Airflow

The project also supports execution through its Docker-based Airflow environment.

Start the configured Airflow services using the repository's Docker Compose configuration, then access the Airflow interface and trigger the local Olist pipeline DAG.

The Airflow DAG executes the PySpark application from the configured project directory.

---

# Project Development Progress

| Stage | Status |
|---|---|
| Local PySpark ingestion | Completed |
| Data validation and referential integrity | Completed |
| Modular transformations | Completed |
| Aggregation and curation | Completed |
| Azure Data Lake Storage ingestion | Completed |
| Azure Databricks processing | Completed |
| Bronze / Silver / Gold | Completed |
| Delta MERGE and schema management | Completed |
| Delta time travel and restore | Completed |
| Change Data Feed | Completed |
| Incremental Gold processing | Completed |
| Local Apache Airflow orchestration | Completed |
| Docker-based Airflow environment | Completed |
| Automated PySpark testing | Completed |
| Airflow DAG testing | Completed |
| GitHub Actions CI | Completed |
| GitHub branch protection | Completed |
| GitHub Actions CD simulation | Completed |
| Production approval gate | Completed |
| Commit SHA verification | Completed |
| Automated Azure deployment | Planned |
| Monitoring and alerting improvements | Planned |

---

# Future Improvements

## Azure Deployment Automation

Extend GitHub Actions CD to deploy the tested pipeline code to Azure Databricks.

Potential additions include:

- Secure Azure authentication
- Deployment credentials and permissions
- Artifact packaging
- Environment-specific configuration
- Deployment verification
- Rollback procedures

## Secrets Management

Introduce or strengthen secure handling of credentials using appropriate Azure identity and secret-management services.

## Monitoring and Observability

Improve operational visibility through:

- Structured pipeline metrics
- Failure alerts
- Data quality monitoring
- Execution duration tracking
- Pipeline health reporting

## dbt

Explore dbt for SQL-based transformation modelling and testing.

Potential topics include:

- Models
- Sources
- `source()`
- `ref()`
- Data tests
- Documentation

## Additional CI/CD Improvements

Future work may include:

- Build artifact verification
- Deployment smoke tests
- Automated release packaging
- Environment-specific configuration
- Production rollback strategies

---

# Key Learning Outcomes

This project demonstrates hands-on experience with:

- Building modular PySpark ETL applications
- Processing relational e-commerce datasets
- Designing Bronze, Silver, and Gold architectures
- Implementing data quality and referential integrity checks
- Managing dataset grain
- Performing Spark joins and aggregations
- Building reusable transformation functions
- Using Azure Databricks and ADLS Gen2
- Working with Delta Lake transactional tables
- Implementing Delta MERGE operations
- Testing schema enforcement and evolution
- Using Delta time travel and restore
- Processing changes through Change Data Feed
- Building incremental Gold aggregations
- Orchestrating PySpark workloads using Apache Airflow
- Running Airflow using Docker
- Writing automated tests using pytest
- Configuring GitHub Actions CI workflows
- Protecting the main branch using required checks
- Building a gated CD workflow
- Verifying deployment commit integrity
- Following feature-branch and Pull Request workflows

---

# Author

**Anish Dhakal**

Data Engineering Portfolio Project

[GitHub](https://github.com/anishdhakal54) | [LinkedIn](https://www.linkedin.com/in/anish-dhakal54)

---

**Project Repository:** [olist-pyspark-etl](https://github.com/anishdhakal54/olist-pyspark-etl)
