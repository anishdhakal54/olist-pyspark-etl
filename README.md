# Olist E-commerce PySpark ETL Pipeline

## Project Overview

This project implements an end-to-end ETL pipeline for the Olist Brazilian e-commerce dataset using PySpark.

The pipeline ingests multiple raw CSV datasets, applies data-quality and referential-integrity checks, performs transformations and aggregations, builds curated analytical datasets, and writes the final outputs in Parquet format.

The project is structured as a modular data engineering application rather than a single notebook, separating ingestion, validation, transformation, curation, configuration, writing, and orchestration logic.

## Architecture

The pipeline follows the following flow:

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

The complete pipeline is orchestrated from `src/main.py`.

## Technologies Used

* Python
* PySpark
* Spark SQL / DataFrame API
* Parquet
* Git
* GitHub
* Jupyter Notebook
* Linux

## Dataset

The project uses the Olist Brazilian e-commerce dataset.

The pipeline ingests the following datasets:

* Orders
* Customers
* Order Items
* Order Payments
* Order Reviews
* Products
* Sellers
* Geolocation
* Product Category Name Translation

The source CSV files are stored locally under `data/raw/` and are excluded from Git.

## Project Structure

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

## Pipeline Components

### 1. Ingestion

`src/ingestion.py`

The ingestion layer reads the raw Olist CSV files using explicitly defined Spark schemas.

A reusable CSV reader is used so ingestion behaviour remains separate from dataset-specific validation and transformation logic.

Ingestion events and failures are recorded using Python logging.

### 2. Data Quality Validation

`src/validation.py`

Reusable validation functions are implemented for data-quality checks.

The pipeline currently includes:

* Duplicate detection using business keys
* Date consistency validation
* Referential-integrity validation using left anti joins

Critical structural problems stop the pipeline, while non-critical anomalies are logged as warnings.

For example, the pipeline checks that:

```text
order_items.product_id → products.product_id
order_items.seller_id  → sellers.seller_id
```

If orphan records are found for these relationships, the pipeline raises an exception rather than producing potentially misleading curated data.

The pipeline also detects cases where an order approval timestamp occurs before the purchase timestamp. This is treated as a warning rather than a fatal pipeline error.

## Transformations

`src/transformation.py`

Order data is enriched with additional analytical attributes.

Current transformations include:

### Delivery Duration

Calculates the number of days between purchase and delivery.

```text
order_purchase_timestamp
        +
order_delivered_customer_date
        ↓
delivery_days
```

### Purchase Date Parts

Extracts:

* Purchase year
* Purchase month

These attributes make downstream time-based analysis easier.

### Late Delivery Flag

Creates an `is_late` indicator by comparing the actual customer delivery date with the estimated delivery date.

## Aggregations

Before building the curated order dataset, lower-grain datasets are aggregated to order level.

### Order Item Aggregation

Order items are grouped by `order_id` to calculate:

* Number of order items
* Total item value
* Total freight value

### Payment Aggregation

Payments are grouped by `order_id` to calculate:

* Total payment value
* Payment count

This prevents one-to-many joins from unintentionally duplicating order records in the curated order dataset.

## Curated Data Model

The pipeline produces two primary curated datasets.

### Curated Orders

**Grain: one row per order**

The curated orders dataset combines:

* Order information
* Customer information
* Delivery metrics
* Purchase year and month
* Late-delivery indicator
* Order-item aggregates
* Payment aggregates

Before writing the dataset, the pipeline validates that `order_id` remains unique.

### Curated Order Items

**Grain: one row per order item**

The curated order-items dataset combines:

* Order-item information
* Product information
* Seller information

The expected business key is:

```text
(order_id, order_item_id)
```

The pipeline validates this composite key before writing the output.

## Output

Curated datasets are written in Parquet format:

```text
data/curated/orders/
data/curated/order_items/
```

Parquet was chosen instead of CSV for curated data because it provides a columnar storage format suitable for analytical workloads and integrates efficiently with Spark.

The write operation uses overwrite mode so repeated development runs replace the previous generated output.

## Error Handling and Logging

The pipeline uses structured logging to record ingestion activity, data-quality warnings, failures, and successful pipeline completion.

The main pipeline is protected using `try`, `except`, and `finally`.

```text
try
  → execute ETL pipeline

except
  → log pipeline failure
  → re-raise exception

finally
  → stop Spark safely
```

This ensures the Spark session is stopped even when an exception occurs during pipeline execution.

A successful execution ends with:

```text
ETL pipeline completed successfully
```

## Running the Pipeline

### 1. Clone the repository

```bash
git clone git@github.com:anishdhakal54/olist-pyspark-etl.git
cd olist-pyspark-etl
```

### 2. Create and activate a Python virtual environment

```bash
python -m venv .venv
source .venv/bin/activate
```

### 3. Install the required dependencies

Install PySpark and the project's required Python dependencies in the virtual environment.

### 4. Add the Olist CSV files

Place the raw datasets under:

```text
data/raw/
```

### 5. Run the ETL pipeline

From the project root:

```bash
python src/main.py
```

The generated Parquet datasets will be written under:

```text
data/curated/
```

## Engineering Decisions

### Explicit Schemas

Spark schemas are explicitly defined instead of relying entirely on automatic schema inference.

This gives the pipeline greater control over expected data types during ingestion.

### Separation of Concerns

Pipeline responsibilities are separated into modules:

```text
ingestion.py       → reading data
validation.py      → data-quality checks
transformation.py  → row-level transformations
curation.py        → aggregation and curated datasets
writer.py          → output writing
config.py          → configuration and paths
main.py            → orchestration
```

This makes the project easier to understand, test, maintain, and extend.

### Grain-Aware Data Modelling

The pipeline deliberately maintains different grains for the two curated outputs:

```text
orders       → one row per order
order_items  → one row per order item
```

One-to-many datasets such as payments and order items are aggregated before being joined to the order-level dataset.

### Fail Fast on Critical Data Quality Problems

Structural problems such as duplicate business keys and orphan foreign-key records are treated as pipeline failures.

Non-critical anomalies that may represent genuine source-data issues are logged as warnings instead of automatically deleting or modifying the source records.

## Future Improvements

Possible extensions to the project include:

* Automated unit and integration tests
* Additional configurable data-quality rules
* Cloud object storage
* Azure-based deployment
* Pipeline orchestration and scheduling
* CI/CD with GitHub Actions
* Partitioned Parquet output
* Incremental processing
* Monitoring and alerting
* Containerisation with Docker
* Additional curated analytical tables

## Key Learning Outcomes

This project demonstrates practical experience with:

* Building an end-to-end PySpark ETL pipeline
* Working with multiple related datasets
* Defining explicit Spark schemas
* Designing reusable ingestion functions
* Implementing data-quality checks
* Performing referential-integrity validation
* Understanding and preserving dataset grain
* Handling one-to-many relationships through aggregation
* Creating reusable PySpark transformations
* Writing analytical datasets in Parquet format
* Implementing logging and error handling
* Structuring a data engineering project into reusable modules
* Using Git and GitHub throughout the development workflow

## Author

**Anish Dhakal**

Data Engineering Portfolio Project
