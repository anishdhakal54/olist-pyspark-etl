# Databricks notebook source

# ============================================================
# BRONZE LAYER
# ============================================================

from pyspark.sql import functions as F

# ============================================================
# PIPELINE CONFIGURATION
# ============================================================

dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("container", "")

dbutils.widgets.text("processing_start", "")
dbutils.widgets.text("processing_end", "")

storage_account = dbutils.widgets.get("storage_account").strip()
container = dbutils.widgets.get("container").strip()

processing_start = dbutils.widgets.get("processing_start").strip()
processing_end = dbutils.widgets.get("processing_end").strip()

if not storage_account:
    raise ValueError("Missing required parameter: storage_account")

if not container:
    raise ValueError("Missing required parameter: container")

if not processing_start:
    raise ValueError("Missing required parameter: processing_start")

if not processing_end:
    raise ValueError("Missing required parameter: processing_end")

# Convert processing window start to the simulated arrival-date partition
processing_date = processing_start[:10]

print(f"Processing window: {processing_start} -> {processing_end}")
print(f"Processing source date: {processing_date}")


# ============================================================
# ADLS PATHS
# ============================================================

BASE_PATH = f"abfss://{container}@{storage_account}" ".dfs.core.windows.net"

RAW_PATH = f"{BASE_PATH}/raw"
BRONZE_PATH = f"{BASE_PATH}/bronze"
SIMULATED_RAW_PATH = f"{BASE_PATH}/raw_simulated"


# ============================================================
# DATASET CONFIGURATION
# ============================================================

datasets = {
    "orders": "olist_orders_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}


# ============================================================
# READ RAW DATA
# ============================================================

bronze_data = {}

for dataset_name, file_name in datasets.items():

    if dataset_name == "orders":
        source_path = f"{SIMULATED_RAW_PATH}/orders/" f"arrival_date={processing_date}"
    else:
        source_path = f"{RAW_PATH}/{file_name}"

    df = (
        spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", True)
        .option("multiLine", True if dataset_name == "order_reviews" else False)
        .load(source_path)
    )

    # Add Bronze metadata
    df = (
        df.withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_processing_start", F.to_timestamp(F.lit(processing_start)))
        .withColumn("_processing_end", F.to_timestamp(F.lit(processing_end)))
    )

    bronze_data[dataset_name] = df


# ============================================================
# WRITE BRONZE DELTA TABLES
# ============================================================

for dataset_name, df in bronze_data.items():

    target_path = f"{BRONZE_PATH}/{dataset_name}"

    if dataset_name == "orders":
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", replace_condition)
            .option("overwriteSchema", "true")
            .save(target_path)
        )
    else:
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(target_path)
        )

    print(f"Bronze dataset written: " f"{dataset_name}")


# ============================================================
# PIPELINE COMPLETE
# ============================================================

print("=" * 50)
print("BRONZE PROCESSING COMPLETED")
print("=" * 50)
