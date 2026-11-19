# Databricks notebook source
# ============================================================
# GOLD LAYER
# ============================================================

from pyspark.sql import functions as F

# ============================================================
# PIPELINE CONFIGURATION
# ============================================================

dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("container", "")

storage_account = dbutils.widgets.get("storage_account").strip()
container = dbutils.widgets.get("container").strip()

if not storage_account:
    raise ValueError("Missing required parameter: storage_account")

if not container:
    raise ValueError("Missing required parameter: container")


# ============================================================
# ADLS PATHS
# ============================================================

BASE_PATH = f"abfss://{container}@{storage_account}" ".dfs.core.windows.net"

SILVER_PATH = f"{BASE_PATH}/silver/orders"
GOLD_PATH = f"{BASE_PATH}/gold/daily_sales"


# ============================================================
# READ CURRENT SILVER STATE
# ============================================================

silver_df = spark.read.format("delta").load(SILVER_PATH)


# ============================================================
# CREATE GOLD DATA
# ============================================================

gold_df = (
    silver_df.filter(F.col("order_status") == "delivered")
    .withColumn("order_date", F.to_date("order_purchase_timestamp"))
    .groupBy("order_date")
    .agg(
        F.countDistinct("order_id").alias("total_delivered_orders"),
        F.sum("total_payment_value").alias("total_delivered_value"),
    )
    .withColumn(
        "avg_delivered_order_value",
        F.when(
            F.col("total_delivered_orders") > 0,
            F.col("total_delivered_value") / F.col("total_delivered_orders"),
        ).otherwise(F.lit(0.0)),
    )
)


# ============================================================
# WRITE GOLD TABLE
# ============================================================
#
# Gold is rebuilt from the complete current Silver state.
#
# A full overwrite is intentional. If a corrected Silver record
# changes its business date or changes from delivered to another
# status, an existing Gold aggregate may need to disappear
# completely. MERGE alone would not delete an aggregate key that
# no longer exists in the newly calculated Gold dataset.
#
# Rebuilding Gold ensures it always represents the current
# accepted Silver state.
# ============================================================

(
    gold_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(GOLD_PATH)
)


# ============================================================
# PIPELINE COMPLETE
# ============================================================

print("Gold layer completed successfully.")
