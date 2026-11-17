# Databricks notebook source

from pyspark.sql import functions as F

# ============================================================
# CONFIGURATION
# ============================================================

dbutils.widgets.text("storage_account", "stolistdev54")
dbutils.widgets.text("container", "olist")

storage_account = dbutils.widgets.get("storage_account")
container = dbutils.widgets.get("container")

BASE_PATH = f"abfss://{container}@{storage_account}.dfs.core.windows.net"

BRONZE_ORDERS_PATH = f"{BASE_PATH}/bronze/orders"
SILVER_ORDERS_PATH = f"{BASE_PATH}/silver/orders"


# ============================================================
# LOAD DATA
# ============================================================

bronze_orders = spark.read.format("delta").load(BRONZE_ORDERS_PATH)
silver_orders = spark.read.format("delta").load(SILVER_ORDERS_PATH)


# ============================================================
# 1. VALIDATE LATE-ARRIVING ORDERS
# ============================================================

late_arrivals_bronze = bronze_orders.filter(
    (F.to_date("_processing_start") == F.lit("2017-11-27"))
    & (F.to_date("order_purchase_timestamp") == F.lit("2017-11-25"))
)

late_order_ids = [
    row.order_id for row in late_arrivals_bronze.select("order_id").collect()
]

late_arrivals_silver = silver_orders.filter(F.col("order_id").isin(late_order_ids))

bronze_late_count = late_arrivals_bronze.count()
silver_late_count = late_arrivals_silver.count()

assert (
    bronze_late_count == 5
), f"Expected 5 late arrivals in Bronze, found {bronze_late_count}"

assert (
    silver_late_count == 5
), f"Expected 5 late arrivals in Silver, found {silver_late_count}"

print("Late-arrival validation: PASS")
print(f"Bronze late arrivals: {bronze_late_count}")
print(f"Silver late arrivals: {silver_late_count}")


# ============================================================
# 2. VALIDATE CORRECTED ORDER
# ============================================================

corrected_order_id = "010078e0913f48d014483bd856c01ca1"

bronze_versions = bronze_orders.filter(F.col("order_id") == corrected_order_id)

silver_version = silver_orders.filter(F.col("order_id") == corrected_order_id)

bronze_version_count = bronze_versions.count()
silver_version_count = silver_version.count()

assert (
    bronze_version_count == 2
), f"Expected 2 Bronze versions, found {bronze_version_count}"

assert (
    silver_version_count == 1
), f"Expected 1 Silver version, found {silver_version_count}"

silver_result = silver_version.select(
    "order_estimated_delivery_date",
    "_processing_start",
).first()

assert str(silver_result.order_estimated_delivery_date) == "2017-12-20 00:00:00"
assert str(silver_result._processing_start) == "2017-11-28 00:00:00"

print("\nCorrected-order validation: PASS")
print(f"Bronze versions: {bronze_version_count}")
print(f"Silver versions: {silver_version_count}")
print(
    "Corrected delivery date:",
    silver_result.order_estimated_delivery_date,
)
print(
    "Latest processing start:",
    silver_result._processing_start,
)


# ============================================================
# 3. VALIDATE CURRENT NOV 28 WINDOW STATE
# ============================================================

nov28 = bronze_orders.filter(F.to_date("_processing_start") == F.lit("2017-11-28"))

nov28_rows = nov28.count()
nov28_distinct_orders = nov28.select("order_id").distinct().count()
total_bronze_rows = bronze_orders.count()

assert nov28_rows == 381, f"Expected 381 Nov 28 rows, found {nov28_rows}"

assert (
    nov28_distinct_orders == 381
), f"Expected 381 distinct Nov 28 orders, found {nov28_distinct_orders}"

assert (
    total_bronze_rows == 2850
), f"Expected 2850 total Bronze rows, found {total_bronze_rows}"

print("\nCurrent Nov 28 state validation: PASS")
print(f"Nov 28 rows: {nov28_rows}")
print(f"Nov 28 distinct order IDs: {nov28_distinct_orders}")
print(f"Total Bronze rows: {total_bronze_rows}")


# ============================================================
# VALIDATION SUMMARY
# ============================================================

print("\n======================================")
print("INCREMENTAL PIPELINE VALIDATION: PASS")
print("======================================")
print("Late arrivals:       PASS")
print("Corrected records:   PASS")
print("Post-rerun state:    PASS")
