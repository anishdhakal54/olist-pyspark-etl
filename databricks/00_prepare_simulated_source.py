# Databricks notebook source

from pyspark.sql import functions as F

# ============================================================
# 1. CONFIGURATION
# ============================================================

dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("container", "")

storage_account = dbutils.widgets.get("storage_account").strip()
container = dbutils.widgets.get("container").strip()

if not storage_account:
    raise ValueError("Missing required parameter: storage_account")

if not container:
    raise ValueError("Missing required parameter: container")


BASE_PATH = f"abfss://{container}@{storage_account}" ".dfs.core.windows.net"

RAW_ORDERS_PATH = f"{BASE_PATH}/raw/olist_orders_dataset.csv"

SIMULATED_ORDERS_PATH = f"{BASE_PATH}/raw_simulated/orders"


# ============================================================
# 2. READ ORIGINAL ORDERS
# ============================================================

orders_df = (
    spark.read.format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load(RAW_ORDERS_PATH)
)

print(f"Original orders loaded: {orders_df.count():,}")


# ============================================================
# 3. SELECT OUR FIVE-DAY SIMULATION WINDOW
# ============================================================

simulated_orders_df = orders_df.filter(
    (F.to_date("order_purchase_timestamp") >= "2017-11-24")
    & (F.to_date("order_purchase_timestamp") <= "2017-11-28")
).withColumn("arrival_date", F.to_date("order_purchase_timestamp"))


# ============================================================
# 4. SIMULATE FIVE LATE-ARRIVING ORDERS
#
# These orders were purchased on Nov 25 but will not appear
# in the source until the Nov 27 processing batch.
# ============================================================

late_orders_df = (
    simulated_orders_df.filter(F.to_date("order_purchase_timestamp") == "2017-11-25")
    .orderBy("order_id")
    .limit(5)
)

late_order_ids = [
    row["order_id"] for row in late_orders_df.select("order_id").collect()
]

print("Late-arriving order IDs:")

for order_id in late_order_ids:
    print(order_id)


simulated_orders_df = simulated_orders_df.withColumn(
    "arrival_date",
    F.when(
        F.col("order_id").isin(late_order_ids), F.to_date(F.lit("2017-11-27"))
    ).otherwise(F.col("arrival_date")),
)


# ============================================================
# 5. SIMULATE ONE CORRECTED ORDER
#
# Take one order that originally arrives on Nov 26.
# A corrected version of that same order arrives on Nov 28.
#
# Bronze should preserve both versions.
# Silver will later keep the newest version using
# _processing_start.
# ============================================================

correction_source_df = (
    simulated_orders_df.filter(F.col("arrival_date") == "2017-11-26")
    .orderBy("order_id")
    .limit(1)
)


corrected_order_df = correction_source_df.withColumn(
    "order_estimated_delivery_date", F.to_timestamp(F.lit("2017-12-20 00:00:00"))
).withColumn("arrival_date", F.to_date(F.lit("2017-11-28")))


corrected_order_id = correction_source_df.select("order_id").first()["order_id"]

print(f"Corrected order ID: {corrected_order_id}")


simulated_orders_df = simulated_orders_df.unionByName(corrected_order_df)


# ============================================================
# 6. WRITE DAILY ARRIVAL BATCHES TO ADLS
# ============================================================

arrival_dates = [
    "2017-11-24",
    "2017-11-25",
    "2017-11-26",
    "2017-11-27",
    "2017-11-28",
]


for arrival_date in arrival_dates:

    daily_batch_df = simulated_orders_df.filter(F.col("arrival_date") == arrival_date)

    output_path = f"{SIMULATED_ORDERS_PATH}/" f"arrival_date={arrival_date}"

    (daily_batch_df.write.mode("overwrite").option("header", True).csv(output_path))

    row_count = daily_batch_df.count()

    print(f"{arrival_date}: " f"{row_count:,} rows written")


# ============================================================
# 7. VALIDATE SIMULATED SOURCE
# ============================================================

total_rows = simulated_orders_df.count()

unique_orders = simulated_orders_df.select("order_id").distinct().count()

duplicate_versions = total_rows - unique_orders


print("=" * 60)
print("SIMULATED SOURCE CREATED")
print("=" * 60)

print(f"Total rows:             {total_rows:,}")
print(f"Unique order IDs:       {unique_orders:,}")
print(f"Additional versions:    {duplicate_versions:,}")
print(f"Late-arriving orders:   {len(late_order_ids)}")
print(f"Corrected order ID:     {corrected_order_id}")

print("=" * 60)


# ============================================================
# 8. SHOW BOTH VERSIONS OF CORRECTED ORDER
# ============================================================

(
    simulated_orders_df.filter(F.col("order_id") == corrected_order_id)
    .select(
        "order_id",
        "order_purchase_timestamp",
        "order_estimated_delivery_date",
        "arrival_date",
    )
    .orderBy("arrival_date")
    .show(truncate=False)
)
