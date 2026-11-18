# Databricks notebook source

# ============================================================
# OLIST - SILVER LAYER
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# ============================================================
# 1. PIPELINE CONFIGURATION
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


BASE_PATH = f"abfss://{container}@{storage_account}" ".dfs.core.windows.net"

BRONZE_PATH = f"{BASE_PATH}/bronze"
SILVER_PATH = f"{BASE_PATH}/silver"

SILVER_ORDERS_PATH = f"{SILVER_PATH}/orders"
QUARANTINE_ORDERS_PATH = f"{SILVER_PATH}/quarantine/orders"
RUN_METRICS_PATH = f"{SILVER_PATH}/run_metrics"


# ============================================================
# 2. READ BRONZE DATA
# ============================================================

orders_df = spark.read.format("delta").load(f"{BRONZE_PATH}/orders")
current_run_orders_df = orders_df.filter(
    F.col("_processing_start") == F.to_timestamp(F.lit(processing_start))
)

current_run_input_count = current_run_orders_df.count()

print(f"Current run input orders: {current_run_input_count}")

order_items_df = spark.read.format("delta").load(f"{BRONZE_PATH}/order_items")

order_payments_df = spark.read.format("delta").load(f"{BRONZE_PATH}/order_payments")

customers_df = spark.read.format("delta").load(f"{BRONZE_PATH}/customers")


# ============================================================
# 3. REMOVE BRONZE METADATA
# ============================================================


def remove_metadata(df):

    metadata_columns = ["_ingestion_timestamp", "_source_file"]

    existing_columns = [column for column in metadata_columns if column in df.columns]

    return df.drop(*existing_columns)


# Orders keep processing-window metadata because it is needed
# to determine the latest version of each order.
orders_df = remove_metadata(orders_df)

# Static reference/transaction datasets do not need
# processing-window metadata in Silver.
order_items_df = remove_metadata(order_items_df).drop(
    "_processing_start", "_processing_end"
)

order_payments_df = remove_metadata(order_payments_df).drop(
    "_processing_start", "_processing_end"
)

customers_df = remove_metadata(customers_df).drop(
    "_processing_start", "_processing_end"
)


# ============================================================
# 4. KEEP LATEST VERSION OF EACH ORDER
# ============================================================


latest_order_window = Window.partitionBy("order_id").orderBy(
    F.col("_processing_start").desc()
)

orders_df = (
    orders_df.withColumn("_row_number", F.row_number().over(latest_order_window))
    .filter(F.col("_row_number") == 1)
    .drop("_row_number")
)

order_items_df = order_items_df.dropDuplicates(["order_id", "order_item_id"])

customers_df = customers_df.dropDuplicates(["customer_id"])


# ============================================================
# 5. AGGREGATE ORDER ITEMS
# ============================================================

order_items_agg_df = order_items_df.groupBy("order_id").agg(
    F.sum("price").alias("total_item_price"),
    F.sum("freight_value").alias("total_freight_value"),
    F.count("*").alias("item_count"),
)


# ============================================================
# 6. AGGREGATE PAYMENTS
# ============================================================

payment_agg_df = order_payments_df.groupBy("order_id").agg(
    F.sum("payment_value").alias("total_payment_value"),
    F.count("*").alias("payment_count"),
)


# ============================================================
# 7. BUILD SILVER ORDERS DATASET
# ============================================================

silver_orders_df = (
    orders_df.join(order_items_agg_df, on="order_id", how="left")
    .join(payment_agg_df, on="order_id", how="left")
    .join(customers_df, on="customer_id", how="left")
)


# ============================================================
# 8. HANDLE NULL AGGREGATE VALUES
# ============================================================

silver_orders_df = (
    silver_orders_df.withColumn(
        "total_item_price", F.coalesce(col("total_item_price"), F.lit(0.0))
    )
    .withColumn(
        "total_freight_value", F.coalesce(col("total_freight_value"), F.lit(0.0))
    )
    .withColumn("item_count", F.coalesce(col("item_count"), F.lit(0)))
    .withColumn(
        "total_payment_value", F.coalesce(col("total_payment_value"), F.lit(0.0))
    )
    .withColumn("payment_count", F.coalesce(col("payment_count"), F.lit(0)))
)


# ============================================================
# 9. DATA QUALITY VALIDATION
# ============================================================

duplicate_orders_df = (
    silver_orders_df.groupBy("order_id").count().filter(col("count") > 1)
)


orphan_orders_df = orders_df.join(customers_df, on="customer_id", how="left_anti")


invalid_approval_dates_df = silver_orders_df.filter(
    col("order_approved_at") < col("order_purchase_timestamp")
)


invalid_delivery_dates_df = silver_orders_df.filter(
    col("order_delivered_customer_date") < col("order_delivered_carrier_date")
)

# Records with impossible delivery chronology are quarantined
# rather than written to the accepted Silver dataset.
quarantine_orders_df = invalid_delivery_dates_df.withColumn(
    "_quarantine_reason", F.lit("delivery_before_carrier_timestamp")
)

valid_silver_orders_df = silver_orders_df.filter(
    col("order_delivered_customer_date").isNull()
    | col("order_delivered_carrier_date").isNull()
    | (col("order_delivered_customer_date") >= col("order_delivered_carrier_date"))
)

current_run_valid_silver_df = valid_silver_orders_df.filter(
    F.col("_processing_start") == F.to_timestamp(F.lit(processing_start))
)

current_run_silver_count = current_run_valid_silver_df.count()

current_run_quarantine_df = quarantine_orders_df.filter(
    F.col("_processing_start") == F.to_timestamp(F.lit(processing_start))
)

current_run_quarantine_count = current_run_quarantine_df.count()

current_run_quarantine_rate = (
    current_run_quarantine_count / current_run_input_count
    if current_run_input_count > 0
    else 0.0
)

print(f"Current run quarantined orders: {current_run_quarantine_count}")
print(f"Current run quarantine rate: {current_run_quarantine_rate:.2%}")

print(f"Current run accepted Silver orders: {current_run_silver_count}")


valid_order_statuses = [
    "created",
    "approved",
    "invoiced",
    "processing",
    "shipped",
    "delivered",
    "unavailable",
    "canceled",
]


invalid_status_df = silver_orders_df.filter(
    ~col("order_status").isin(valid_order_statuses)
)


# ============================================================
# 10. VALIDATION COUNTS
# ============================================================

duplicate_count = duplicate_orders_df.count()

orphan_count = orphan_orders_df.count()

invalid_approval_count = invalid_approval_dates_df.count()

invalid_delivery_count = invalid_delivery_dates_df.count()

invalid_status_count = invalid_status_df.count()

total_order_count = silver_orders_df.count()
quarantine_count = quarantine_orders_df.count()

quarantine_rate = quarantine_count / total_order_count if total_order_count > 0 else 0.0

QUARANTINE_RATE_THRESHOLD = 0.05

print("Silver validation results")
print("=" * 50)

print(f"Duplicate order IDs: " f"{duplicate_count}")

print(f"Orphan customer references: " f"{orphan_count}")

print(f"Invalid approval dates: " f"{invalid_approval_count}")

print(f"Invalid delivery dates: " f"{invalid_delivery_count}")

print(f"Invalid order statuses: " f"{invalid_status_count}")

print("=" * 50)

print(f"Quarantined orders: {quarantine_count}")
print(f"Quarantine rate: {quarantine_rate:.2%}")
print(f"Quarantine threshold: {QUARANTINE_RATE_THRESHOLD:.2%}")

# ============================================================
# 11. VALIDATION GATE
# ============================================================

if duplicate_count > 0:

    raise ValueError(
        f"Silver validation failed: "
        f"{duplicate_count} duplicate "
        f"order IDs found."
    )


if orphan_count > 0:

    raise ValueError(
        f"Silver validation failed: "
        f"{orphan_count} orphan "
        f"customer references found."
    )


if invalid_approval_count > 0:

    raise ValueError(
        f"Silver validation failed: "
        f"{invalid_approval_count} invalid "
        f"approval timestamps found."
    )


if invalid_status_count > 0:

    raise ValueError(
        f"Silver validation failed: "
        f"{invalid_status_count} invalid "
        f"order statuses found."
    )


if invalid_delivery_count > 0:

    print(
        f"WARNING: {invalid_delivery_count} "
        "records contain delivery timestamps "
        "earlier than carrier timestamps."
    )


# ============================================================
# 12. WRITE QUARANTINED RECORDS
# ============================================================
if quarantine_count > 0:

    if DeltaTable.isDeltaTable(spark, QUARANTINE_ORDERS_PATH):

        quarantine_table = DeltaTable.forPath(spark, QUARANTINE_ORDERS_PATH)

        (
            quarantine_table.alias("target")
            .merge(
                quarantine_orders_df.alias("source"),
                "target.order_id = source.order_id",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    else:

        (
            quarantine_orders_df.write.format("delta")
            .mode("overwrite")
            .save(QUARANTINE_ORDERS_PATH)
        )

    print(f"Quarantined {quarantine_count} invalid " "orders successfully.")

if quarantine_rate > QUARANTINE_RATE_THRESHOLD:

    raise ValueError(
        f"Silver validation failed: quarantine rate "
        f"{quarantine_rate:.2%} exceeds threshold "
        f"{QUARANTINE_RATE_THRESHOLD:.2%}."
    )
# ============================================================
# 13. WRITE / MERGE SILVER DELTA TABLE
# ============================================================

if DeltaTable.isDeltaTable(spark, SILVER_ORDERS_PATH):
    silver_orders_table = DeltaTable.forPath(spark, SILVER_ORDERS_PATH)

    if quarantine_count > 0:

        (
            silver_orders_table.alias("target")
            .merge(
                quarantine_orders_df.select("order_id").alias("source"),
                "target.order_id = source.order_id",
            )
            .whenMatchedDelete()
            .execute()
        )

    (
        silver_orders_table.alias("target")
        .merge(
            valid_silver_orders_df.alias("source"), "target.order_id = source.order_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("Silver orders Delta table " "updated successfully.")

else:

    (
        valid_silver_orders_df.write.format("delta")
        .mode("overwrite")
        .save(SILVER_ORDERS_PATH)
    )
    print("Silver orders Delta table " "created successfully.")


# ============================================================
# 14. ENABLE CHANGE DATA FEED
# ============================================================

spark.sql(f"""
    ALTER TABLE delta.`{SILVER_ORDERS_PATH}`
    SET TBLPROPERTIES (
        delta.enableChangeDataFeed = true
    )
    """)


# ============================================================
# 15. FINAL VALIDATION
# ============================================================

final_silver_df = spark.read.format("delta").load(SILVER_ORDERS_PATH)


final_count = final_silver_df.count()

final_invalid_delivery_count = final_silver_df.filter(
    col("order_delivered_customer_date") < col("order_delivered_carrier_date")
).count()

if final_invalid_delivery_count > 0:
    raise ValueError(
        f"Final Silver validation failed: "
        f"{final_invalid_delivery_count} quarantined records "
        f"remain in Silver."
    )


# ============================================================
# 16. WRITE PIPELINE RUN METRICS
# ============================================================

run_metrics_df = spark.createDataFrame(
    [
        (
            processing_start,
            processing_end,
            current_run_input_count,
            current_run_silver_count,
            current_run_quarantine_count,
            current_run_quarantine_rate,
        )
    ],
    [
        "processing_start",
        "processing_end",
        "input_orders",
        "silver_orders",
        "quarantined_orders",
        "quarantine_rate",
    ],
).withColumn(
    "recorded_at",
    F.current_timestamp(),
)

(run_metrics_df.write.format("delta").mode("append").save(RUN_METRICS_PATH))

print("=" * 50)

print("SILVER PROCESSING COMPLETED")

print(f"Silver orders: " f"{final_count:,} rows")

print("Change Data Feed: enabled")

print("=" * 50)
