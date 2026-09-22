# Databricks notebook source

# ============================================================
# OLIST - SILVER LAYER
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.functions import col
from delta.tables import DeltaTable


# ============================================================
# 1. PIPELINE CONFIGURATION
# ============================================================

dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("container", "")

storage_account = dbutils.widgets.get("storage_account").strip()
container = dbutils.widgets.get("container").strip()

if not storage_account:
    raise ValueError("Missing required parameter: storage_account")

if not container:
    raise ValueError("Missing required parameter: container")


BASE_PATH = (
    f"abfss://{container}@{storage_account}"
    ".dfs.core.windows.net"
)

BRONZE_PATH = f"{BASE_PATH}/bronze"
SILVER_PATH = f"{BASE_PATH}/silver"

SILVER_ORDERS_PATH = f"{SILVER_PATH}/orders"


# ============================================================
# 2. READ BRONZE DATA
# ============================================================

orders_df = (
    spark.read
    .format("delta")
    .load(f"{BRONZE_PATH}/orders")
)

order_items_df = (
    spark.read
    .format("delta")
    .load(f"{BRONZE_PATH}/order_items")
)

order_payments_df = (
    spark.read
    .format("delta")
    .load(f"{BRONZE_PATH}/order_payments")
)

customers_df = (
    spark.read
    .format("delta")
    .load(f"{BRONZE_PATH}/customers")
)


# ============================================================
# 3. REMOVE BRONZE METADATA
# ============================================================

def remove_metadata(df):

    metadata_columns = [
        "_ingestion_timestamp",
        "_source_file"
    ]

    existing_columns = [
        column
        for column in metadata_columns
        if column in df.columns
    ]

    return df.drop(*existing_columns)


orders_df = remove_metadata(orders_df)
order_items_df = remove_metadata(order_items_df)
order_payments_df = remove_metadata(order_payments_df)
customers_df = remove_metadata(customers_df)


# ============================================================
# 4. REMOVE DUPLICATES
# ============================================================

orders_df = orders_df.dropDuplicates(
    ["order_id"]
)

order_items_df = order_items_df.dropDuplicates(
    ["order_id", "order_item_id"]
)

customers_df = customers_df.dropDuplicates(
    ["customer_id"]
)


# ============================================================
# 5. AGGREGATE ORDER ITEMS
# ============================================================

order_items_agg_df = (
    order_items_df
    .groupBy("order_id")
    .agg(
        F.sum("price").alias(
            "total_item_price"
        ),

        F.sum("freight_value").alias(
            "total_freight_value"
        ),

        F.count("*").alias(
            "item_count"
        )
    )
)


# ============================================================
# 6. AGGREGATE PAYMENTS
# ============================================================

payment_agg_df = (
    order_payments_df
    .groupBy("order_id")
    .agg(
        F.sum("payment_value").alias(
            "total_payment_value"
        ),

        F.count("*").alias(
            "payment_count"
        )
    )
)


# ============================================================
# 7. BUILD SILVER ORDERS DATASET
# ============================================================

silver_orders_df = (
    orders_df

    .join(
        order_items_agg_df,
        on="order_id",
        how="left"
    )

    .join(
        payment_agg_df,
        on="order_id",
        how="left"
    )

    .join(
        customers_df,
        on="customer_id",
        how="left"
    )
)


# ============================================================
# 8. HANDLE NULL AGGREGATE VALUES
# ============================================================

silver_orders_df = (
    silver_orders_df

    .withColumn(
        "total_item_price",
        F.coalesce(
            col("total_item_price"),
            F.lit(0.0)
        )
    )

    .withColumn(
        "total_freight_value",
        F.coalesce(
            col("total_freight_value"),
            F.lit(0.0)
        )
    )

    .withColumn(
        "item_count",
        F.coalesce(
            col("item_count"),
            F.lit(0)
        )
    )

    .withColumn(
        "total_payment_value",
        F.coalesce(
            col("total_payment_value"),
            F.lit(0.0)
        )
    )

    .withColumn(
        "payment_count",
        F.coalesce(
            col("payment_count"),
            F.lit(0)
        )
    )
)


# ============================================================
# 9. DATA QUALITY VALIDATION
# ============================================================

duplicate_orders_df = (
    silver_orders_df
    .groupBy("order_id")
    .count()
    .filter(
        col("count") > 1
    )
)


orphan_orders_df = (
    orders_df
    .join(
        customers_df,
        on="customer_id",
        how="left_anti"
    )
)


invalid_approval_dates_df = (
    silver_orders_df
    .filter(
        col("order_approved_at")
        < col("order_purchase_timestamp")
    )
)


invalid_delivery_dates_df = (
    silver_orders_df
    .filter(
        col("order_delivered_customer_date")
        < col("order_delivered_carrier_date")
    )
)


valid_order_statuses = [
    "created",
    "approved",
    "invoiced",
    "processing",
    "shipped",
    "delivered",
    "unavailable",
    "canceled"
]


invalid_status_df = (
    silver_orders_df
    .filter(
        ~col("order_status").isin(
            valid_order_statuses
        )
    )
)


# ============================================================
# 10. VALIDATION COUNTS
# ============================================================

duplicate_count = (
    duplicate_orders_df.count()
)

orphan_count = (
    orphan_orders_df.count()
)

invalid_approval_count = (
    invalid_approval_dates_df.count()
)

invalid_delivery_count = (
    invalid_delivery_dates_df.count()
)

invalid_status_count = (
    invalid_status_df.count()
)


print("Silver validation results")
print("=" * 50)

print(
    f"Duplicate order IDs: "
    f"{duplicate_count}"
)

print(
    f"Orphan customer references: "
    f"{orphan_count}"
)

print(
    f"Invalid approval dates: "
    f"{invalid_approval_count}"
)

print(
    f"Invalid delivery dates: "
    f"{invalid_delivery_count}"
)

print(
    f"Invalid order statuses: "
    f"{invalid_status_count}"
)

print("=" * 50)


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
# 12. WRITE / MERGE SILVER DELTA TABLE
# ============================================================

if DeltaTable.isDeltaTable(
    spark,
    SILVER_ORDERS_PATH
):

    silver_orders_table = (
        DeltaTable.forPath(
            spark,
            SILVER_ORDERS_PATH
        )
    )

    (
        silver_orders_table
        .alias("target")

        .merge(
            silver_orders_df.alias("source"),
            "target.order_id = source.order_id"
        )

        .whenMatchedUpdateAll()

        .whenNotMatchedInsertAll()

        .execute()
    )

    print(
        "Silver orders Delta table "
        "updated successfully."
    )

else:

    (
        silver_orders_df.write
        .format("delta")
        .mode("overwrite")
        .save(SILVER_ORDERS_PATH)
    )

    print(
        "Silver orders Delta table "
        "created successfully."
    )


# ============================================================
# 13. ENABLE CHANGE DATA FEED
# ============================================================

spark.sql(
    f"""
    ALTER TABLE delta.`{SILVER_ORDERS_PATH}`
    SET TBLPROPERTIES (
        delta.enableChangeDataFeed = true
    )
    """
)


# ============================================================
# 14. FINAL VALIDATION
# ============================================================

final_silver_df = (
    spark.read
    .format("delta")
    .load(SILVER_ORDERS_PATH)
)


final_count = (
    final_silver_df.count()
)


print("=" * 50)

print(
    "SILVER PROCESSING COMPLETED"
)

print(
    f"Silver orders: "
    f"{final_count:,} rows"
)

print(
    "Change Data Feed: enabled"
)

print("=" * 50)