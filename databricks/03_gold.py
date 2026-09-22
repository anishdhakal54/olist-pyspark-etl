# ============================================================
# GOLD LAYER
# ============================================================

from pyspark.sql import functions as F
from delta.tables import DeltaTable


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

BASE_PATH = (
    f"abfss://{container}@{storage_account}"
    ".dfs.core.windows.net"
)

SILVER_PATH = f"{BASE_PATH}/silver/orders"
GOLD_PATH = f"{BASE_PATH}/gold/daily_sales"


# ============================================================
# READ SILVER DATA
# ============================================================

silver_df = (
    spark.read
    .format("delta")
    .load(SILVER_PATH)
)


# ============================================================
# CREATE GOLD DATA
# ============================================================

gold_df = (
    silver_df
    .filter(
        F.col("order_status") == "delivered"
    )
    .withColumn(
        "order_date",
        F.to_date("order_purchase_timestamp")
    )
    .groupBy("order_date")
    .agg(
        F.countDistinct("order_id").alias(
            "total_delivered_orders"
        ),
        F.sum("total_payment_value").alias(
            "total_delivered_value"
        )
    )
    .withColumn(
        "avg_delivered_order_value",
        F.when(
            F.col("total_delivered_orders") > 0,
            F.col("total_delivered_value")
            / F.col("total_delivered_orders")
        ).otherwise(F.lit(0.0))
    )
)


# ============================================================
# WRITE / MERGE GOLD TABLE
# ============================================================

if DeltaTable.isDeltaTable(
    spark,
    GOLD_PATH
):

    gold_table = DeltaTable.forPath(
        spark,
        GOLD_PATH
    )

    (
        gold_table.alias("target")
        .merge(
            gold_df.alias("source"),
            "target.order_date = source.order_date"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

else:

    (
        gold_df.write
        .format("delta")
        .mode("overwrite")
        .save(GOLD_PATH)
    )


# ============================================================
# PIPELINE COMPLETE
# ============================================================

print("Gold layer completed successfully.")