from pyspark.sql import SparkSession

from src.curation import (
    aggregate_order_items,
    aggregate_payment,
    build_order_curated,
    builder_order_items_curated,
)

from src.validation import validate_duplicates

import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    spark = None

    try:
        # Create local Spark session
        spark = (
            SparkSession.builder
            .appName("OlistGold")
            .master("local[*]")
            .getOrCreate()
        )

        # Read required datasets from Silver
        orders = spark.read.parquet(
            "data/silver/orders"
        )

        customers = spark.read.parquet(
            "data/silver/customers"
        )

        order_items = spark.read.parquet(
            "data/silver/order_items"
        )

        order_payment = spark.read.parquet(
            "data/silver/order_payment"
        )

        products = spark.read.parquet(
            "data/silver/products"
        )

        sellers = spark.read.parquet(
            "data/silver/sellers"
        )

        logger.info("Successfully loaded required Silver datasets")

        # Aggregate payments
        payment_agg = aggregate_payment(
            df=order_payment
        )

        # Aggregate order items
        order_items_agg = aggregate_order_items(
            order_items
        )

        # Build curated orders table
        curated_orders = build_order_curated(
            orders,
            order_items_agg_df=order_items_agg,
            payment_agg_df=payment_agg,
            customer_df=customers
        )

        # Build curated order items table
        curated_order_items = builder_order_items_curated(
            orders_items_df=order_items,
            products_df=products,
            sellers_df=sellers
        )

        # Final Gold validation
        validate_duplicates(
            curated_orders,
            ["order_id"]
        )

        validate_duplicates(
            curated_order_items,
            ["order_id", "order_item_id"]
        )

        # Write Gold tables
        curated_orders.write \
            .format("parquet") \
            .mode("overwrite") \
            .save("data/gold/orders")

        logger.info(
            "Successfully wrote curated orders to data/gold/orders"
        )

        curated_order_items.write \
            .format("parquet") \
            .mode("overwrite") \
            .save("data/gold/order_items")

        logger.info(
            "Successfully wrote curated order items to data/gold/order_items"
        )

        logger.info("Gold processing completed successfully")

    except Exception as e:
        logger.error(f"Gold processing failed: {e}")
        raise

    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()