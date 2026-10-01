from pyspark.sql import SparkSession

from src.validation import (
    date_validation,
    referential_integrity_check,
)

from src.transformation import (
    add_delivery_days,
    add_date_parts,
    add_late_delivery_flag,
)

import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


DATASETS = [
    "orders",
    "customers",
    "geolocation",
    "order_items",
    "order_payment",
    "orders_reviews",
    "products",
    "sellers",
    "category_name_translation",
]

def main():
    spark = None

    try:
        spark = (
            SparkSession.builder
            .appName("OlistSilver")
            .master("local[*]")
            .getOrCreate()
        )

        dataframes = {}

        for name in DATASETS:
            bronze_path = f"data/bronze/{name}"

            dataframes[name] = spark.read.parquet(bronze_path)

            logger.info(f"Successfully read {name} from Bronze")

        invalid_purchase_approval = date_validation(
            df=dataframes["orders"],
            earlier_column="order_purchase_timestamp",
            later_column="order_approved_at"
        )

        invalid_purchase_approval_count = invalid_purchase_approval.count()

        if invalid_purchase_approval_count > 0:
            logger.warning(
                f"Found {invalid_purchase_approval_count} approved dates "
                "earlier than the order purchase date."
            )
        orphan_products_count = referential_integrity_check(
            child_df=dataframes["order_items"],
            parent_df=dataframes["products"],
            key_column="product_id"
        ).count()

        if orphan_products_count > 0:
            logger.error(
                f"Product referential integrity validation failed. "
                f"Found {orphan_products_count} orphan records."
            )
            raise Exception(
                f"Product referential integrity validation failed. "
                f"Found {orphan_products_count} orphan records."
            )


        orphan_seller_count = referential_integrity_check(
            child_df=dataframes["order_items"],
            parent_df=dataframes["sellers"],
            key_column="seller_id"
        ).count()

        if orphan_seller_count > 0:
            logger.error(
                f"Seller referential integrity validation failed. "
                f"Found {orphan_seller_count} orphan records."
            )
            raise Exception(
                f"Seller referential integrity validation failed. "
                f"Found {orphan_seller_count} orphan records."
            )
        # Transform orders for the Silver layer
        orders_transformed = add_delivery_days(
            dataframes["orders"]
        )

        orders_transformed = add_date_parts(
            orders_transformed,
            date_column="order_purchase_timestamp",
            prefix="purchase"
        )

        orders_transformed = add_late_delivery_flag(
            df=orders_transformed
        )

        # Replace Bronze orders with transformed Silver orders
        dataframes["orders"] = orders_transformed

        # Write all datasets to the Silver layer
        for name, df in dataframes.items():
            silver_path = f"data/silver/{name}"

            df.write \
                .format("parquet") \
                .mode("overwrite") \
                .save(silver_path)

            logger.info(
                f"Successfully wrote {name} to {silver_path}"
            )

        logger.info("Silver processing completed successfully")

    except Exception as e:
        logger.error(f"Silver processing failed: {e}")
        raise

    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()