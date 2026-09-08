from pyspark.sql import SparkSession
from config import olist_config,output_path


import logging


from ingestion import ingest_olist_data
from validation import (validate_duplicates,date_validation,referential_integrity_check)
from transformation import add_delivery_days, add_date_parts, add_late_delivery_flag
from curation import aggregate_order_items, aggregate_payment, build_order_curated,builder_order_items_curated
from writer import write_parquet
logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)


def main():
    spark =None
    try:
        spark = SparkSession.builder.appName("OlistETL").master("local[*]").getOrCreate()

        dataframe = {}

        for name, config in olist_config.items():
            try:
                dataframe[name] = ingest_olist_data(
                    spark=spark,
                    raw_path_to_file=config["filename"],
                    file_schema=config["schema"]
                )
            
            except Exception as e:
                logger.error(f"Failed to ingest {config['filename']}: {e}")
                raise      

        invalid_purchase_approval = date_validation(
            df=dataframe["orders"],
            earlier_column="order_purchase_timestamp",
            later_column="order_approved_at"
        )

        invalid_purchase_approval_count = invalid_purchase_approval.count()

        if invalid_purchase_approval_count > 0:
            logger.warning(
                f"No of approved dates earlier than order purchase date {invalid_purchase_approval_count}."
            )

        orphan_products_count = referential_integrity_check(child_df=dataframe['order_items'],parent_df=dataframe['products'],key_column="product_id").count()
        if orphan_products_count:
            logger.error(
            f"Referential integrity error" )
            raise Exception(f"Product referential integrity validation failed. Found {orphan_products_count} orphan records.")

        orphan_seller_count = referential_integrity_check(child_df=dataframe['order_items'],parent_df=dataframe['sellers'],key_column="seller_id").count()
        if orphan_seller_count:
            logger.error(
            f"Referential integrity error" )
            raise Exception(f"seller referential integrity validation failed. Found {orphan_seller_count} orphan records.")



        order_transformed = add_delivery_days(dataframe["orders"])

        order_transformed = add_date_parts(order_transformed,date_column="order_purchase_timestamp",prefix="purchase")



        order_transformed = add_late_delivery_flag(df=order_transformed)


        payment_agg = aggregate_payment(df=dataframe["order_payment"])

        order_items_agg = aggregate_order_items(dataframe["order_items"])

        curate_orders = build_order_curated(order_transformed,order_items_agg_df=order_items_agg,payment_agg_df=payment_agg,customer_df=dataframe["customers"])

        curate_orders_items = builder_order_items_curated(orders_items_df=dataframe['order_items'],products_df=dataframe['products'],sellers_df=dataframe["sellers"])
       

        validate_duplicates(
            curate_orders,
            ["order_id"]
        )
        validate_duplicates(curate_orders_items,["order_id", "order_item_id"])
        write_parquet(
            curate_orders,
            output_location=output_path / "orders"
        )
        write_parquet(
            curate_orders_items,
            output_location=output_path / "order_items"
        )
        logger.info("ETL pipeline completed successfully")

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise
        
    finally:
        if spark is not None:
            spark.stop()



##Entry point
if __name__ == "__main__":
    main()


