from pyspark.sql import SparkSession
from config import olist_config,output_path

spark = SparkSession.builder.appName("OlistETL").master("local[*]").getOrCreate()

import logging


from ingestion import ingest_olist_data
from validation import (validate_duplicates)
from transformation import add_delivery_days, add_date_parts, add_late_delivery_flag
from curation import aggregate_order_items, aggregate_payment, build_order_curated,builder_order_items_curated
from writer import write_parquet
logging.basicConfig(level=logging.INFO)

dataframe = {}

for name, config in olist_config.items():
    try:
        dataframe[name] = ingest_olist_data(
            spark=spark,
            raw_path_to_file=config["filename"],
            file_schema=config["schema"]
        )
    except Exception as e:
        print(f"Failed to ingest {config['filename']}: {e}")
        raise

for name, df in dataframe.items():
    print(f"{name}: {df.count()}")


order_transformed = add_delivery_days(dataframe["orders"])

order_transformed = add_date_parts(order_transformed,date_column="order_purchase_timestamp",prefix="purchase")



order_transformed = add_late_delivery_flag(df=order_transformed)


payment_agg = aggregate_payment(df=dataframe["order_payment"])

order_items_agg = aggregate_order_items(dataframe["order_items"])

curate_orders = build_order_curated(order_transformed,order_items_agg_df=order_items_agg,payment_agg_df=payment_agg,customer_df=dataframe["customers"])
logger = logging.getLogger(__name__)

validate_duplicates(curate_orders,["order_id"])

curate_orders_items = builder_order_items_curated(orders_items_df=dataframe['order_items'],products_df=dataframe['products'],sellers_df=dataframe["sellers"])
print(curate_orders_items.count())

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



order_parquet = spark.read.format("parquet").load(str(output_path/"orders"))
order_parquet.printSchema()


order_item_parquet = spark.read.format("parquet").load(str(output_path/"order_items"))
order_item_parquet.printSchema()


