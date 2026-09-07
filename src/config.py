from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    IntegerType, DoubleType, DecimalType
)
from pathlib import Path

## Schemas
order_schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("order_status", StringType()),
    StructField("order_purchase_timestamp", TimestampType()),
    StructField("order_approved_at", TimestampType()),
    StructField("order_delivered_carrier_date", TimestampType()),
    StructField("order_delivered_customer_date", TimestampType()),
    StructField("order_estimated_delivery_date", TimestampType())
])

order_item_schema = StructType([
    StructField("order_id", StringType()),
    StructField("order_item_id", IntegerType()),
    StructField("product_id", StringType()),
    StructField("seller_id", StringType()),
    StructField("shipping_limit_date", TimestampType()),
    StructField("price", DoubleType()),
    StructField("freight_value", DoubleType())
])

customer_schema = StructType([
    StructField("customer_id", StringType()),
    StructField("customer_unique_id", StringType()),
    StructField("customer_zip_code_prefix", StringType()),
    StructField("customer_city", StringType()),
    StructField("customer_state", StringType())
])

product_schema = StructType([
    StructField("product_id", StringType()),
    StructField("product_category_name", StringType()),
    StructField("product_name_lenght", IntegerType()),
    StructField("product_description_lenght", IntegerType()),
    StructField("product_photos_qty", IntegerType()),
    StructField("product_weight_g", IntegerType()),
    StructField("product_length_cm", IntegerType()),
    StructField("product_height_cm", IntegerType()),
    StructField("product_width_cm", IntegerType())
])

seller_schema = StructType([
    StructField("seller_id", StringType()),
    StructField("seller_zip_code_prefix", StringType()),
    StructField("seller_city", StringType()),
    StructField("seller_state", StringType())
])

payment_schema = StructType([
    StructField("order_id", StringType()),
    StructField("payment_sequential", IntegerType()),
    StructField("payment_type", StringType()),
    StructField("payment_installments", IntegerType()),
    StructField("payment_value", DecimalType(18, 2))
])

review_schema = StructType([
    StructField("review_id", StringType()),
    StructField("order_id", StringType()),
    StructField("review_score", IntegerType()),
    StructField("review_comment_title", StringType()),
    StructField("review_comment_message", StringType()),
    StructField("review_creation_date", TimestampType()),
    StructField("review_answer_timestamp", TimestampType())
])

geolocation_schema = StructType([
    StructField("geolocation_zip_code_prefix", StringType()),
    StructField("geolocation_lat", DoubleType()),
    StructField("geolocation_lng", DoubleType()),
    StructField("geolocation_city", StringType()),
    StructField("geolocation_state", StringType())
])

category_translation_schema = StructType([
    StructField("product_category_name", StringType()),
    StructField("product_category_name_english", StringType())
])


## Ingestion configuration
olist_config = {
    "orders": {"filename": "olist_orders_dataset.csv", "schema": order_schema},
    "customers": {"filename": "olist_customers_dataset.csv", "schema": customer_schema},
    "geolocation": {"filename": "olist_geolocation_dataset.csv", "schema": geolocation_schema},
    "order_items": {"filename": "olist_order_items_dataset.csv", "schema": order_item_schema},
    "order_payment": {"filename": "olist_order_payments_dataset.csv", "schema": payment_schema},
    "orders_reviews": {"filename": "olist_order_reviews_dataset.csv", "schema": review_schema},
    "products": {"filename": "olist_products_dataset.csv", "schema": product_schema},
    "sellers": {"filename": "olist_sellers_dataset.csv", "schema": seller_schema},
    "category_name_translation": {
        "filename": "product_category_name_translation.csv",
        "schema": category_translation_schema
    }
}

PROJECT_ROOT= Path(__file__).resolve().parent.parent
RAW_PATH= PROJECT_ROOT/"data"/"raw"

output_path = PROJECT_ROOT/"data"/"curated"