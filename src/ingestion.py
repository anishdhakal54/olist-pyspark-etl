from pyspark.sql.types import ( StructType,StructField,StringType,IntegerType,DecimalType,TimestampType,DoubleType)
import logging
from config import RAW_PATH

order_schema = StructType([
StructField("order_id",StringType()),
StructField("customer_id",StringType()),
StructField("order_status",StringType()),
StructField("order_purchase_timestamp",TimestampType()),
StructField("order_approved_at",TimestampType()),
StructField("order_delivered_carrier_date",TimestampType()),
StructField("order_delivered_customer_date",TimestampType()),
StructField("order_estimated_delivery_date",TimestampType())

])

customer_schema = StructType([
StructField("customer_id",StringType()),
StructField("customer_unique_id",StringType()),
StructField("customer_zip_code_prefix",StringType()),
StructField("customer_city",StringType()),
StructField("customer_state",StringType())])

order_item_schema = StructType([
StructField("order_id",StringType()),
StructField("order_item_id",IntegerType()),     
StructField("product_id",StringType()),      
StructField("seller_id",StringType()),      
StructField("shipping_limit_date",TimestampType()),  
StructField("price",DoubleType()),      
StructField("freight_value",DoubleType())

])


product_schema = StructType([
StructField( "product_id",StringType()),
StructField("product_category_name",StringType()),
StructField("product_name_lenght",IntegerType()),
StructField("product_description_lenght",IntegerType()),
StructField("product_photos_qty",IntegerType()),
StructField("product_weight_g",IntegerType()),
StructField("product_length_cm",IntegerType()),
StructField("product_height_cm",IntegerType()),
StructField("product_width_cm",IntegerType())
])



seller_schema = StructType([
StructField("seller_id",StringType()),
StructField("seller_zip_code_prefix", StringType()),
StructField("seller_city",StringType()),
StructField("seller_state",StringType())
])

payment_schema=StructType([
StructField("order_id",StringType()),
StructField("payment_sequential",IntegerType()),
StructField("payment_type",StringType()),
StructField("payment_installments", IntegerType()),
StructField("payment_value",DecimalType(18,2))
])

review_schema = StructType([
StructField("review_id",StringType()),
StructField("order_id",StringType()),
StructField("review_score",IntegerType()),
StructField("review_comment_title",StringType()),
StructField("review_comment_message",StringType()),
StructField("review_creation_date",TimestampType()),
StructField("review_answer_timestamp", TimestampType())
])

geolocation_schema = StructType([
StructField("geolocation_zip_code_prefix",StringType()),
StructField("geolocation_lat",DoubleType()),
StructField("geolocation_lng",DoubleType()),
StructField("geolocation_city",StringType()),
StructField("geolocation_state",StringType())

])

category_translation_schema = StructType([
StructField("product_category_name",StringType()),
StructField("product_category_name_english",StringType())
])


def read_csv(spark, path, schema, multiline=False):

    reader = spark.read.format("csv") \
        .option("header", True) \
        .schema(schema)

    if multiline:
        reader = reader.option("multiLine", True).option("quote", '"').option("escape", '"')

    df = reader.load(path)

    return df

def ingest_olist_data(spark,raw_path_to_file,file_schema):

    try:
     logger.info(f"Reading file :{ raw_path_to_file}")

     df = read_csv(spark=spark,path=str(RAW_PATH/raw_path_to_file),schema=file_schema,multiline=False)
     logger.info(f"Successfully read file: {raw_path_to_file}")
     return df
    except Exception as e:
       logger.error(f"failed on the file: {raw_path_to_file}: {e}")
       raise

# ingest_olist_data(spark=spark,raw_path_to_file="olist_orders_dataset.csv",file_schema=order_schema)

logger = logging.getLogger(__name__)