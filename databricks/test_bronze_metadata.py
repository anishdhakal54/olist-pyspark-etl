from pyspark.sql import functions as F

test_df = (
    spark.read
    .format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load(
        "abfss://olist@stolistdataanish.dfs.core.windows.net/raw/olist_orders_dataset.csv"
    )
)

display(test_df.limit(5))

test_df = test_df.withColumn(
    "_ingestion_timestamp",
    F.current_timestamp()
)

display(
    test_df.select(
        "order_id",
        "_ingestion_timestamp"
    ).limit(5)
)

test_df = test_df.withColumn(
    "_source_file",
    F.col("_metadata.file_path")
)

display(
    test_df.select(
        "order_id",
        "_ingestion_timestamp",
        "_source_file"
    ).limit(5)
)

TEST_BRONZE_PATH = (
    "abfss://olist@stolistdataanish.dfs.core.windows.net/"
    "test/bronze_metadata"
)

(
    test_df.write
    .format("delta")
    .mode("overwrite")
    .save(TEST_BRONZE_PATH)
)

saved_df = (
    spark.read
    .format("delta")
    .load(TEST_BRONZE_PATH)
)

display(
    saved_df.select(
        "order_id",
        "_ingestion_timestamp",
        "_source_file"
    ).limit(5)
)

