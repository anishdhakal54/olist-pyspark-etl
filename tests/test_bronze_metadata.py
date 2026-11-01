from src.ingestion import add_bronze_metadata
from datetime import datetime

def test_add_bronze_metadata(spark):
    df = spark.createDataFrame(
    [("order_001",)],
    ["order_id"]
)
    processing_start = "2026-10-30T00:00:00"
    processing_end = "2026-10-31T00:00:00"
	
    result_df = add_bronze_metadata(df,processing_start,processing_end)
    row = result_df.collect()[0]

    assert row["_ingestion_timestamp"] is not None
    assert row["_processing_start"] == datetime(2026, 10, 30, 0, 0, 0)
    assert row["_processing_end"] == datetime(2026, 10, 31, 0, 0, 0)