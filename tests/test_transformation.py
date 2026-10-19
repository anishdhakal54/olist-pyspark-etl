from src.transformation import add_delivery_days,add_date_parts,add_late_delivery_flag
from datetime import datetime
from pyspark.sql.types import StructType, StructField, TimestampType
from src.validation import (
    null_checks,
    duplicate_checks,
    date_validation,
    valid_values_check,
    referential_integrity_check,
    validate_duplicates,
)





def test_add_delivery_days(spark):
    df = spark.createDataFrame([
    (datetime(2026,10,1),datetime(2026,10,5),)],["order_purchase_timestamp","order_delivered_customer_date"])
    
    result_df = add_delivery_days(df)
    actual = result_df.collect()[0]["delivery_days"]

    assert actual ==4



def test_add_date_parts(spark):
    df = spark.createDataFrame([(datetime(2026,10,5),)],["order_purchase_timestamp"])

    result_df = add_date_parts(df,"order_purchase_timestamp","purchase")
    row = result_df.collect()[0]

    purchase_year = row["purchase_year"]
    purchase_month = row["purchase_month"]

    assert purchase_year==2026
    assert purchase_month ==10

def test_add_late_delivery_flag(spark):
    df = spark.createDataFrame([
        (datetime(2026, 10, 1), datetime(2026, 10, 5)),   # late
        (datetime(2026, 10, 10), datetime(2026, 10, 5)),  # not late
        (datetime(2026, 10, 5), datetime(2026, 10,5))
    ], [
        "order_estimated_delivery_date",
        "order_delivered_customer_date"
    ])

    result_df = add_late_delivery_flag(df)

    rows = result_df.collect()

    first_row = rows[0]["is_late"]
    second_row = rows[1]["is_late"]
    third_row = rows[2]["is_late"]

    assert first_row == 1
    assert second_row == 0
    assert third_row ==0

def test_late_delivery_with_null(spark):
    schema = StructType([
        StructField("order_estimated_delivery_date", TimestampType(), True),
        StructField("order_delivered_customer_date", TimestampType(), True)
    ])

    df = spark.createDataFrame([
        (datetime(2026, 10, 10), None)
    ], schema=schema)

    result_df = add_late_delivery_flag(df)

    actual = result_df.collect()[0]["is_late"]

    assert actual is None

