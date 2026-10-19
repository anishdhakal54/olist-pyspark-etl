from datetime import datetime

from src.validation import (
    null_checks,
    duplicate_checks,
    date_validation,
    valid_values_check,
    referential_integrity_check,
    validate_duplicates,
)

def test_null_checks(spark):
    # Arrange
    df = spark.createDataFrame([
        (1, 101),
        (2, None),
        (None, 103),
    ], ["order_id", "customer_id"])

    # Act
    result_df = null_checks(df)
    row = result_df.collect()[0]

    # Assert
    assert row["order_id_null_count"] == 1
    assert row["customer_id_null_count"] == 1


def test_duplicate_checks(spark):
    # Arrange
    df = spark.createDataFrame([
        (101,),
        (102,),
        (102,),
        (103,),
        (103,),
        (103,),
    ], ["order_id"])

    # Act
    result_df = duplicate_checks(df, ["order_id"])

    results = {
        row["order_id"]: row["count"]
        for row in result_df.collect()
    }

    # Assert
    assert results == {
        102: 2,
        103: 3,
    }


def test_date_validation(spark):
    # Arrange
    df = spark.createDataFrame([
        (
            1,
            datetime(2026, 10, 1),
            datetime(2026, 10, 5),
        ),
        (
            2,
            datetime(2026, 10, 10),
            datetime(2026, 10, 5),
        ),
    ], [
        "order_id",
        "purchase_date",
        "delivery_date",
    ])

    # Act
    result_df = date_validation(
        df,
        "purchase_date",
        "delivery_date",
    )

    results = result_df.collect()

    # Assert
    assert len(results) == 1
    assert results[0]["order_id"] == 2


def test_valid_values_check(spark):
    # Arrange
    df = spark.createDataFrame([
        (1, "delivered"),
        (2, "shipped"),
        (3, "cancelled"),
        (4, "INVALID_STATUS"),
    ], ["order_id", "order_status"])

    valid_statuses = [
        "delivered",
        "shipped",
        "cancelled",
    ]

    # Act
    result_df = valid_values_check(
        df,
        "order_status",
        valid_statuses,
    )

    results = result_df.collect()

    # Assert
    assert len(results) == 1
    assert results[0]["order_id"] == 4
    assert results[0]["order_status"] == "INVALID_STATUS"


def test_referential_integrity_check(spark):
    # Arrange
    parent_df = spark.createDataFrame([
        (101,),
        (102,),
        (103,),
    ], ["customer_id"])

    child_df = spark.createDataFrame([
        (1, 101),
        (2, 102),
        (3, 999),
    ], [
        "order_id",
        "customer_id",
    ])

    # Act
    result_df = referential_integrity_check(
        child_df,
        parent_df,
        "customer_id",
    )

    results = result_df.collect()

    # Assert
    assert len(results) == 1
    assert results[0]["customer_id"] == 999
    assert results[0]["order_id"] == 3


def test_validate_duplicates_passes_when_no_duplicates(spark):
    # Arrange
    df = spark.createDataFrame([
        (101,),
        (102,),
        (103,),
    ], ["order_id"])

    # Act
    validate_duplicates(df, ["order_id"])

    # Assert
    # No exception = validation passed


def test_validate_duplicates_raises_exception(spark):
    # Arrange
    df = spark.createDataFrame([
        (101,),
        (102,),
        (102,),
    ], ["order_id"])

    # Act + Assert
    try:
        validate_duplicates(df, ["order_id"])
        assert False, "Expected duplicate validation to raise an exception"
    except Exception as error:
        assert "Duplicate validation failed" in str(error)