
import pytest
from pyspark.sql import functions as F

from src.curation import (
    aggregate_order_items,
    aggregate_payment,
    build_order_curated,
    builder_order_items_curated,
)
from src.validation import (
    duplicate_checks,
    referential_integrity_check,
    validate_duplicates,
)


@pytest.fixture
def sample_data(spark):
    orders = spark.createDataFrame(
        [
            ("o1", "c1"),
            ("o2", "c2"),
            ("o3", "c3"),
        ],
        ["order_id", "customer_id"],
    )

    customers = spark.createDataFrame(
        [
            ("c1", "customer_1"),
            ("c2", "customer_2"),
            ("c3", "customer_3"),
        ],
        ["customer_id", "customer_unique_id"],
    )

    items = spark.createDataFrame(
        [
            ("o1", 1, "p1", "s1", 100.0, 10.0),
            ("o1", 2, "p2", "s1", 50.0, 5.0),
            ("o2", 1, "p3", "s2", 200.0, 20.0),
        ],
        [
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "price",
            "freight_value",
        ],
    )

    payments = spark.createDataFrame(
        [
            ("o1", 100.0),
            ("o1", 65.0),
            ("o2", 220.0),
        ],
        ["order_id", "payment_value"],
    )

    return orders, customers, items, payments


@pytest.fixture
def curated_orders(sample_data):
    orders, customers, items, payments = sample_data

    items_agg = aggregate_order_items(items)
    payments_agg = aggregate_payment(payments)

    return build_order_curated(
        orders,
        items_agg,
        payments_agg,
        customers,
    )


def test_no_row_multiplication(sample_data, curated_orders):
    orders, _, _, _ = sample_data

    assert curated_orders.count() == orders.count()


def test_one_row_per_order(curated_orders):
    assert (
        curated_orders.count()
        == curated_orders.select("order_id").distinct().count()
    )


def test_no_missing_customer_relationships(sample_data):
    orders, customers, _, _ = sample_data

    orphans = referential_integrity_check(
        orders, customers, "customer_id"
    )

    assert orphans.count() == 0


def test_no_missing_order_item_relationships(sample_data):
    orders, _, items, _ = sample_data

    orphans = referential_integrity_check(
        items, orders, "order_id"
    )

    assert orphans.count() == 0


def test_item_aggregation_accuracy(sample_data):
    _, _, items, _ = sample_data

    result = aggregate_order_items(items)
    row = result.filter(F.col("order_id") == "o1").first()

    assert row["order_item_count"] == 2
    assert row["total_item_value"] == pytest.approx(150.0)
    assert row["total_freight"] == pytest.approx(15.0)


def test_payment_aggregation_accuracy(sample_data):
    _, _, _, payments = sample_data

    result = aggregate_payment(payments)
    row = result.filter(F.col("order_id") == "o1").first()

    assert row["payment_count"] == 2
    assert row["total_payment_value"] == pytest.approx(165.0)


def test_orders_without_items_are_preserved(curated_orders):
    row = curated_orders.filter(
        F.col("order_id") == "o3"
    ).first()

    assert row is not None
    assert row["order_item_count"] is None
    assert row["total_item_value"] is None


def test_no_duplicate_order_ids_in_source(sample_data):
    orders, _, _, _ = sample_data

    duplicates = duplicate_checks(orders, ["order_id"])

    assert duplicates.count() == 0


def test_missing_customer_is_detected(spark):
    orders = spark.createDataFrame(
        [("o1", "c1"), ("o2", "missing")],
        ["order_id", "customer_id"],
    )

    customers = spark.createDataFrame(
        [("c1",)],
        ["customer_id"],
    )

    orphans = referential_integrity_check(
        orders, customers, "customer_id"
    )

    assert orphans.count() == 1
    assert orphans.first()["customer_id"] == "missing"


def test_duplicate_order_is_rejected(spark):
    orders = spark.createDataFrame(
        [("o1",), ("o1",), ("o2",)],
        ["order_id"],
    )

    with pytest.raises(Exception, match="Duplicate validation failed"):
        validate_duplicates(orders, ["order_id"])


def test_duplicate_customer_join_is_detected(spark):
    orders = spark.createDataFrame(
        [("o1", "c1")],
        ["order_id", "customer_id"],
    )

    customers = spark.createDataFrame(
        [
            ("c1", "customer_1"),
            ("c1", "customer_duplicate"),
        ],
        ["customer_id", "customer_unique_id"],
    )

    items_agg = spark.createDataFrame(
        [("o1", 1)],
        ["order_id", "order_item_count"],
    )

    payments_agg = spark.createDataFrame(
        [("o1", 100.0)],
        ["order_id", "total_payment_value"],
    )

    result = build_order_curated(
        orders, items_agg, payments_agg, customers
    )

    assert result.count() == 2
    assert result.select("order_id").distinct().count() == 1


def test_order_items_curated_preserves_rows(spark):
    items = spark.createDataFrame(
        [
            ("o1", "p1", "s1"),
            ("o1", "p2", "s2"),
        ],
        ["order_id", "product_id", "seller_id"],
    )

    products = spark.createDataFrame(
        [
            ("p1", "electronics"),
            ("p2", "books"),
        ],
        ["product_id", "category"],
    )

    sellers = spark.createDataFrame(
        [
            ("s1", "London"),
            ("s2", "Manchester"),
        ],
        ["seller_id", "city"],
    )

    result = builder_order_items_curated(
        items, products, sellers
    )

    assert result.count() == items.count()
    assert result.filter(F.col("category").isNull()).count() == 0
    assert result.filter(F.col("city").isNull()).count() == 0
