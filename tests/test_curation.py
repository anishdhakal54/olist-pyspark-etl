from src.curation import (
    aggregate_payment,
    aggregate_order_items,
    build_order_curated,
    builder_order_items_curated,
)


def test_aggregate_payment(spark):
    df = spark.createDataFrame(
        [
            ("A", 10.0),
            ("A", 20.0),
            ("B", 30.0),
        ],
        ["order_id", "payment_value"]
    )

    result_df = aggregate_payment(df)
    rows = result_df.collect()

    assert len(rows) == 2

    result = {row["order_id"]: row for row in rows}

    assert result["A"]["total_payment_value"] == 30.0
    assert result["A"]["payment_count"] == 2
    assert result["B"]["total_payment_value"] == 30.0
    assert result["B"]["payment_count"] == 1


def test_aggregate_order_items(spark):
    df = spark.createDataFrame(
        [
            ("A", 1, 100.0, 10.0),
            ("A", 2, 50.0, 5.0),
            ("B", 1, 75.0, 8.0),
        ],
        ["order_id", "order_item_id", "price", "freight_value"]
    )

    result_df = aggregate_order_items(df)
    rows = result_df.collect()

    assert len(rows) == 2

    result = {row["order_id"]: row for row in rows}

    assert result["A"]["order_item_count"] == 2
    assert result["A"]["total_item_value"] == 150.0
    assert result["A"]["total_freight"] == 15.0

    assert result["B"]["order_item_count"] == 1
    assert result["B"]["total_item_value"] == 75.0
    assert result["B"]["total_freight"] == 8.0


def test_build_order_curated_does_not_multiply_orders(spark):
    orders_df = spark.createDataFrame(
        [
            ("A", "C1"),
            ("B", "C2"),
        ],
        ["order_id", "customer_id"]
    )

    order_items_agg_df = spark.createDataFrame(
        [
            ("A", 2, 150.0, 15.0),
            ("B", 1, 75.0, 8.0),
        ],
        [
            "order_id",
            "order_item_count",
            "total_item_value",
            "total_freight",
        ]
    )

    payment_agg_df = spark.createDataFrame(
        [
            ("A", 150.0, 2),
            ("B", 75.0, 1),
        ],
        ["order_id", "total_payment_value", "payment_count"]
    )

    customer_df = spark.createDataFrame(
        [
            ("C1", "London"),
            ("C2", "Manchester"),
        ],
        ["customer_id", "customer_city"]
    )

    result_df = build_order_curated(
        orders_df,
        order_items_agg_df,
        payment_agg_df,
        customer_df
    )

    assert result_df.count() == orders_df.count()
    assert result_df.select("order_id").distinct().count() == 2


def test_builder_order_items_curated_does_not_multiply_items(spark):
    order_items_df = spark.createDataFrame(
        [
            ("A", 1, "P1", "S1"),
            ("A", 2, "P2", "S2"),
            ("B", 1, "P1", "S1"),
        ],
        ["order_id", "order_item_id", "product_id", "seller_id"]
    )

    products_df = spark.createDataFrame(
        [
            ("P1", "electronics"),
            ("P2", "books"),
        ],
        ["product_id", "product_category"]
    )

    sellers_df = spark.createDataFrame(
        [
            ("S1", "London"),
            ("S2", "Birmingham"),
        ],
        ["seller_id", "seller_city"]
    )

    result_df = builder_order_items_curated(
        order_items_df,
        products_df,
        sellers_df
    )

    assert result_df.count() == order_items_df.count()

    assert (
        result_df
        .select("order_id", "order_item_id")
        .distinct()
        .count()
        == 3
    ) 