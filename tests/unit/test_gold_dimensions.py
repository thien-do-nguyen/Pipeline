from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pyspark.sql import SparkSession

from ecommerce_pipeline.transformations.gold.dimensions import (
    build_dim_category,
    build_dim_product,
    build_dim_shop,
)


def test_shop_and_category_dimensions_expose_scd2_contract(spark: SparkSession) -> None:
    shops = spark.createDataFrame(
        [(1, "shop-public", "Shop A", "shop-a", "active", datetime(2025, 1, 1), datetime(2026, 1, 1))],
        """shop_id int, public_shop_id string, shop_name string, shop_slug string, status string,
           created_at timestamp, updated_at timestamp""",
    )
    categories = spark.createDataFrame(
        [
            (1, None, "Parent", "parent", True, datetime(2025, 1, 1), datetime(2026, 2, 1)),
            (2, 1, "Child", "child", True, datetime(2025, 2, 1), datetime(2026, 1, 1)),
        ],
        """category_id int, parent_category_id int, category_name string, slug string, is_active boolean,
           created_at timestamp, updated_at timestamp""",
    )

    shop = build_dim_shop(shops, spark).filter("source_shop_id = 1").first()
    category = build_dim_category(categories, spark).filter("source_category_id = 2").first()

    assert shop["shop_key"] > 0
    assert shop["attribute_hash"]
    assert shop["effective_from"] == datetime(2026, 1, 1)
    assert shop["is_current"]
    assert category["category_key"] > 0
    assert category["parent_category_name"] == "Parent"
    assert category["effective_from"] == datetime(2026, 2, 1)
    assert category["is_current"]


def test_product_high_frequency_type1_values_do_not_change_attribute_hash(spark: SparkSession) -> None:
    products = spark.createDataFrame(
        [
            (
                10,
                1,
                2,
                "product-public",
                "SKU-10",
                "product-10",
                "Product",
                "Brand",
                "{}",
                "[]",
                "active",
                True,
                datetime(2025, 1, 1),
                datetime(2026, 1, 1),
            )
        ],
        """product_id int, shop_id int, category_id int, public_product_id string, product_sku string,
           product_slug string, product_name string, brand string, attributes_json string, images_json string,
           status string, is_featured boolean, created_at timestamp, updated_at timestamp""",
    )
    variant_schema = """
        product_variant_id int, public_variant_id string, product_id int, variant_sku string,
        variant_name string, options_json string, unit_price decimal(12,2), compare_at_price decimal(12,2),
        currency string, stock_quantity int, reserved_quantity int, weight_kg decimal(8,3),
        images_json string, status string, is_default boolean, created_at timestamp, updated_at timestamp
    """
    original = spark.createDataFrame(
        [
            (
                100,
                "variant-public",
                10,
                "VAR-100",
                "Default",
                "{}",
                Decimal("100.00"),
                Decimal("120.00"),
                "VND",
                10,
                1,
                Decimal("1.000"),
                "[]",
                "active",
                True,
                datetime(2025, 1, 1),
                datetime(2026, 1, 1),
            )
        ],
        variant_schema,
    )
    price_and_stock_change = spark.createDataFrame(
        [
            (
                100,
                "variant-public",
                10,
                "VAR-100",
                "Default",
                "{}",
                Decimal("150.00"),
                Decimal("170.00"),
                "VND",
                4,
                2,
                Decimal("1.000"),
                "[]",
                "active",
                True,
                datetime(2025, 1, 1),
                datetime(2026, 2, 1),
            )
        ],
        variant_schema,
    )

    before = build_dim_product(products, original, spark).filter("source_product_variant_id = 100").first()
    after = build_dim_product(products, price_and_stock_change, spark).filter("source_product_variant_id = 100").first()

    assert before["attribute_hash"] == after["attribute_hash"]
    assert before["current_unit_price"] == Decimal("100.00")
    assert after["current_unit_price"] == Decimal("150.00")
    assert before["product_key"] != after["product_key"]
