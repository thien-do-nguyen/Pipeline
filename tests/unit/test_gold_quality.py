from __future__ import annotations

from decimal import Decimal
from typing import cast
from unittest.mock import Mock, patch

import pytest
from pyspark.sql import DataFrame, SparkSession

from ecommerce_pipeline.adapters.lakehouse import LakehouseAdapter
from ecommerce_pipeline.pipelines.quality import GoldQualityChecker, _FactMetrics

FACT_SCHEMA = """
    source_order_id int,
    source_order_item_id int,
    sales_key long,
    quantity int,
    unit_price_amount decimal(18,2),
    gross_sales_amount decimal(18,2),
    line_discount_amount decimal(18,2),
    order_discount_amount_allocated decimal(18,2),
    total_discount_amount decimal(18,2),
    tax_amount decimal(18,2),
    shipping_amount_allocated decimal(18,2),
    net_sales_amount decimal(18,2),
    order_date_key int,
    order_time_key int,
    customer_key long,
    product_key long,
    shop_key long,
    category_key long,
    ship_to_location_key long,
    bill_to_location_key long
"""


def _checker() -> GoldQualityChecker:
    return GoldQualityChecker(cast(LakehouseAdapter, object()))


def _valid_row() -> tuple[object, ...]:
    return (
        1,
        11,
        101,
        2,
        Decimal("50.00"),
        Decimal("100.00"),
        Decimal("5.00"),
        Decimal("5.00"),
        Decimal("10.00"),
        Decimal("8.00"),
        Decimal("2.00"),
        Decimal("100.00"),
        20260101,
        3600,
        201,
        301,
        401,
        501,
        601,
        602,
    )


def _fact_metrics(checker: GoldQualityChecker, fact: DataFrame) -> _FactMetrics:
    frame = checker._fact_metrics_frame(fact)
    row = frame.first()
    assert row is not None
    return checker._fact_metrics_from_row(row)


def test_fact_quality_rules_share_one_metrics_aggregation(spark: SparkSession) -> None:
    fact = spark.createDataFrame([_valid_row()], FACT_SCHEMA)
    checker = _checker()

    metrics = _fact_metrics(checker, fact)

    checker._assert_fact_metrics(metrics, "fact_sales")
    assert metrics.rows == 1
    assert metrics.gross_sales_total == "100.00"
    assert metrics.discount_total == "10.00"
    assert metrics.net_sales_total == "100.00"


def test_fact_metrics_detect_duplicate_source_key(spark: SparkSession) -> None:
    first = _valid_row()
    duplicate_source_key = (*first[:2], 102, *first[3:])
    fact = spark.createDataFrame([first, duplicate_source_key], FACT_SCHEMA)
    checker = _checker()

    metrics = _fact_metrics(checker, fact)

    with pytest.raises(ValueError, match="source_order_id"):
        checker._assert_fact_metrics(metrics, "fact_sales")


def test_fact_metrics_detect_invalid_amount_and_unresolved_key(spark: SparkSession) -> None:
    values = list(_valid_row())
    values[3] = 0
    values[14] = 0
    fact = spark.createDataFrame([tuple(values)], FACT_SCHEMA)
    checker = _checker()

    metrics = _fact_metrics(checker, fact)

    assert metrics.invalid_amount_rows == 1
    assert metrics.unresolved_key_rows == 1
    with pytest.raises(ValueError, match="quantity or monetary"):
        checker._assert_fact_metrics(metrics, "fact_sales")


def test_quality_cache_releases_only_entries_it_owns(spark: SparkSession) -> None:
    owned = spark.range(1)
    caller_owned = spark.range(1).selectExpr("id AS caller_id").cache()
    try:
        with _checker()._cached_for_quality(owned, caller_owned):
            assert owned.is_cached
            assert caller_owned.is_cached

        assert not owned.is_cached
        assert caller_owned.is_cached
    finally:
        caller_owned.unpersist()


def test_incremental_quality_does_not_recache_materialized_fact() -> None:
    checker = _checker()
    fact = Mock(is_cached=False)
    orders = Mock(is_cached=True)
    items = Mock(is_cached=True)
    metrics = Mock()
    report = Mock()
    with (
        patch.object(checker, "_collect_quality_metrics", return_value=(metrics, 1, 1)),
        patch.object(checker, "_assert_fact_metrics"),
        patch.object(checker, "_assert_dimension_integrity"),
        patch.object(checker, "_assert_reconciliation", return_value=report),
    ):
        result = checker.run_incremental(
            fact,
            {"orders": orders, "order_items": items},
            fact_is_materialized=True,
        )

    assert result is report
    fact.cache.assert_not_called()
    fact.unpersist.assert_not_called()
