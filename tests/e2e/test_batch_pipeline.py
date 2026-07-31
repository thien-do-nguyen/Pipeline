from __future__ import annotations

import os
from pathlib import Path

import pytest

from ecommerce_pipeline.config.loader import load_config
from ecommerce_pipeline.config.models import LakehouseConfig, SparkConfig
from ecommerce_pipeline.control.batch_runs import new_batch_id
from ecommerce_pipeline.control.gold_releases import GoldReleaseStore
from ecommerce_pipeline.generator.models import SeedPlan
from ecommerce_pipeline.generator.scenarios import seed_continuous, seed_once
from ecommerce_pipeline.ingestion.batch.extract_to_bronze import extract_all_to_bronze
from ecommerce_pipeline.pipelines.build_gold import build_gold
from ecommerce_pipeline.pipelines.build_silver import build_silver
from ecommerce_pipeline.runtime.spark import build_spark
from ecommerce_pipeline.validation.batch import validate_batch_lakehouse


@pytest.mark.e2e
def test_postgres_to_gold_is_incremental_idempotent_and_reconciled(tmp_path: Path) -> None:
    if os.getenv("RUN_E2E") != "1":
        pytest.skip("Set RUN_E2E=1 with PostgreSQL running")

    seed_once("local", SeedPlan(customers=8, orders=20), seed=42, reset=True)
    base = load_config("local")
    spark_values = {**base.spark.config, "spark.sql.shuffle.partitions": "2", "spark.ui.enabled": "false"}
    local_jars = os.getenv("E2E_SPARK_JARS")
    configure_delta_package = local_jars is None
    if local_jars:
        spark_values.pop("spark.jars.packages", None)
        spark_values["spark.jars"] = local_jars
    test_config = base.model_copy(
        update={
            "lakehouse": LakehouseConfig(base_path=str(tmp_path / "lakehouse")),
            "postgres": base.postgres.model_copy(
                update={
                    "max_jdbc_partitions": 4,
                    "target_events_per_partition": 10,
                }
            ),
            "spark": SparkConfig(
                master="local[2]",
                app_name="batch-e2e-test",
                configure_delta_package=configure_delta_package,
                config=spark_values,
            ),
        }
    )
    spark = build_spark(test_config)
    try:
        first_batch_id = new_batch_id(test_config.application.timezone)
        first = extract_all_to_bronze(spark, test_config, first_batch_id)
        assert all(result.record_count > 0 for result in first.results)
        first_silver = build_silver(
            spark,
            test_config,
            batch_id=first_batch_id,
            bronze_manifest=first,
        )
        build_gold(spark, test_config, batch_id=first_batch_id, silver_manifest=first_silver)
        first_report = validate_batch_lakehouse(spark, test_config)

        releases = GoldReleaseStore(spark, test_config)
        release_before = releases.latest()
        assert release_before is not None
        lakehouse = releases.snapshot()
        customer_versions_before = lakehouse.read_table("gold", "dim_customer").count()
        product_versions_before = lakehouse.read_table("gold", "dim_product").count()
        shop_versions_before = lakehouse.read_table("gold", "dim_shop").count()
        category_versions_before = lakehouse.read_table("gold", "dim_category").count()
        fact_before = lakehouse.read_table("gold", "fact_sales")
        fact_rows_before = fact_before.count()
        created_at_before = fact_before.agg({"created_at": "min"}).first()[0]

        second_batch_id = new_batch_id(test_config.application.timezone)
        second = extract_all_to_bronze(spark, test_config, second_batch_id)
        assert all(result.record_count == 0 for result in second.results)
        second_silver = build_silver(
            spark,
            test_config,
            batch_id=second_batch_id,
            bronze_manifest=second,
        )
        build_gold(spark, test_config, batch_id=second_batch_id, silver_manifest=second_silver)
        assert lakehouse.read_table("gold", "fact_sales").count() == fact_rows_before
        assert lakehouse.read_table("gold", "fact_sales").agg({"created_at": "min"}).first()[0] == created_at_before
        assert lakehouse.read_table("gold", "dim_customer").count() == customer_versions_before

        seed_continuous("local", seed=99, orders_per_batch=2, interval_seconds=0, max_batches=1)
        third_batch_id = new_batch_id(test_config.application.timezone)
        third = extract_all_to_bronze(spark, test_config, third_batch_id)
        changed = {result.table_name: result.record_count for result in third.results}
        assert changed["orders"] == 4  # two inserts, one lifecycle update and one hard delete
        assert changed["app_users"] == 1
        assert changed["user_addresses"] == 1
        assert changed["products"] == 1
        assert changed["product_variants"] == 1
        assert changed["shops"] == 1
        assert changed["categories"] == 1
        assert changed["vouchers"] == 2  # rotate the delete marker
        assert changed["payments"] == 4
        assert changed["shipments"] == 4

        third_silver = build_silver(
            spark,
            test_config,
            batch_id=third_batch_id,
            bronze_manifest=third,
        )
        build_gold(spark, test_config, batch_id=third_batch_id, silver_manifest=third_silver)
        final_report = validate_batch_lakehouse(spark, test_config)
        assert lakehouse.read_table("gold", "dim_customer").count() == customer_versions_before
        assert lakehouse.read_table("gold", "fact_sales").count() == fact_rows_before

        release_after = releases.latest()
        assert release_after is not None
        for table_name in ("dim_customer", "dim_product", "dim_shop", "dim_category"):
            assert release_after.gold_versions[table_name] == release_before.gold_versions[table_name] + 1
        assert release_after.gold_versions["fact_sales"] == release_before.gold_versions["fact_sales"] + 2

        lakehouse = releases.snapshot()
        assert final_report.gold.source_order_rows == first_report.gold.source_order_rows + 1
        assert lakehouse.read_table("gold", "dim_customer").count() == customer_versions_before + 1
        assert lakehouse.read_table("gold", "dim_product").count() == product_versions_before + 2
        assert lakehouse.read_table("gold", "dim_shop").count() == shop_versions_before + 1
        assert lakehouse.read_table("gold", "dim_category").count() == category_versions_before + 1
    finally:
        spark.stop()
