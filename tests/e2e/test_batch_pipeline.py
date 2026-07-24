from __future__ import annotations

import os
from pathlib import Path

import pytest

from ecommerce_pipeline.adapters.lakehouse import LakehouseAdapter
from ecommerce_pipeline.config.loader import load_config
from ecommerce_pipeline.config.models import LakehouseConfig, SparkConfig
from ecommerce_pipeline.control.batch_runs import new_batch_id
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
        first = extract_all_to_bronze(spark, test_config, new_batch_id(test_config.application.timezone))
        assert all(result.record_count > 0 for result in first)
        build_silver(spark, test_config, batch_id=new_batch_id(test_config.application.timezone))
        build_gold(spark, test_config)
        first_report = validate_batch_lakehouse(spark, test_config)

        lakehouse = LakehouseAdapter(spark, test_config)
        customer_versions_before = lakehouse.read_table("gold", "dim_customer").count()
        fact_before = lakehouse.read_table("gold", "fact_sales")
        fact_rows_before = fact_before.count()
        created_at_before = fact_before.agg({"created_at": "min"}).first()[0]

        second = extract_all_to_bronze(spark, test_config, new_batch_id(test_config.application.timezone))
        assert all(result.record_count == 0 for result in second)
        build_silver(spark, test_config, batch_id=new_batch_id(test_config.application.timezone))
        build_gold(spark, test_config)
        assert lakehouse.read_table("gold", "fact_sales").count() == fact_rows_before
        assert lakehouse.read_table("gold", "fact_sales").agg({"created_at": "min"}).first()[0] == created_at_before
        assert lakehouse.read_table("gold", "dim_customer").count() == customer_versions_before

        seed_continuous("local", seed=99, orders_per_batch=2, interval_seconds=0, max_batches=1)
        third = extract_all_to_bronze(spark, test_config, new_batch_id(test_config.application.timezone))
        changed = {result.table_name: result.record_count for result in third}
        assert changed["orders"] == 3  # two new orders and one status update
        assert changed["app_users"] == 1
        assert changed["product_variants"] == 1

        build_silver(spark, test_config, batch_id=new_batch_id(test_config.application.timezone))
        build_gold(spark, test_config)
        final_report = validate_batch_lakehouse(spark, test_config)
        assert final_report.gold.source_order_rows == first_report.gold.source_order_rows + 2
        assert final_report.gold.fact_rows > first_report.gold.fact_rows
        assert lakehouse.read_table("gold", "dim_customer").count() == customer_versions_before + 1
    finally:
        spark.stop()
