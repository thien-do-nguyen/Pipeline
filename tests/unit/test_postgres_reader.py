from __future__ import annotations

from unittest.mock import Mock, call

from pydantic import SecretStr

from ecommerce_pipeline.adapters.postgres import PostgresReader
from ecommerce_pipeline.config.models import (
    AppConfig,
    ApplicationConfig,
    LakehouseConfig,
    PostgresConfig,
    SparkConfig,
)


def _config() -> AppConfig:
    return AppConfig(
        application=ApplicationConfig(name="pipeline", source_system="ecommerce", timezone="Asia/Ho_Chi_Minh"),
        postgres=PostgresConfig(
            host="localhost",
            port=5432,
            database="ecommerce",
            user="admin",
            password=SecretStr("secret"),
            source_schema="customer_app",
            jdbc_driver="org.postgresql.Driver",
            retry_initial_backoff_seconds=0,
        ),
        spark=SparkConfig(app_name="test"),
        lakehouse=LakehouseConfig(base_path="data/lakehouse"),
        environment="test",
    )


def test_partitioned_reader_applies_parallelism_and_connection_limits() -> None:
    spark = Mock()
    jdbc = spark.read.format.return_value
    jdbc.option.return_value = jdbc
    expected = Mock()
    jdbc.load.return_value = expected
    reader = PostgresReader(spark, _config())

    actual = reader.read_partitioned_query(
        "SELECT event_id AS _event_id FROM customer_app.change_events",
        partition_column="_event_id",
        lower_bound=101,
        upper_bound=501,
        num_partitions=4,
    )

    assert actual is expected
    assert call("queryTimeout", "300") in jdbc.option.call_args_list
    assert call("ApplicationName", "pipeline") in jdbc.option.call_args_list
    assert call("partitionColumn", "_event_id") in jdbc.option.call_args_list
    assert call("lowerBound", "101") in jdbc.option.call_args_list
    assert call("upperBound", "501") in jdbc.option.call_args_list
    assert call("numPartitions", "4") in jdbc.option.call_args_list


def test_retry_uses_the_configured_attempt_limit() -> None:
    reader = PostgresReader(Mock(), _config())
    action = Mock(side_effect=[ConnectionError("first"), ConnectionError("second"), "done"])

    assert reader.with_retry(action, operation="test") == "done"
    assert action.call_count == 3
