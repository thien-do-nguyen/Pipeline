from __future__ import annotations

from pydantic import SecretStr

from ecommerce_pipeline.config.models import (
    AppConfig,
    ApplicationConfig,
    LakehouseConfig,
    PostgresConfig,
    SparkConfig,
)
from ecommerce_pipeline.contracts.bronze_tables import BRONZE_TABLES, get_bronze_contract
from ecommerce_pipeline.contracts.change_events import EventCursor
from ecommerce_pipeline.contracts.silver_tables import SILVER_TABLES
from ecommerce_pipeline.ingestion.batch.bronze_operations import (
    batch_upper_bounds_query,
    change_event_query,
    jdbc_partition_count,
)


def _config() -> AppConfig:
    return AppConfig(
        application=ApplicationConfig(name="pipeline", source_system="ecommerce", timezone="Asia/Ho_Chi_Minh"),
        postgres=PostgresConfig(
            host="localhost",
            port=5432,
            database="ecommerce",
            user="admin",
            password=SecretStr("admin"),
            source_schema="customer_app",
            jdbc_driver="org.postgresql.Driver",
        ),
        spark=SparkConfig(app_name="test"),
        lakehouse=LakehouseConfig(base_path="data/lakehouse"),
        environment="test",
    )


def test_change_event_query_is_bounded_by_event_id() -> None:
    contract = get_bronze_contract("orders")
    cursor = EventCursor(last_event_id=99)

    query = change_event_query(_config(), contract, cursor, 150)

    assert "customer_app.change_events" in query
    assert "jsonb_populate_record(NULL::customer_app.orders" in query
    assert "SELECT record.*" in query
    assert "e.event_id > 99 AND e.event_id <= 150" in query
    assert "ORDER BY" not in query
    assert "e.operation AS _operation" in query


def test_initial_query_starts_after_zero() -> None:
    query = change_event_query(_config(), get_bronze_contract("vouchers"), None, 12)

    assert "e.event_id > 0 AND e.event_id <= 12" in query


def test_upper_bound_query_captures_all_sources_in_one_snapshot() -> None:
    query = batch_upper_bounds_query(
        _config(),
        {
            "orders": EventCursor(last_event_id=99),
            "vouchers": None,
        },
    )

    assert "('orders', 99), ('vouchers', 0)" in query
    assert "events.source_table = cursors.source_table" in query
    assert "events.event_id > cursors.last_event_id" in query
    assert "ORDER BY events.event_id LIMIT 500000" in query


def test_jdbc_partition_count_is_dynamic_and_source_throttled() -> None:
    config = _config()

    assert jdbc_partition_count(config, EventCursor(100), 50_100) == 1
    assert jdbc_partition_count(config, EventCursor(100), 250_100) == 3
    assert jdbc_partition_count(config, EventCursor(100), 900_100) == 4


def test_query_without_events_preserves_the_source_schema() -> None:
    query = change_event_query(_config(), get_bronze_contract("orders"), None, None)

    assert "FROM customer_app.orders t WHERE FALSE" in query
    assert "SELECT t.*" in query
    assert "CAST(NULL AS BIGINT) AS _event_id" in query


def test_contract_collections_are_immutable() -> None:
    contract = get_bronze_contract("order_items")

    assert contract.primary_keys == ("order_item_id",)
    assert isinstance(contract.primary_keys, tuple)


def test_password_hash_is_the_only_column_excluded_from_full_bronze() -> None:
    assert get_bronze_contract("app_users").excluded_columns == ("password_hash",)


def test_bronze_and_silver_contracts_cover_the_same_sources() -> None:
    assert set(BRONZE_TABLES) == set(SILVER_TABLES)
    for table_name, bronze_contract in BRONZE_TABLES.items():
        assert bronze_contract.primary_keys == SILVER_TABLES[table_name].primary_keys
