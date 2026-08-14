from ecommerce_pipeline.contracts.bronze_tables import BRONZE_TABLES
from ecommerce_pipeline.contracts.cdc_routing import CDC_DOMAIN_TABLES, cdc_topic_for_table, cdc_topic_pattern
from ecommerce_pipeline.jobs.run_cloud_cdc import _event_hubs_jaas


def test_cloud_domains_cover_each_bronze_table_once() -> None:
    routed_tables = [table for tables in CDC_DOMAIN_TABLES.values() for table in tables]

    assert len(routed_tables) == len(set(routed_tables))
    assert set(routed_tables) == set(BRONZE_TABLES)


def test_cloud_reader_subscribes_only_to_domain_topics() -> None:
    assert cdc_topic_pattern() == r"ecommerce\.domain\..*"


def test_each_table_resolves_to_its_domain_topic() -> None:
    for domain, tables in CDC_DOMAIN_TABLES.items():
        for table_name in tables:
            assert cdc_topic_for_table(table_name) == f"ecommerce.domain.{domain}"


def test_databricks_event_hubs_jaas_uses_shaded_kafka_login_module() -> None:
    connection = "Endpoint=sb://example/;SharedAccessKeyName=listen;SharedAccessKey=secret"

    jaas = _event_hubs_jaas(connection)

    assert jaas.startswith("kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required ")
    assert 'username="$ConnectionString"' in jaas
    assert f'password="{connection}";' in jaas
