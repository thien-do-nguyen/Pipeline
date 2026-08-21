from pathlib import Path

from ecommerce_pipeline.contracts.bronze_tables import BRONZE_TABLES
from ecommerce_pipeline.contracts.cdc_routing import CDC_DOMAIN_TABLES, cdc_topic_for_table, cdc_topic_pattern
from ecommerce_pipeline.jobs.bootstrap_cloud_cdc import parse_args as parse_bootstrap_args
from ecommerce_pipeline.jobs.run_cloud_cdc import _emit_canary_alerts, _event_hubs_jaas


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


def test_idle_canary_does_not_alert_on_business_freshness(capsys) -> None:
    _emit_canary_alerts(
        {
            "raw_freshness_seconds": 900,
            "silver_freshness_seconds": 900,
            "gold_freshness_seconds": 900,
            "pending_gold_count": 0,
            "pending_gold_oldest_age_seconds": None,
            "quarantine_count": 0,
        },
        raw_metrics={"input_rows": 0, "latest_offsets_behind_latest": 0},
    )

    assert capsys.readouterr().out == ""


def test_active_canary_alerts_on_stale_business_freshness(capsys) -> None:
    _emit_canary_alerts(
        {
            "raw_freshness_seconds": 900,
            "silver_freshness_seconds": 10,
            "gold_freshness_seconds": 10,
            "pending_gold_count": 0,
            "pending_gold_oldest_age_seconds": None,
            "quarantine_count": 0,
        },
        raw_metrics={"input_rows": 1, "latest_offsets_behind_latest": 0},
    )

    assert "metric=raw_freshness_seconds value=900 threshold=300" in capsys.readouterr().out


def test_cloud_debezium_heartbeat_advances_a_published_control_row() -> None:
    terraform = Path("infra/cloud/cdc/main.tf").read_text(encoding="utf-8")
    bootstrap = Path("src/ecommerce_pipeline/jobs/bootstrap_cloud_cdc.py").read_text(encoding="utf-8")

    assert "DEBEZIUM_SOURCE_HEARTBEAT_ACTION_QUERY" in terraform
    assert "cdc_control.debezium_heartbeat" in terraform
    assert 'sql.Identifier("cdc_control", "debezium_heartbeat")' in bootstrap
    assert "DEBEZIUM_SOURCE_SCHEMA_INCLUDE_LIST" in terraform
    assert '"customer_app,cdc_control"' in terraform
    assert "DEBEZIUM_TRANSFORMS_ROUTEHEARTBEAT_TOPIC_REPLACEMENT" in terraform
    assert "local.event_hubs.heartbeat" in terraform
    assert "source_topic_prefix = azurerm_eventhub_namespace.cdc.name" in terraform
    assert 'transaction = "${azurerm_eventhub_namespace.cdc.name}.transaction.v1"' in terraform
    assert "DEBEZIUM_SOURCE_TOPIC_PREFIX" in terraform
    assert "local.source_topic_prefix" in terraform


def test_cloud_cdc_job_defaults_to_paused_continuous_mode() -> None:
    job = Path("resources/cdc.job.yml").read_text(encoding="utf-8")

    assert "continuous:" in job
    assert "pause_status: PAUSED" in job
    assert "default: continuous" in job
    assert '"{{job.parameters.execution_mode}}"' in job
    assert "quartz_cron_expression" not in job


def test_cloud_cdc_runtime_reset_is_explicit_and_uses_cloud_slot() -> None:
    default_args = parse_bootstrap_args([])
    reset_args = parse_bootstrap_args(["--reset-runtime-state"])

    assert default_args.reset_runtime_state is False
    assert reset_args.reset_runtime_state is True
    assert reset_args.slot == "ecommerce_cdc_cloud"


def test_cloud_destroy_clears_postgres_runtime_state_after_terraform() -> None:
    makefile = Path("Makefile").read_text(encoding="utf-8")
    destroy_recipe = makefile.split("cdc-cloud-destroy: cloud-env", maxsplit=1)[1].split(
        "deploy-cdc-cloud: cloud-env", maxsplit=1
    )[0]

    assert "terraform -chdir=$(CDC_TERRAFORM_DIR) destroy" in destroy_recipe
    assert "$(MAKE) cdc-cloud-pg-reset-state" in destroy_recipe
