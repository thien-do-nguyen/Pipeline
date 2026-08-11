from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.sdk import DAG

PROJECT_ROOT = os.getenv("ECOMMERCE_PROJECT_ROOT", "/opt/airflow/project")
CONFIG_PATH = os.getenv("ECOMMERCE_CONFIG_PATH", f"{PROJECT_ROOT}/configs/local.yaml")
BASE_CONFIG_PATH = os.getenv("ECOMMERCE_BASE_CONFIG_PATH", f"{PROJECT_ROOT}/configs/base.yaml")
BATCH_SCHEDULE = os.getenv("ECOMMERCE_BATCH_SCHEDULE", "").strip() or None

RUN_ID = "{{ dag_run.run_after.strftime('%Y%m%d%H%M%S%f') }}"
COMMON_ENV = {
    "ECOMMERCE_CONFIG_PATH": CONFIG_PATH,
    "ECOMMERCE_BASE_CONFIG_PATH": BASE_CONFIG_PATH,
}


def _batch_command(mode: str) -> str:
    return (
        "python -m ecommerce_pipeline.jobs.run_batch "
        ' --env "$ECOMMERCE_CONFIG_PATH"'
        ' --base-config "$ECOMMERCE_BASE_CONFIG_PATH"'
        f" --mode {mode}"
        ' --batch-id "$BATCH_ID"'
    )


def _preflight_command(check: str) -> str:
    return (
        "python -m ecommerce_pipeline.jobs.run_preflight"
        f" --check {check}"
        ' --env "$ECOMMERCE_CONFIG_PATH"'
        ' --base-config "$ECOMMERCE_BASE_CONFIG_PATH"'
    )


def _select_gold_owner() -> str:
    from ecommerce_pipeline.config.loader import load_config

    config = load_config(CONFIG_PATH, base_path=BASE_CONFIG_PATH)
    return "build_gold_curated" if config.coordination.gold_owner == "batch" else "gold_owned_by_streaming"


def _layer_env(layer: str) -> dict[str, str]:
    return {**COMMON_ENV, "BATCH_ID": f"{RUN_ID}-{layer}"}


with DAG(
    dag_id="ecommerce_batch_local",
    description="Orchestrate layer-isolated local trigger-CDC Lakehouse batch jobs.",
    schedule=BATCH_SCHEDULE,
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    dagrun_timeout=timedelta(hours=2),
    default_args={
        "owner": "data-platform",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
        "execution_timeout": timedelta(hours=2),
    },
    tags=["ecommerce", "batch", "local", "lakehouse", "trigger-cdc"],
) as dag:
    check_postgres_source = BashOperator(
        task_id="check_postgres_source",
        bash_command=_preflight_command("postgres"),
        cwd=PROJECT_ROOT,
        env=COMMON_ENV,
        append_env=True,
        do_xcom_push=False,
        execution_timeout=timedelta(minutes=2),
    )

    start_spark_and_ingest_bronze = BashOperator(
        task_id="start_spark_and_ingest_bronze",
        bash_command=_batch_command("bronze"),
        cwd=PROJECT_ROOT,
        env=_layer_env("bronze"),
        append_env=True,
        do_xcom_push=False,
    )

    build_unified_silver = BashOperator(
        task_id="build_unified_silver",
        bash_command=_batch_command("silver"),
        cwd=PROJECT_ROOT,
        env=_layer_env("silver"),
        append_env=True,
        do_xcom_push=False,
    )

    select_gold_owner = BranchPythonOperator(
        task_id="select_gold_owner",
        python_callable=_select_gold_owner,
        retries=0,
    )

    build_gold_curated = BashOperator(
        task_id="build_gold_curated",
        bash_command=_batch_command("gold"),
        cwd=PROJECT_ROOT,
        env=_layer_env("gold"),
        append_env=True,
        do_xcom_push=False,
    )

    validate_gold_release = BashOperator(
        task_id="validate_gold_release",
        bash_command=_batch_command("validate"),
        cwd=PROJECT_ROOT,
        env=_layer_env("validate"),
        append_env=True,
        do_xcom_push=False,
        retries=0,
    )

    gold_owned_by_streaming = EmptyOperator(
        task_id="gold_owned_by_streaming",
    )

    check_postgres_source >> start_spark_and_ingest_bronze >> build_unified_silver >> select_gold_owner
    select_gold_owner >> build_gold_curated >> validate_gold_release
    select_gold_owner >> gold_owned_by_streaming
