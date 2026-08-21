from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from itertools import pairwise
from typing import Literal

from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

from ecommerce_pipeline.control.batch_flow import BATCH_STAGES, BatchStage

ExecutionTarget = Literal["local", "cloud"]

PROJECT_ROOT = os.getenv("ECOMMERCE_PROJECT_ROOT", "/opt/airflow/project")
CONFIG_PATH = os.getenv("ECOMMERCE_CONFIG_PATH", f"{PROJECT_ROOT}/configs/local.yaml")
BASE_CONFIG_PATH = os.getenv("ECOMMERCE_BASE_CONFIG_PATH", f"{PROJECT_ROOT}/configs/base.yaml")
DATABRICKS_JOB_ID = os.getenv("DATABRICKS_JOB_ID", "").strip() or "not-configured"
DATABRICKS_CONN_ID = os.getenv("DATABRICKS_CONN_ID", "databricks_default")
DATABRICKS_POLL_SECONDS = int(os.getenv("DATABRICKS_POLL_SECONDS", "5"))
DATABRICKS_HTTP_TIMEOUT_SECONDS = int(os.getenv("DATABRICKS_HTTP_TIMEOUT_SECONDS", "15"))
RUN_TIMEOUT_SECONDS = int(os.getenv("DATABRICKS_RUN_TIMEOUT_SECONDS", "7200"))
DAG_TIMEOUT_SECONDS = int(os.getenv("AIRFLOW_BATCH_DAG_TIMEOUT_SECONDS", "14400"))

# Stable for every task retry in one DAG run. Layer progress and idempotency
# remain in Delta commit metadata/control tables rather than XCom manifests.
BATCH_ID = "{{ dag_run.run_after.strftime('%Y%m%d%H%M%S%f') }}"
COMMON_LOCAL_ENV = {
    "ECOMMERCE_CONFIG_PATH": CONFIG_PATH,
    "ECOMMERCE_BASE_CONFIG_PATH": BASE_CONFIG_PATH,
    "BATCH_ID": BATCH_ID,
}


@dataclass(frozen=True)
class BatchDagConfig:
    dag_id: str
    target: ExecutionTarget
    schedule: str | None


class BoundedDatabricksRunNowOperator(DatabricksRunNowOperator):
    """Use the provider run-now lifecycle with a bounded REST request timeout.

    The provider's 180-second hook default can leave a completed Databricks run
    displayed as RUNNING in Airflow during a transient status-request timeout.
    Retrying a short status request is safe: the durable run ID remains the
    source of truth and no second Databricks run is submitted.
    """

    def _get_hook(self, caller: str) -> DatabricksHook:
        return DatabricksHook(
            self.databricks_conn_id,
            timeout_seconds=DATABRICKS_HTTP_TIMEOUT_SECONDS,
            retry_limit=self.databricks_retry_limit,
            retry_delay=self.databricks_retry_delay,
            retry_args=self.databricks_retry_args,
            caller=caller,
        )


def _schedule(name: str) -> str | None:
    return os.getenv(name, "").strip() or None


def _local_pipeline_command(mode: str) -> str:
    return (
        "python -m ecommerce_pipeline.jobs.run_batch"
        ' --env "$ECOMMERCE_CONFIG_PATH"'
        ' --base-config "$ECOMMERCE_BASE_CONFIG_PATH"'
        f" --mode {mode}"
        ' --batch-id "$BATCH_ID"'
    )


def _local_task(stage: BatchStage) -> BashOperator:
    if stage.task_id == "check_source":
        command = (
            "python -m ecommerce_pipeline.jobs.run_preflight"
            " --check postgres"
            ' --env "$ECOMMERCE_CONFIG_PATH"'
            ' --base-config "$ECOMMERCE_BASE_CONFIG_PATH"'
        )
        timeout = timedelta(minutes=2)
    else:
        command = _local_pipeline_command(stage.pipeline_mode)
        timeout = timedelta(seconds=RUN_TIMEOUT_SECONDS)
    return BashOperator(
        task_id=stage.task_id,
        bash_command=command,
        cwd=PROJECT_ROOT,
        env=COMMON_LOCAL_ENV,
        append_env=True,
        do_xcom_push=False,
        execution_timeout=timeout,
        retries=0 if stage.task_id == "validate_release" else 1,
    )


def _cloud_task(stage: BatchStage) -> DatabricksRunNowOperator:
    return BoundedDatabricksRunNowOperator(
        task_id=stage.task_id,
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=DATABRICKS_JOB_ID,
        job_parameters={
            "pipeline_mode": stage.pipeline_mode,
            "batch_id": BATCH_ID,
        },
        polling_period_seconds=DATABRICKS_POLL_SECONDS,
        wait_for_termination=True,
        durable=True,
        do_xcom_push=True,
        execution_timeout=timedelta(seconds=RUN_TIMEOUT_SECONDS),
        retries=0 if stage.task_id == "validate_release" else 1,
    )


def build_batch_dag(config: BatchDagConfig) -> DAG:
    with DAG(
        dag_id=config.dag_id,
        description=f"Run the shared Lakehouse Batch flow on {config.target} Spark compute.",
        schedule=config.schedule,
        start_date=datetime(2026, 1, 1, tzinfo=UTC),
        catchup=False,
        max_active_runs=1,
        max_active_tasks=1,
        dagrun_timeout=timedelta(seconds=DAG_TIMEOUT_SECONDS),
        default_args={
            "owner": "data-platform",
            "retry_delay": timedelta(minutes=1),
        },
        tags=["ecommerce", "batch", config.target, "lakehouse", "trigger-cdc"],
    ) as dag:
        task_factory = _local_task if config.target == "local" else _cloud_task
        tasks = [task_factory(stage) for stage in BATCH_STAGES]
        for upstream, downstream in pairwise(tasks):
            upstream >> downstream
    return dag


local_batch_dag = build_batch_dag(
    BatchDagConfig(
        dag_id="ecommerce_batch_local",
        target="local",
        schedule=_schedule("ECOMMERCE_BATCH_SCHEDULE"),
    )
)

cloud_batch_dag = build_batch_dag(
    BatchDagConfig(
        dag_id="ecommerce_databricks_batch_cloud",
        target="cloud",
        schedule=_schedule("ECOMMERCE_CLOUD_BATCH_SCHEDULE"),
    )
)
