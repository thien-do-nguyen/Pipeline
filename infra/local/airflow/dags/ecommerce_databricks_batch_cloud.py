from __future__ import annotations

import hashlib
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import UTC, datetime, timedelta
from typing import Any

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from airflow.sdk.exceptions import AirflowSkipException

CLOUD_BATCH_SCHEDULE = os.getenv("ECOMMERCE_CLOUD_BATCH_SCHEDULE", "").strip() or None
POLL_SECONDS = int(os.getenv("DATABRICKS_POLL_SECONDS", "20"))
RUN_TIMEOUT_SECONDS = int(os.getenv("DATABRICKS_RUN_TIMEOUT_SECONDS", "7200"))


def _env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise AirflowSkipException(f"{name} is not set; skipping cloud Databricks batch trigger")
    return value


def _request_databricks(
    host: str,
    token: str,
    path: str,
    *,
    payload: dict[str, Any] | None = None,
    query: dict[str, Any] | None = None,
) -> dict[str, Any]:
    base_url = host.rstrip("/")
    if query:
        path = f"{path}?{urllib.parse.urlencode(query)}"
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    request = urllib.request.Request(
        f"{base_url}{path}",
        data=data,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        method="POST" if payload is not None else "GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Databricks API failed: status={exc.code} body={body[:500]}") from exc


def _trigger_and_wait(stage: str, **context: Any) -> None:
    host = _env("DATABRICKS_HOST")
    token = _env("DATABRICKS_TOKEN")
    job_id = int(_env("DATABRICKS_JOB_ID"))
    airflow_run_id = f"{context['dag'].dag_id}:{context['run_id']}"
    run_key = hashlib.sha256(f"{airflow_run_id}:{stage}".encode()).hexdigest()
    batch_id = f"cloud-{run_key[:20]}-{stage}"

    run = _request_databricks(
        host,
        token,
        "/api/2.2/jobs/run-now",
        payload={
            "job_id": job_id,
            "idempotency_token": run_key,
            "job_parameters": {
                "pipeline_mode": stage,
                "batch_id": batch_id,
            },
        },
    )
    run_id = run["run_id"]
    started_at = time.monotonic()
    print(f"[databricks] stage={stage} job_id={job_id} run_id={run_id} batch_id={batch_id}", flush=True)

    while True:
        status = _request_databricks(host, token, "/api/2.2/jobs/runs/get", query={"run_id": run_id})
        state = status.get("state", {})
        life_cycle = state.get("life_cycle_state")
        result = state.get("result_state")
        message = state.get("state_message", "")
        print(
            f"[databricks] stage={stage} run_id={run_id} life_cycle={life_cycle} result={result} message={message}",
            flush=True,
        )
        if life_cycle in {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}:
            if life_cycle == "TERMINATED" and result == "SUCCESS":
                return
            raise RuntimeError(f"Databricks stage failed: stage={stage} run_id={run_id} state={state}")
        if time.monotonic() - started_at > RUN_TIMEOUT_SECONDS:
            raise TimeoutError(f"Databricks stage timed out: stage={stage} run_id={run_id}")
        time.sleep(POLL_SECONDS)


def _cloud_stage(stage: str) -> PythonOperator:
    return PythonOperator(
        task_id=f"run_cloud_{stage}",
        python_callable=_trigger_and_wait,
        op_kwargs={"stage": stage},
    )


with DAG(
    dag_id="ecommerce_databricks_batch_cloud",
    description="Orchestrate isolated Bronze, Silver, Gold, and validation runs on Databricks.",
    schedule=CLOUD_BATCH_SCHEDULE,
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    dagrun_timeout=timedelta(seconds=(RUN_TIMEOUT_SECONDS * 5) + 600),
    default_args={
        "owner": "data-platform",
        "retries": 0,
        "execution_timeout": timedelta(seconds=RUN_TIMEOUT_SECONDS + 300),
    },
    tags=["ecommerce", "batch", "cloud", "databricks", "lakehouse"],
) as dag:
    run_cloud_check_postgres = _cloud_stage("check_postgres")
    run_cloud_bronze = _cloud_stage("bronze")
    run_cloud_silver = _cloud_stage("silver")
    run_cloud_gold = _cloud_stage("gold")
    run_cloud_validate = _cloud_stage("validate")

    run_cloud_check_postgres >> run_cloud_bronze
    run_cloud_bronze >> run_cloud_silver >> run_cloud_gold >> run_cloud_validate
