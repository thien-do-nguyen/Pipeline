from __future__ import annotations

import hashlib
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from typing import Any

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

CLOUD_BATCH_SCHEDULE = os.getenv("ECOMMERCE_CLOUD_BATCH_SCHEDULE", "").strip() or None
POLL_SECONDS = int(os.getenv("DATABRICKS_POLL_SECONDS", "5"))
RUN_TIMEOUT_SECONDS = int(os.getenv("DATABRICKS_RUN_TIMEOUT_SECONDS", "7200"))


def _env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(
            f"{name} is not set in the Airflow container. Update .env and recreate Airflow with `make airflow-up`."
        )
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


def _read_pipeline_output(host: str, token: str, run: dict[str, Any]) -> tuple[str, str]:
    tasks = run.get("tasks") or []
    if len(tasks) != 1 or "run_id" not in tasks[0]:
        return "", ""
    output = _request_databricks(
        host,
        token,
        "/api/2.2/jobs/runs/get-output",
        query={"run_id": tasks[0]["run_id"]},
    )
    return str(output.get("logs") or ""), str(output.get("error") or "").strip()


def _print_new_pipeline_output(
    host: str,
    token: str,
    run: dict[str, Any],
    previous_logs: str,
    *,
    output_started: bool,
) -> tuple[str, bool, str]:
    logs, error = _read_pipeline_output(host, token, run)
    if logs.startswith(previous_logs):
        new_logs = logs[len(previous_logs) :]
    elif logs != previous_logs:
        # Databricks can rotate/truncate driver output. Do not suppress the new
        # buffer merely because it no longer has the previous prefix.
        print("[databricks-output] buffer_reset", flush=True)
        new_logs = logs
    else:
        new_logs = ""

    if new_logs:
        if not output_started:
            print("[databricks-output] begin", flush=True)
            output_started = True
        print(new_logs.rstrip("\n"), flush=True)
    return logs, output_started, error


def _trigger_and_wait(**context: Any) -> None:
    host = _env("DATABRICKS_HOST")
    token = _env("DATABRICKS_TOKEN")
    job_id = int(_env("DATABRICKS_JOB_ID"))
    airflow_run_id = f"{context['dag'].dag_id}:{context['run_id']}"
    stable_key = hashlib.sha256(airflow_run_id.encode()).hexdigest()
    attempt_number = int(context["task_instance"].try_number)
    attempt_key = hashlib.sha256(f"{stable_key}:attempt:{attempt_number}".encode()).hexdigest()
    batch_id = f"cloud-{stable_key[:20]}"

    run = _request_databricks(
        host,
        token,
        "/api/2.2/jobs/run-now",
        payload={
            "job_id": job_id,
            # A retry must not reuse a terminal failed Databricks run. Calls
            # repeated inside the same Airflow attempt remain idempotent.
            "idempotency_token": attempt_key,
            "job_parameters": {
                "pipeline_mode": "workflow",
                "batch_id": batch_id,
            },
        },
    )
    run_id = run["run_id"]
    started_at = time.monotonic()
    print(
        f"[databricks] mode=workflow attempt={attempt_number} "
        f"job_id={job_id} run_id={run_id} batch_id={batch_id}",
        flush=True,
    )

    previous_state: tuple[object, object, object] | None = None
    last_heartbeat = 0.0
    last_output_poll = 0.0
    pipeline_logs = ""
    output_started = False
    pipeline_error = ""
    while True:
        status = _request_databricks(host, token, "/api/2.2/jobs/runs/get", query={"run_id": run_id})
        state = status.get("state", {})
        life_cycle = state.get("life_cycle_state")
        result = state.get("result_state")
        message = state.get("state_message", "")
        now = time.monotonic()
        current_state = (life_cycle, result, message)
        if current_state != previous_state or now - last_heartbeat >= 60:
            print(
                f"[databricks] mode=workflow run_id={run_id} "
                f"life_cycle={life_cycle} result={result} message={message}",
                flush=True,
            )
            previous_state = current_state
            last_heartbeat = now
        if now - last_output_poll >= max(POLL_SECONDS, 15):
            # The child task/output endpoint may not exist while the run is
            # queued or installing its library. Terminal retrieval below is
            # authoritative and reports failures.
            with suppress(Exception):
                pipeline_logs, output_started, pipeline_error = _print_new_pipeline_output(
                    host,
                    token,
                    status,
                    pipeline_logs,
                    output_started=output_started,
                )
            last_output_poll = now
        if life_cycle in {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}:
            try:
                pipeline_logs, output_started, pipeline_error = _print_new_pipeline_output(
                    host,
                    token,
                    status,
                    pipeline_logs,
                    output_started=output_started,
                )
            except Exception as output_error:
                print(f"[databricks-output] retrieval_failed error={output_error}", flush=True)
            if pipeline_error:
                print(f"[databricks-error] {pipeline_error}", flush=True)
            if output_started:
                print("[databricks-output] end", flush=True)
            if life_cycle == "TERMINATED" and result == "SUCCESS":
                return
            raise RuntimeError(f"Databricks workflow failed: run_id={run_id} state={state}")
        if time.monotonic() - started_at > RUN_TIMEOUT_SECONDS:
            raise TimeoutError(f"Databricks workflow timed out: run_id={run_id}")
        time.sleep(POLL_SECONDS)


with DAG(
    dag_id="ecommerce_databricks_batch_cloud",
    description="Run the cloud Lakehouse workflow once and surface Databricks pipeline logs in Airflow.",
    schedule=CLOUD_BATCH_SCHEDULE,
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    dagrun_timeout=timedelta(seconds=RUN_TIMEOUT_SECONDS + 600),
    default_args={
        "owner": "data-platform",
        "retries": 0,
        "execution_timeout": timedelta(seconds=RUN_TIMEOUT_SECONDS + 300),
    },
    tags=["ecommerce", "batch", "cloud", "databricks", "lakehouse"],
) as dag:
    run_cloud_pipeline = PythonOperator(
        task_id="run_cloud_pipeline",
        python_callable=_trigger_and_wait,
    )
