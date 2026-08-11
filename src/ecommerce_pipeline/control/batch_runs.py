from __future__ import annotations

import json
import os
import re
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from datetime import datetime
from pathlib import Path
from time import monotonic, sleep
from zoneinfo import ZoneInfo

DEFAULT_TIMEZONE = "Asia/Ho_Chi_Minh"
_SAFE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


def _now(timezone_name: str = DEFAULT_TIMEZONE) -> datetime:
    return datetime.now(ZoneInfo(timezone_name))


def new_batch_id(timezone_name: str = DEFAULT_TIMEZONE) -> str:
    return _now(timezone_name).strftime("%Y%m%d%H%M%S%f")


def log_batch_run_status(
    batch_id: str,
    status: str,
    timezone_name: str = DEFAULT_TIMEZONE,
    total_records: int = 0,
    error: str | None = None,
    timings_ms: dict[str, int] | None = None,
) -> None:
    """Emit batch progress to stdout for collection by Airflow or Databricks."""

    _validate_safe_name(batch_id, "batch_id")
    timestamp = _now(timezone_name).isoformat()
    payload: dict[str, object] = {
        "batch_id": batch_id,
        "status": status,
        "event_at": timestamp,
        "total_records": total_records,
        "timings_ms": timings_ms or {},
    }
    if error:
        payload["error"] = error
    _print_batch_status(payload)
    if status in {"SUCCEEDED", "FAILED"}:
        print(f"[batch-run] {json.dumps(payload, separators=(',', ':'))}", flush=True)


def _print_batch_status(payload: dict[str, object]) -> None:
    status = str(payload["status"])
    parts = [
        "[batch]",
        f"status={status}",
        f"id={payload['batch_id']}",
        f"records={payload['total_records']}",
    ]
    timings = payload.get("timings_ms")
    if isinstance(timings, dict):
        total_ms = timings.get("total")
        if isinstance(total_ms, int):
            parts.append(f"elapsed={total_ms / 1000:.2f}s")
    error = payload.get("error")
    if isinstance(error, str):
        compact_error = " ".join(error.split())
        parts.append(f"error={compact_error[:300]}")
    print(" ".join(parts), flush=True)


def _validate_safe_name(value: str, label: str) -> None:
    if not _SAFE_NAME.fullmatch(value):
        raise ValueError(f"Unsafe {label}: {value!r}")


@contextmanager
def local_pipeline_lock(
    logs_path: str,
    batch_id: str,
    *,
    wait_timeout_seconds: int = 0,
) -> Iterator[None]:
    """Prevent concurrent writers for the local filesystem implementation."""

    lock_path = Path(logs_path) / "_pipeline.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    deadline = monotonic() + wait_timeout_seconds
    while True:
        try:
            descriptor = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            break
        except FileExistsError as exc:
            if monotonic() >= deadline:
                owner = lock_path.read_text(encoding="utf-8").strip() if lock_path.exists() else "unknown"
                raise RuntimeError(f"Another local pipeline run is active: {owner}") from exc
            sleep(1)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as lock_file:
            lock_file.write(batch_id)
        yield
    finally:
        with suppress(FileNotFoundError):
            lock_path.unlink()
