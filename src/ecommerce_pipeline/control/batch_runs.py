from __future__ import annotations

import json
import os
import re
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from uuid import uuid4
from zoneinfo import ZoneInfo

DEFAULT_TIMEZONE = "Asia/Ho_Chi_Minh"
_SAFE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


@dataclass(frozen=True)
class BronzeTableResult:
    batch_id: str
    table_name: str
    output_path: str
    record_count: int
    ingestion_type: str
    delta_version: int
    batch_upper_bound_event_id: int | None = None
    previous_event_id: int | None = None
    current_event_id: int | None = None


def _now(timezone_name: str = DEFAULT_TIMEZONE) -> datetime:
    return datetime.now(ZoneInfo(timezone_name))


def new_batch_id(timezone_name: str = DEFAULT_TIMEZONE) -> str:
    return _now(timezone_name).strftime("%Y%m%d%H%M%S%f")


def _read_json(path: str) -> dict[str, object] | None:
    local_path = Path(path)
    return json.loads(local_path.read_text(encoding="utf-8")) if local_path.exists() else None


def _write_json_atomic(path: str, payload: dict[str, object]) -> None:
    content = json.dumps(payload, indent=2)
    local_path = Path(path)
    local_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = local_path.with_suffix(f"{local_path.suffix}.{uuid4().hex}.tmp")
    tmp_path.write_text(content, encoding="utf-8")
    tmp_path.replace(local_path)


def batch_run_path(logs_path: str, batch_id: str) -> str:
    _validate_safe_name(batch_id, "batch_id")
    return "/".join((logs_path.rstrip("/"), "batch_runs", f"{batch_id}.json"))


def write_batch_run_status(
    logs_path: str,
    batch_id: str,
    status: str,
    timezone_name: str = DEFAULT_TIMEZONE,
    results: list[BronzeTableResult] | None = None,
    error: str | None = None,
    outputs: dict[str, list[str]] | None = None,
    timings_ms: dict[str, int] | None = None,
) -> str:
    output_path = batch_run_path(logs_path, batch_id)
    timestamp = _now(timezone_name).isoformat()
    existing = _read_json(output_path) or {}
    table_payload: object = (
        [asdict(result) for result in results] if results is not None else existing.get("tables", [])
    )
    total_records: object = (
        sum(result.record_count for result in results) if results is not None else existing.get("total_records", 0)
    )
    output_payload: object = outputs if outputs is not None else existing.get("outputs", {})
    timing_payload: object = timings_ms if timings_ms is not None else existing.get("timings_ms", {})
    payload: dict[str, object] = {
        **existing,
        "batch_id": batch_id,
        "status": status,
        "started_at": existing.get("started_at", timestamp),
        "updated_at": timestamp,
        "total_records": total_records,
        "tables": table_payload,
        "outputs": output_payload,
        "timings_ms": timing_payload,
    }
    if error:
        payload["error"] = error
    _write_json_atomic(output_path, payload)
    if existing.get("status") != status or error:
        _print_batch_status(payload, output_path)
    return output_path


def _print_batch_status(payload: dict[str, object], output_path: str) -> None:
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
    if status == "SUCCEEDED":
        parts.append(f"summary={output_path}")
    error = payload.get("error")
    if isinstance(error, str):
        compact_error = " ".join(error.split())
        parts.append(f"error={compact_error[:300]}")
    print(" ".join(parts), flush=True)


def _validate_safe_name(value: str, label: str) -> None:
    if not _SAFE_NAME.fullmatch(value):
        raise ValueError(f"Unsafe {label}: {value!r}")


@contextmanager
def local_pipeline_lock(logs_path: str, batch_id: str) -> Iterator[None]:
    """Prevent concurrent writers for the local filesystem implementation."""

    lock_path = Path(logs_path) / "_pipeline.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError as exc:
        owner = lock_path.read_text(encoding="utf-8").strip() if lock_path.exists() else "unknown"
        raise RuntimeError(f"Another local pipeline run is active: {owner}") from exc
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as lock_file:
            lock_file.write(batch_id)
        yield
    finally:
        with suppress(FileNotFoundError):
            lock_path.unlink()
