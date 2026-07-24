from __future__ import annotations

import json
import os
import re
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import cast
from uuid import uuid4
from zoneinfo import ZoneInfo

from pyspark.sql import SparkSession

DEFAULT_TIMEZONE = "Asia/Ho_Chi_Minh"
_SAFE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


@dataclass(frozen=True)
class BronzeTableResult:
    batch_id: str
    table_name: str
    output_path: str
    record_count: int
    ingestion_type: str
    batch_upper_bound_event_id: int | None = None
    previous_event_id: int | None = None
    current_event_id: int | None = None


def _now(timezone_name: str = DEFAULT_TIMEZONE) -> datetime:
    return datetime.now(ZoneInfo(timezone_name))


def new_batch_id(timezone_name: str = DEFAULT_TIMEZONE) -> str:
    return _now(timezone_name).strftime("%Y%m%d%H%M%S%f")


def _is_remote(path: str) -> bool:
    return "://" in path


def _read_json(path: str, spark: SparkSession | None = None) -> dict[str, object] | None:
    if not _is_remote(path):
        local_path = Path(path)
        return json.loads(local_path.read_text(encoding="utf-8")) if local_path.exists() else None
    if spark is None:
        raise ValueError("spark is required for remote control paths")
    jvm = spark._jvm
    if jvm is None:
        raise RuntimeError("Spark JVM is not available")
    hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
    filesystem = hadoop_path.getFileSystem(spark._jsc.hadoopConfiguration())
    if not filesystem.exists(hadoop_path):
        return None
    stream = filesystem.open(hadoop_path)
    scanner = jvm.java.util.Scanner(stream, "UTF-8").useDelimiter("\\A")
    try:
        return cast(dict[str, object], json.loads(scanner.next() if scanner.hasNext() else "{}"))
    finally:
        scanner.close()


def _write_json_atomic(path: str, payload: dict[str, object], spark: SparkSession | None = None) -> None:
    content = json.dumps(payload, indent=2)
    if not _is_remote(path):
        local_path = Path(path)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = local_path.with_suffix(f"{local_path.suffix}.{uuid4().hex}.tmp")
        tmp_path.write_text(content, encoding="utf-8")
        tmp_path.replace(local_path)
        return
    if spark is None:
        raise ValueError("spark is required for remote control paths")
    jvm = spark._jvm
    if jvm is None:
        raise RuntimeError("Spark JVM is not available")
    hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
    tmp_path = jvm.org.apache.hadoop.fs.Path(f"{path}.{uuid4().hex}.tmp")
    filesystem = hadoop_path.getFileSystem(spark._jsc.hadoopConfiguration())
    filesystem.mkdirs(hadoop_path.getParent())
    writer = jvm.java.io.OutputStreamWriter(filesystem.create(tmp_path, True), "UTF-8")
    writer.write(content)
    writer.close()
    filesystem.delete(hadoop_path, False)
    if not filesystem.rename(tmp_path, hadoop_path):
        raise OSError(f"Cannot atomically write {path}")


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
    spark: SparkSession | None = None,
    outputs: dict[str, list[str]] | None = None,
) -> str:
    output_path = batch_run_path(logs_path, batch_id)
    timestamp = _now(timezone_name).isoformat()
    existing = _read_json(output_path, spark) or {}
    table_payload: object = (
        [asdict(result) for result in results] if results is not None else existing.get("tables", [])
    )
    total_records: object = (
        sum(result.record_count for result in results) if results is not None else existing.get("total_records", 0)
    )
    output_payload: object = outputs if outputs is not None else existing.get("outputs", {})
    payload: dict[str, object] = {
        **existing,
        "batch_id": batch_id,
        "status": status,
        "started_at": existing.get("started_at", timestamp),
        "updated_at": timestamp,
        "total_records": total_records,
        "tables": table_payload,
        "outputs": output_payload,
    }
    if error:
        payload["error"] = error
    _write_json_atomic(output_path, payload, spark)
    return output_path


def _validate_safe_name(value: str, label: str) -> None:
    if not _SAFE_NAME.fullmatch(value):
        raise ValueError(f"Unsafe {label}: {value!r}")


@contextmanager
def local_pipeline_lock(logs_path: str, batch_id: str) -> Iterator[None]:
    """Prevent concurrent writers for the local filesystem implementation."""

    if _is_remote(logs_path):
        yield
        return
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
