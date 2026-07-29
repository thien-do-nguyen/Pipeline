from __future__ import annotations

import json
from pathlib import Path

import pytest

from ecommerce_pipeline.control.batch_runs import (
    local_pipeline_lock,
    write_batch_run_status,
)


def test_batch_status_update_is_atomic_and_preserves_started_at(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    path = write_batch_run_status(str(tmp_path), "batch-1", "RUNNING", timings_ms={"spark_startup": 100})
    assert Path(path).parent == tmp_path / "batch_runs"
    first = json.loads(Path(path).read_text(encoding="utf-8"))
    write_batch_run_status(str(tmp_path), "batch-1", "SUCCEEDED", timings_ms={"spark_startup": 100, "gold": 250})
    final = json.loads(Path(path).read_text(encoding="utf-8"))

    assert final["status"] == "SUCCEEDED"
    assert final["started_at"] == first["started_at"]
    assert final["timings_ms"] == {"spark_startup": 100, "gold": 250}
    assert not list(tmp_path.rglob("*.tmp"))
    console = capsys.readouterr().out
    assert "[batch] status=RUNNING id=batch-1 records=0" in console
    assert "[batch] status=SUCCEEDED id=batch-1 records=0" in console
    assert "batch_status=" not in console
    assert '"tables"' not in console


def test_local_lock_rejects_a_second_writer(tmp_path: Path) -> None:
    with (
        local_pipeline_lock(str(tmp_path), "batch-1"),
        pytest.raises(RuntimeError, match="Another local pipeline run"),
        local_pipeline_lock(str(tmp_path), "batch-2"),
    ):
        pass


def test_unsafe_batch_id_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="Unsafe batch_id"):
        write_batch_run_status(str(tmp_path), "../escape", "RUNNING")
