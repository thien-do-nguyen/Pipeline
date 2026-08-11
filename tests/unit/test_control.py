from __future__ import annotations

from pathlib import Path

import pytest

from ecommerce_pipeline.control.batch_runs import (
    local_pipeline_lock,
    log_batch_run_status,
)


def test_batch_status_is_emitted_without_creating_json(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    log_batch_run_status("batch-1", "RUNNING", timings_ms={"spark_startup": 100})
    log_batch_run_status("batch-1", "SUCCEEDED", timings_ms={"spark_startup": 100, "gold": 250})

    assert not (tmp_path / "batch_runs").exists()
    console = capsys.readouterr().out
    assert "[batch] status=RUNNING id=batch-1 records=0" in console
    assert "[batch] status=SUCCEEDED id=batch-1 records=0" in console
    assert '[batch-run] {"batch_id":"batch-1","status":"SUCCEEDED"' in console
    assert '"timings_ms":{"spark_startup":100,"gold":250}' in console
    assert '"tables"' not in console
    assert '"outputs"' not in console


def test_local_lock_rejects_a_second_writer(tmp_path: Path) -> None:
    with (
        local_pipeline_lock(str(tmp_path), "batch-1"),
        pytest.raises(RuntimeError, match="Another local pipeline run"),
        local_pipeline_lock(str(tmp_path), "batch-2"),
    ):
        pass


def test_unsafe_batch_id_is_rejected() -> None:
    with pytest.raises(ValueError, match="Unsafe batch_id"):
        log_batch_run_status("../escape", "RUNNING")
