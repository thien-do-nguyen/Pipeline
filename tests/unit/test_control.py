from __future__ import annotations

from pathlib import Path
from unittest.mock import Mock

import pytest

from ecommerce_pipeline.config.models import TableReference
from ecommerce_pipeline.control.batch_runs import (
    local_pipeline_lock,
    log_batch_run_status,
)
from ecommerce_pipeline.control.cloud_lock import _ensure_lock_table
from ecommerce_pipeline.control.manifests import (
    BronzeBatchManifest,
    BronzeTableResult,
    SilverBatchManifest,
    SilverTableResult,
    deserialize_manifest,
    serialize_manifest,
)


@pytest.mark.parametrize(
    "manifest",
    [
        BronzeBatchManifest.from_results(
            "batch-1",
            [
                BronzeTableResult(
                    batch_id="batch-1",
                    table_name="orders",
                    record_count=3,
                    ingestion_type="incremental",
                    delta_version=8,
                    operation_counts={"INSERT": 2, "UPDATE": 1},
                )
            ],
        ),
        SilverBatchManifest(
            tables={
                "orders": SilverTableResult(
                    table_name="orders",
                    committed_version=5,
                    schema_version=2,
                )
            }
        ),
    ],
)
def test_pipeline_manifest_survives_orchestrator_round_trip(
    manifest: BronzeBatchManifest | SilverBatchManifest,
) -> None:
    assert deserialize_manifest(serialize_manifest(manifest)) == manifest


def test_pipeline_manifest_rejects_invalid_version() -> None:
    with pytest.raises(ValueError, match="delta_version"):
        deserialize_manifest(
            '{"type":"bronze","batch_id":"batch-1","tables":{"orders":'
            '{"record_count":1,"ingestion_type":"incremental","delta_version":-1,'
            '"operation_counts":{},"schema_version":1}}}'
        )


def test_batch_status_is_emitted_without_creating_json(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    log_batch_run_status("batch-1", "RUNNING", timings_ms={"spark_startup": 100})
    log_batch_run_status(
        "batch-1",
        "SUCCEEDED",
        total_records=0,
        timings_ms={"spark_startup": 100, "gold": 250},
    )

    assert not (tmp_path / "batch_runs").exists()
    console = capsys.readouterr().out
    assert "[batch] status=RUNNING id=batch-1\n" in console
    assert "[batch] status=RUNNING id=batch-1 records=" not in console
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


def test_cloud_lock_recreates_dangling_catalog_registration(monkeypatch: pytest.MonkeyPatch) -> None:
    spark = Mock()
    dataframe = Mock(sparkSession=spark)
    template = Mock()
    template.limit.return_value = dataframe
    reference = TableReference(
        "catalog.silver._pipeline_writer_locks",
        True,
        "abfss://lakehouse@example/control/pipeline_writer_locks",
    )
    write = Mock()
    monkeypatch.setattr(
        "ecommerce_pipeline.control.cloud_lock._lock_table_exists",
        Mock(return_value=False),
    )
    monkeypatch.setattr("ecommerce_pipeline.control.cloud_lock.write_delta", write)

    _ensure_lock_table(template, reference)

    spark.sql.assert_called_once_with(
        "DROP TABLE IF EXISTS `catalog`.`silver`.`_pipeline_writer_locks`"
    )
    write.assert_called_once_with(dataframe.write.format.return_value.mode.return_value, reference)
