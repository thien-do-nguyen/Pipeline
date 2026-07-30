from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from ecommerce_pipeline.adapters.lakehouse import DeltaTableState
from ecommerce_pipeline.config.models import TableReference
from ecommerce_pipeline.control.manifests import BronzeBatchManifest, BronzeTableResult
from ecommerce_pipeline.pipelines import build_silver as silver_module
from ecommerce_pipeline.pipelines.build_silver import SilverBuilder


def _service() -> SilverBuilder:
    service = object.__new__(SilverBuilder)
    service.spark = Mock()
    service.config = SimpleNamespace(
        lakehouse=SimpleNamespace(
            table_reference=lambda layer, table: TableReference(f"lakehouse/{layer}/{table}", False),
        ),
    )
    service.bronze_manifest = BronzeBatchManifest.from_results(
        "batch",
        [
            BronzeTableResult(
                batch_id="batch",
                table_name="orders",
                output_path="lakehouse/bronze/orders",
                record_count=3,
                ingestion_type="incremental",
                delta_version=25,
                previous_delta_version=24,
                operation_counts={"INSERT": 2, "UPDATE": 1},
            )
        ],
    )
    service.lakehouse = Mock()
    return service


def _mock_progress(
    monkeypatch: pytest.MonkeyPatch,
    *,
    silver_version: int | None = 10,
) -> None:
    def table_state(_spark: object, _reference: TableReference, *, pipeline: str) -> DeltaTableState:
        metadata = None if silver_version is None else {"last_processed_bronze_version": silver_version}
        return DeltaTableState(silver_version or 0, silver_version, metadata)

    monkeypatch.setattr(silver_module, "try_delta_table_state", table_state)


def test_silver_processes_only_unapplied_delta_versions(monkeypatch: pytest.MonkeyPatch) -> None:
    service = _service()
    changes = object()
    transformed = object()
    service.read_changes = Mock(return_value=changes)
    service.transform = Mock(return_value=transformed)
    service._validate_schema_version = Mock()
    _mock_progress(monkeypatch)

    result = service.run_table("orders", batch_id="batch-1")

    assert result.output_path == "lakehouse/silver/orders"
    assert result.committed_version == 11
    assert result.processing_mode == "cdf"
    assert result.bronze_starting_version == 11
    assert result.bronze_ending_version == 25
    assert result.source_record_count is None
    service.read_changes.assert_called_once_with("orders", starting_version=11, ending_version=25)
    assert service.transform.call_args.args[1] is changes
    service.lakehouse.upsert_table.assert_called_once_with(
        transformed,
        "silver",
        "orders",
        ("order_id",),
        delete_mode="hard",
        target_exists=True,
    )


def test_silver_skips_transform_when_delta_version_is_current(monkeypatch: pytest.MonkeyPatch) -> None:
    service = _service()
    service.transform = Mock()
    service._validate_schema_version = Mock()
    _mock_progress(monkeypatch, silver_version=25)

    result = service.run_table("orders", batch_id="batch-2")

    assert result.output_path == "lakehouse/silver/orders"
    assert result.committed_version == 25
    assert result.processing_mode == "no_change"
    assert result.source_record_count == 3
    assert result.source_operation_counts == {"INSERT": 2, "UPDATE": 1}
    service.transform.assert_not_called()
    service._validate_schema_version.assert_not_called()
    service.lakehouse.upsert_table.assert_not_called()


def test_silver_recovers_missing_progress_by_rebuilding_once(monkeypatch: pytest.MonkeyPatch) -> None:
    service = _service()
    service._replace_from_snapshot = Mock()
    _mock_progress(monkeypatch, silver_version=None)

    result = service.run_table("orders", batch_id="batch-2")

    assert result.output_path == "lakehouse/silver/orders"
    assert result.committed_version == 1
    assert result.processing_mode == "snapshot"
    service._replace_from_snapshot.assert_called_once()
