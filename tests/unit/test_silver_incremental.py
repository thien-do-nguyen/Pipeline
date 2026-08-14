from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from ecommerce_pipeline.adapters.lakehouse import DeltaPipelineCommit, DeltaTableState
from ecommerce_pipeline.config.models import TableReference
from ecommerce_pipeline.control.manifests import BronzeBatchManifest, BronzeTableResult
from ecommerce_pipeline.pipelines import build_silver as silver_module
from ecommerce_pipeline.pipelines.build_silver import SilverBuilder


def _service(table_name: str = "orders") -> SilverBuilder:
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
                table_name=table_name,
                record_count=3,
                ingestion_type="incremental",
                delta_version=25,
                operation_counts={"INSERT": 2, "UPDATE": 1},
            )
        ],
    )
    service.lakehouse = Mock()
    service.timings_ms = None
    return service


def _mock_progress(
    monkeypatch: pytest.MonkeyPatch,
    *,
    silver_version: int | None = 10,
) -> None:
    def table_state(_spark: object, _reference: TableReference, *, pipeline: str) -> DeltaTableState:
        assert pipeline == silver_module.SILVER_PIPELINE_NAME
        metadata = None if silver_version is None else {"last_processed_bronze_version": silver_version}
        return DeltaTableState(silver_version or 0, silver_version, metadata)

    monkeypatch.setattr(silver_module, "try_delta_table_state", table_state)
    monkeypatch.setattr(
        silver_module,
        "latest_delta_pipeline_commit",
        Mock(return_value=DeltaPipelineCommit(silver_version or 0, {"silver_schema_version": 2})),
    )


def test_silver_processes_only_unapplied_delta_versions(monkeypatch: pytest.MonkeyPatch) -> None:
    service = _service()
    changes = object()
    transformed = object()
    history = object()
    service.read_changes = Mock(return_value=changes)
    service.transform = Mock(return_value=transformed)
    service.transform_history = Mock(return_value=history)
    service.append_change_history = Mock()
    service._validate_schema_version = Mock()
    _mock_progress(monkeypatch)

    result = service.run_table("orders", batch_id="batch-1")

    assert result.committed_version == 11
    service.read_changes.assert_called_once_with("orders", starting_version=11, ending_version=25)
    assert service.transform.call_args.args[1] is changes
    service.transform_history.assert_not_called()
    service.append_change_history.assert_not_called()
    service.lakehouse.upsert_table.assert_called_once_with(
        transformed,
        "silver",
        "orders",
        ("order_id",),
        delete_mode="soft",
        sequence_columns=(
            "_event_occurred_at",
            "_ingestion_priority",
            "_source_event_sequence",
            "_source_event_subsequence",
        ),
        target_exists=True,
        source_is_nonempty=True,
    )


def test_silver_materializes_history_only_for_scd2_sources() -> None:
    expected = {"app_users", "shops", "categories", "products", "product_variants"}

    actual = {name for name, contract in silver_module.SILVER_TABLES.items() if contract.materialize_change_history}

    assert actual == expected


def test_silver_reuses_scd2_source_for_history_and_current_merge(monkeypatch: pytest.MonkeyPatch) -> None:
    service = _service("app_users")
    changes = Mock(is_cached=False)
    changes.cache.return_value = changes
    history = Mock()
    transformed = Mock()
    service.read_changes = Mock(return_value=changes)
    service.transform_history = Mock(return_value=history)
    service.transform = Mock(return_value=transformed)
    service.append_change_history = Mock()
    service._validate_schema_version = Mock()
    _mock_progress(monkeypatch)

    service.run_table("app_users", batch_id="batch-1")

    changes.cache.assert_called_once_with()
    service.transform_history.assert_called_once_with(silver_module.get_silver_contract("app_users"), changes)
    service.transform.assert_called_once_with(silver_module.get_silver_contract("app_users"), changes)
    service.append_change_history.assert_called_once_with(
        silver_module.get_silver_contract("app_users"),
        history,
        batch_id="batch-1",
        bronze_version=25,
        bronze_starting_version=11,
        transaction_version=25,
    )
    changes.unpersist.assert_called_once_with()


def test_silver_skips_transform_when_delta_version_is_current(monkeypatch: pytest.MonkeyPatch) -> None:
    service = _service()
    service.transform = Mock()
    service._validate_schema_version = Mock()
    _mock_progress(monkeypatch, silver_version=25)

    result = service.run_table("orders", batch_id="batch-2")

    assert result.committed_version == 25
    service.transform.assert_not_called()
    service._validate_schema_version.assert_not_called()
    service.lakehouse.upsert_table.assert_not_called()


def test_silver_rejects_existing_table_without_progress(monkeypatch: pytest.MonkeyPatch) -> None:
    service = _service()
    service._replace_from_snapshot = Mock()
    _mock_progress(monkeypatch, silver_version=None)

    with pytest.raises(RuntimeError, match="Silver progress metadata is missing"):
        service.run_table("orders", batch_id="batch-2")

    service._replace_from_snapshot.assert_not_called()
