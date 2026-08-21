from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from ecommerce_pipeline.contracts.gold_tables import GOLD_TABLES
from ecommerce_pipeline.contracts.silver_tables import SILVER_TABLES
from ecommerce_pipeline.control import gold_releases
from ecommerce_pipeline.control.gold_releases import (
    GOLD_PIPELINE_NAME,
    GOLD_RELEASE_PROPERTY,
    RELEASE_SCHEMA_VERSION,
    GoldReleaseStore,
    _release_from_json,
    _validate_versions,
)
from ecommerce_pipeline.control.manifests import SilverBatchManifest, SilverTableResult
from ecommerce_pipeline.validation import batch as batch_validation


@pytest.mark.parametrize(
    "versions",
    [
        {"fact_sales": -1},
        {"fact_sales": True},
    ],
)
def test_release_marker_rejects_invalid_delta_versions(versions: dict[str, int]) -> None:
    with pytest.raises(ValueError, match="Invalid Gold release versions"):
        _validate_versions(versions, "gold_versions")


def test_release_marker_requires_the_complete_gold_table_set() -> None:
    with pytest.raises(ValueError, match="table set mismatch"):
        _validate_versions({"fact_sales": 1}, "gold_versions", expected=set(GOLD_TABLES))


def test_delta_release_marker_round_trip() -> None:
    gold_versions = {name: index for index, name in enumerate(GOLD_TABLES)}
    metadata: dict[str, object] = {
        "schema_version": RELEASE_SCHEMA_VERSION,
        "pipeline": GOLD_PIPELINE_NAME,
        "batch_id": "batch-1",
        "silver_versions": {"orders": 7},
        "gold_versions": gold_versions,
    }

    release = _release_from_json(json.dumps(metadata))

    assert release.batch_id == "batch-1"
    assert release.silver_versions == {"orders": 7}
    assert release.gold_versions == gold_versions


def test_latest_release_reads_properties_once_without_separate_existence_check(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    metadata = {
        "schema_version": RELEASE_SCHEMA_VERSION,
        "pipeline": GOLD_PIPELINE_NAME,
        "batch_id": "batch-1",
        "silver_versions": {"orders": 7},
        "gold_versions": {name: index for index, name in enumerate(GOLD_TABLES)},
    }
    read_properties = Mock(return_value={GOLD_RELEASE_PROPERTY: json.dumps(metadata)})
    monkeypatch.setattr(gold_releases, "try_delta_table_properties", read_properties)
    config = SimpleNamespace(lakehouse=SimpleNamespace(table_reference=lambda _layer, _table: "gold/fact_sales"))

    release = GoldReleaseStore(Mock(), config).latest()

    assert release is not None
    assert release.batch_id == "batch-1"
    read_properties.assert_called_once()


def test_release_validation_accepts_progress_that_superseded_the_batch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = {name: 4 for name in SILVER_TABLES}
    published = dict(expected)
    published["app_users"] = 5
    release = gold_releases.GoldRelease(
        batch_id="newer-writer",
        silver_versions=published,
        gold_versions={name: index for index, name in enumerate(GOLD_TABLES)},
    )
    store = Mock()
    store.latest.return_value = release
    monkeypatch.setattr(batch_validation, "GoldReleaseStore", Mock(return_value=store))
    manifest = SilverBatchManifest(
        tables={
            name: SilverTableResult(name, committed_version=version, schema_version=2)
            for name, version in expected.items()
        }
    )

    report = batch_validation.validate_gold_release(Mock(), Mock(), manifest)

    assert report["batch_id"] == "newer-writer"
    assert report["superseded"] is True


def test_release_validation_rejects_progress_behind_the_batch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = {name: 4 for name in SILVER_TABLES}
    published = dict(expected)
    published["orders"] = 3
    release = gold_releases.GoldRelease(
        batch_id="lagging-writer",
        silver_versions=published,
        gold_versions={name: index for index, name in enumerate(GOLD_TABLES)},
    )
    store = Mock()
    store.latest.return_value = release
    monkeypatch.setattr(batch_validation, "GoldReleaseStore", Mock(return_value=store))
    manifest = SilverBatchManifest(
        tables={
            name: SilverTableResult(name, committed_version=version, schema_version=2)
            for name, version in expected.items()
        }
    )

    with pytest.raises(ValueError, match="Gold progress is behind"):
        batch_validation.validate_gold_release(Mock(), Mock(), manifest)


def test_release_validation_uses_atomic_gold_control_state_when_xcom_manifest_is_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = {name: 4 for name in SILVER_TABLES}
    release = gold_releases.GoldRelease(
        batch_id="delta-controlled",
        silver_versions=expected,
        gold_versions={name: index for index, name in enumerate(GOLD_TABLES)},
    )
    store = Mock()
    store.latest.return_value = release
    monkeypatch.setattr(batch_validation, "GoldReleaseStore", Mock(return_value=store))

    report = batch_validation.validate_gold_release(Mock(), Mock())

    assert report["batch_id"] == "delta-controlled"
    assert report["superseded"] is False
    assert report["validation_source"] == "gold_release"
