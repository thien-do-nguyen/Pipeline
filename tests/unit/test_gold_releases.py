from __future__ import annotations

import json

import pytest

from ecommerce_pipeline.contracts.gold_tables import GOLD_TABLES
from ecommerce_pipeline.control.gold_releases import (
    GOLD_PIPELINE_NAME,
    RELEASE_SCHEMA_VERSION,
    _decode_versions,
    _release_from_json,
    _validate_versions,
)


def test_manifest_versions_round_trip() -> None:
    versions = {name: index for index, name in enumerate(GOLD_TABLES)}

    assert (
        _decode_versions(
            json.dumps(versions),
            "gold_versions",
            expected=set(GOLD_TABLES),
        )
        == versions
    )


@pytest.mark.parametrize(
    "versions",
    [
        {"fact_sales": -1},
        {"fact_sales": True},
    ],
)
def test_manifest_rejects_invalid_delta_versions(versions: dict[str, int]) -> None:
    with pytest.raises(ValueError, match="Invalid Gold release versions"):
        _validate_versions(versions, "gold_versions")


def test_manifest_requires_the_complete_gold_table_set() -> None:
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
