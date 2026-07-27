from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from pyspark.sql import SparkSession

from ecommerce_pipeline.adapters.lakehouse import (
    LakehouseAdapter,
    delta_commit_metadata,
    delta_table_properties,
    set_delta_table_property,
)
from ecommerce_pipeline.config.models import AppConfig
from ecommerce_pipeline.contracts.gold_tables import GOLD_TABLES

GOLD_PIPELINE_NAME = "silver_to_gold"
GOLD_RELEASE_PROPERTY = "pipeline.goldActiveRelease"
LEGACY_GOLD_PUBLISH_MANIFEST = "_publish_manifest"
RELEASE_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class GoldRelease:
    batch_id: str
    silver_versions: dict[str, int]
    gold_versions: dict[str, int]
    legacy: bool = False


class GoldReleaseStore:
    """Atomically publish and resolve version-pinned Gold snapshots."""

    def __init__(self, spark: SparkSession, config: AppConfig) -> None:
        self.spark = spark
        self.config = config
        self.lakehouse = LakehouseAdapter(spark, config)

    def latest(self) -> GoldRelease | None:
        fact_path = self.config.lakehouse.table_path("gold", "fact_sales")
        if self.lakehouse.table_exists("gold", "fact_sales"):
            raw_release = delta_table_properties(self.spark, fact_path).get(GOLD_RELEASE_PROPERTY)
            if raw_release is not None:
                return _release_from_json(raw_release)
        return self._legacy_latest()

    def _legacy_latest(self) -> GoldRelease | None:
        if not self.lakehouse.table_exists("gold", LEGACY_GOLD_PUBLISH_MANIFEST):
            return None
        row = (
            self.lakehouse.read_table("gold", LEGACY_GOLD_PUBLISH_MANIFEST)
            .select(
                "schema_version",
                "pipeline",
                "batch_id",
                "silver_versions",
                "gold_versions",
            )
            .first()
        )
        if row is None:
            raise ValueError("Gold publish manifest is empty")
        if row["schema_version"] != RELEASE_SCHEMA_VERSION:
            raise ValueError(f"Unsupported Gold publish manifest schema: {row['schema_version']}")
        if row["pipeline"] != GOLD_PIPELINE_NAME:
            raise ValueError(f"Unexpected Gold publish pipeline: {row['pipeline']}")
        return GoldRelease(
            batch_id=str(row["batch_id"]),
            silver_versions=_decode_versions(row["silver_versions"], "silver_versions"),
            gold_versions=_decode_versions(row["gold_versions"], "gold_versions", expected=set(GOLD_TABLES)),
            legacy=True,
        )

    def publish(
        self,
        *,
        batch_id: str,
        silver_versions: dict[str, int],
        gold_versions: dict[str, int],
    ) -> GoldRelease:
        _validate_versions(silver_versions, "silver_versions")
        _validate_versions(gold_versions, "gold_versions", expected=set(GOLD_TABLES))
        current_gold_versions = dict(gold_versions)
        fact_path = self.config.lakehouse.table_path("gold", "fact_sales")
        current_gold_versions["fact_sales"] += 1
        release = GoldRelease(
            batch_id=batch_id,
            silver_versions=dict(silver_versions),
            gold_versions=current_gold_versions,
        )
        metadata: dict[str, object] = {
            "schema_version": RELEASE_SCHEMA_VERSION,
            "pipeline": GOLD_PIPELINE_NAME,
            "batch_id": release.batch_id,
            "silver_versions": release.silver_versions,
            "gold_versions": release.gold_versions,
        }
        with delta_commit_metadata(self.spark, metadata):
            set_delta_table_property(
                self.spark,
                fact_path,
                GOLD_RELEASE_PROPERTY,
                json.dumps(metadata, separators=(",", ":"), sort_keys=True),
            )
        return release

    def snapshot(self) -> LakehouseAdapter:
        release = self.latest()
        if release is None:
            raise RuntimeError("Gold has no published release")
        return self.lakehouse.with_gold_versions(release.gold_versions)


def _release_from_json(raw: str) -> GoldRelease:
    try:
        metadata = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("Invalid Gold release marker JSON") from exc
    if not isinstance(metadata, dict):
        raise ValueError("Invalid Gold release marker")
    return _release_from_metadata(metadata)


def _decode_versions(raw: Any, field: str, *, expected: set[str] | None = None) -> dict[str, int]:
    if not isinstance(raw, str):
        raise ValueError(f"Invalid Gold publish manifest field: {field}")
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid Gold publish manifest JSON: {field}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"Invalid Gold publish manifest field: {field}")
    versions = {str(name): version for name, version in value.items()}
    _validate_versions(versions, field, expected=expected)
    return versions


def _release_from_metadata(metadata: dict[str, object]) -> GoldRelease:
    schema_version = metadata.get("schema_version")
    if schema_version != RELEASE_SCHEMA_VERSION:
        raise ValueError(f"Unsupported Gold release schema: {schema_version}")
    batch_id = metadata.get("batch_id")
    if not isinstance(batch_id, str) or not batch_id:
        raise ValueError("Invalid Gold release batch_id")
    silver_versions = _versions_from_value(metadata.get("silver_versions"), "silver_versions")
    gold_versions = _versions_from_value(
        metadata.get("gold_versions"),
        "gold_versions",
        expected=set(GOLD_TABLES),
    )
    return GoldRelease(
        batch_id=batch_id,
        silver_versions=silver_versions,
        gold_versions=gold_versions,
    )


def _versions_from_value(raw: object, field: str, *, expected: set[str] | None = None) -> dict[str, int]:
    if not isinstance(raw, dict):
        raise ValueError(f"Invalid Gold release field: {field}")
    versions = {str(name): version for name, version in raw.items()}
    _validate_versions(versions, field, expected=expected)
    return versions


def _validate_versions(versions: dict[str, int], field: str, *, expected: set[str] | None = None) -> None:
    if any(
        not isinstance(name, str)
        or not name
        or not isinstance(version, int)
        or isinstance(version, bool)
        or version < 0
        for name, version in versions.items()
    ):
        raise ValueError(f"Invalid Gold release versions: {field}")
    if expected is not None and set(versions) != expected:
        raise ValueError(
            f"Gold release table set mismatch: missing={sorted(expected - set(versions))}, "
            f"unexpected={sorted(set(versions) - expected)}"
        )
