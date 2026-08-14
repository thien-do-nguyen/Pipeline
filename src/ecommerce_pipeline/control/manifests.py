from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Literal


@dataclass(frozen=True)
class BronzeTableResult:
    batch_id: str
    table_name: str
    record_count: int
    ingestion_type: str
    delta_version: int
    operation_counts: dict[str, int] = field(default_factory=dict)
    schema_version: int = 1


@dataclass(frozen=True)
class BronzeBatchManifest:
    batch_id: str
    tables: dict[str, BronzeTableResult]

    @classmethod
    def from_results(cls, batch_id: str, results: list[BronzeTableResult]) -> BronzeBatchManifest:
        tables = {result.table_name: result for result in results}
        if len(tables) != len(results):
            raise ValueError("Bronze manifest contains duplicate table names")
        if any(result.batch_id != batch_id for result in results):
            raise ValueError("Bronze manifest contains a mismatched batch_id")
        return cls(batch_id=batch_id, tables=tables)

    @property
    def results(self) -> list[BronzeTableResult]:
        return list(self.tables.values())


@dataclass(frozen=True)
class SilverTableResult:
    table_name: str
    committed_version: int
    schema_version: int


@dataclass(frozen=True)
class SilverBatchManifest:
    tables: dict[str, SilverTableResult]

    @property
    def committed_versions(self) -> dict[str, int]:
        return {name: result.committed_version for name, result in self.tables.items()}


PipelineManifest = BronzeBatchManifest | SilverBatchManifest


def serialize_manifest(manifest: PipelineManifest) -> str:
    """Serialize compact control metadata for an orchestrator such as Airflow XCom."""

    if isinstance(manifest, BronzeBatchManifest):
        payload: dict[str, object] = {
            "type": "bronze",
            "batch_id": manifest.batch_id,
            "tables": {
                name: {
                    "record_count": result.record_count,
                    "ingestion_type": result.ingestion_type,
                    "delta_version": result.delta_version,
                    "operation_counts": result.operation_counts,
                    "schema_version": result.schema_version,
                }
                for name, result in manifest.tables.items()
            },
        }
    else:
        payload = {
            "type": "silver",
            "tables": {
                name: {
                    "committed_version": result.committed_version,
                    "schema_version": result.schema_version,
                }
                for name, result in manifest.tables.items()
            },
        }
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def deserialize_manifest(raw: str) -> PipelineManifest:
    """Validate metadata received from an external task boundary."""

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("Invalid upstream manifest JSON") from exc
    if not isinstance(payload, dict) or not isinstance(payload.get("tables"), dict):
        raise ValueError("Invalid upstream manifest payload")
    manifest_type = payload.get("type")
    if manifest_type == "bronze":
        batch_id = payload.get("batch_id")
        if not isinstance(batch_id, str) or not batch_id:
            raise ValueError("Invalid Bronze manifest batch_id")
        results = [
            BronzeTableResult(
                batch_id=batch_id,
                table_name=_table_name(name),
                record_count=_non_negative_int(values, "record_count", name),
                ingestion_type=_string_field(values, "ingestion_type", name),
                delta_version=_non_negative_int(values, "delta_version", name),
                operation_counts=_operation_counts(values, name),
                schema_version=_non_negative_int(values, "schema_version", name),
            )
            for name, values in payload["tables"].items()
        ]
        return BronzeBatchManifest.from_results(batch_id, results)
    if manifest_type == "silver":
        tables = {
            _table_name(name): SilverTableResult(
                table_name=_table_name(name),
                committed_version=_non_negative_int(values, "committed_version", name),
                schema_version=_non_negative_int(values, "schema_version", name),
            )
            for name, values in payload["tables"].items()
        }
        return SilverBatchManifest(tables=tables)
    raise ValueError(f"Unsupported upstream manifest type: {manifest_type!r}")


def _table_name(value: object) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError("Invalid manifest table name")
    return value


def _table_values(value: object, table_name: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"Invalid manifest table metadata: {table_name!r}")
    return value


def _non_negative_int(values: object, field_name: str, table_name: object) -> int:
    value = _table_values(values, table_name).get(field_name)
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"Invalid manifest {field_name}: {table_name!r}")
    return value


def _string_field(values: object, field_name: str, table_name: object) -> str:
    value = _table_values(values, table_name).get(field_name)
    if not isinstance(value, str) or not value:
        raise ValueError(f"Invalid manifest {field_name}: {table_name!r}")
    return value


def _operation_counts(values: object, table_name: object) -> dict[str, int]:
    raw = _table_values(values, table_name).get("operation_counts")
    if not isinstance(raw, dict) or any(
        not isinstance(name, str)
        or not isinstance(count, int)
        or isinstance(count, bool)
        or count < 0
        for name, count in raw.items()
    ):
        raise ValueError(f"Invalid manifest operation_counts: {table_name!r}")
    return raw


@dataclass(frozen=True)
class GoldCandidateManifest:
    batch_id: str
    changed_tables: frozenset[str]
    committed_versions: dict[str, int]
    silver_versions: dict[str, int]
    quality_status: Literal["PASSED"]
