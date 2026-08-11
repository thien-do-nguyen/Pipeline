from __future__ import annotations

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


@dataclass(frozen=True)
class GoldCandidateManifest:
    batch_id: str
    changed_tables: frozenset[str]
    committed_versions: dict[str, int]
    silver_versions: dict[str, int]
    quality_status: Literal["PASSED"]
