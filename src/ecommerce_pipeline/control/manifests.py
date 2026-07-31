from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal


@dataclass(frozen=True)
class BronzeTableResult:
    batch_id: str
    table_name: str
    output_path: str
    record_count: int
    ingestion_type: str
    delta_version: int
    previous_delta_version: int
    operation_counts: dict[str, int] = field(default_factory=dict)
    schema_version: int = 1
    batch_upper_bound_event_id: int | None = None
    previous_event_id: int | None = None
    current_event_id: int | None = None


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

    @property
    def changed_tables(self) -> frozenset[str]:
        return frozenset(name for name, result in self.tables.items() if result.record_count > 0)

    @property
    def committed_versions(self) -> dict[str, int]:
        return {name: result.delta_version for name, result in self.tables.items()}

    @property
    def total_records(self) -> int:
        return sum(result.record_count for result in self.tables.values())


@dataclass(frozen=True)
class SilverTableResult:
    table_name: str
    output_path: str
    processing_mode: Literal["snapshot", "cdf", "no_change"]
    bronze_starting_version: int | None
    bronze_ending_version: int
    committed_version: int
    schema_version: int
    source_record_count: int | None = None
    source_operation_counts: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class SilverBatchManifest:
    batch_id: str
    tables: dict[str, SilverTableResult]

    @property
    def outputs(self) -> list[str]:
        return [result.output_path for result in self.tables.values()]

    @property
    def committed_versions(self) -> dict[str, int]:
        return {name: result.committed_version for name, result in self.tables.items()}

    @property
    def changed_tables(self) -> frozenset[str]:
        return frozenset(name for name, result in self.tables.items() if result.processing_mode != "no_change")


@dataclass(frozen=True)
class GoldCandidateManifest:
    batch_id: str
    changed_tables: frozenset[str]
    previous_versions: dict[str, int]
    committed_versions: dict[str, int]
    silver_versions: dict[str, int]
    quality_status: Literal["PASSED"]
