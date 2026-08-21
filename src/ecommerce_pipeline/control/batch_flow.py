from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BatchStage:
    task_id: str
    pipeline_mode: str


BATCH_STAGES = (
    BatchStage("check_source", "check_postgres"),
    BatchStage("run_bronze", "bronze"),
    BatchStage("run_silver", "silver"),
    BatchStage("run_gold", "gold"),
    BatchStage("validate_release", "validate_release"),
)

__all__ = ["BATCH_STAGES", "BatchStage"]
