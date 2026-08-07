from __future__ import annotations

from pathlib import Path

from ecommerce_pipeline.config.models import TableReference


def assert_local_delta_target_matches_checkpoints(
    reference: TableReference,
    checkpoint_locations: tuple[str, ...],
    *,
    target_label: str,
) -> None:
    """Fail fast when local checkpoint state exists but its Delta target was deleted."""

    if reference.is_catalog or _looks_like_cloud_path(reference.value):
        return
    if Path(reference.value, "_delta_log").exists():
        return

    existing_checkpoints = [location for location in checkpoint_locations if _local_checkpoint_exists(location)]
    if not existing_checkpoints:
        return

    checkpoints = ", ".join(existing_checkpoints)
    raise RuntimeError(
        f"{target_label} Delta target is missing but streaming checkpoint state exists. "
        f"target={reference.value} checkpoints={checkpoints}. "
        "Stop run-stream-local and run-silver-stream-local, run 'make cdc-state-reset', then start the streams again."
    )


def _local_checkpoint_exists(location: str) -> bool:
    if _looks_like_cloud_path(location):
        return False
    checkpoint = Path(location)
    return any((checkpoint / child).exists() for child in ("metadata", "offsets", "commits", "sources"))


def _looks_like_cloud_path(value: str) -> bool:
    return "://" in value or value.startswith("dbfs:")
