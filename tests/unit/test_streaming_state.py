from __future__ import annotations

from pathlib import Path

import pytest

from ecommerce_pipeline.config.models import TableReference
from ecommerce_pipeline.ingestion.streaming.state import assert_local_delta_target_matches_checkpoints


def test_missing_local_delta_target_with_checkpoint_fails_fast(tmp_path: Path) -> None:
    target = TableReference(str(tmp_path / "lakehouse" / "bronze" / "streaming" / "cdc_events"), False)
    checkpoint = tmp_path / "checkpoints" / "ecommerce-cdc-to-bronze" / "v2"
    checkpoint.mkdir(parents=True)
    (checkpoint / "metadata").write_text("{}", encoding="utf-8")

    with pytest.raises(RuntimeError, match="make cdc-state-reset"):
        assert_local_delta_target_matches_checkpoints(
            target,
            (str(checkpoint),),
            target_label="Raw CDC Bronze",
        )


def test_missing_local_delta_target_without_checkpoint_is_allowed(tmp_path: Path) -> None:
    target = TableReference(str(tmp_path / "lakehouse" / "bronze" / "streaming" / "cdc_events"), False)

    assert_local_delta_target_matches_checkpoints(
        target,
        (str(tmp_path / "checkpoints" / "missing"),),
        target_label="Raw CDC Bronze",
    )
