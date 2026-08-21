from ecommerce_pipeline.control.batch_flow import BATCH_STAGES


def test_batch_flow_has_one_ordered_contract_for_local_and_cloud() -> None:
    assert [(stage.task_id, stage.pipeline_mode) for stage in BATCH_STAGES] == [
        ("check_source", "check_postgres"),
        ("run_bronze", "bronze"),
        ("run_silver", "silver"),
        ("run_gold", "gold"),
        ("validate_release", "validate_release"),
    ]
    assert len({stage.task_id for stage in BATCH_STAGES}) == len(BATCH_STAGES)
