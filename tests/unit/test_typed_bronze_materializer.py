import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from ecommerce_pipeline.config.models import TableReference
from ecommerce_pipeline.ingestion.streaming import typed_bronze
from ecommerce_pipeline.ingestion.streaming.typed_bronze import TypedBronzeMaterializer


def test_append_sets_transaction_on_dataframe_writer_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    materializer = object.__new__(TypedBronzeMaterializer)
    materializer.settings = SimpleNamespace(query_name="cdc-to-silver", checkpoint_version="v3")
    dataframe = Mock()
    writer = dataframe.write.format.return_value
    writer.mode.return_value = writer
    writer.option.return_value = writer
    write_delta = Mock()
    monkeypatch.setattr(typed_bronze, "write_delta", write_delta)
    metadata = {"pipeline": "cdc_to_typed_bronze", "stream_batch_id": 7}
    reference = TableReference("/tmp/orders", is_catalog=False)

    materializer._append(
        dataframe,
        reference,
        application_suffix="orders",
        batch_id=7,
        metadata=metadata,
    )

    assert writer.option.call_args_list[-3].args == (
        "txnAppId",
        "cdc-to-silver-v3-typed-orders",
    )
    assert writer.option.call_args_list[-2].args == ("txnVersion", 7)
    assert writer.option.call_args_list[-1].args == (
        "userMetadata",
        json.dumps(metadata, separators=(",", ":"), sort_keys=True),
    )
    write_delta.assert_called_once_with(writer, reference)
