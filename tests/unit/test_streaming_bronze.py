from unittest.mock import Mock

import pytest

from ecommerce_pipeline.config.models import TableReference
from ecommerce_pipeline.ingestion.streaming import bronze


def test_missing_bronze_target_is_created_with_cdf(monkeypatch: pytest.MonkeyPatch) -> None:
    df = Mock()
    empty = df.sparkSession.createDataFrame.return_value
    writer = empty.write.format.return_value
    writer.mode.return_value = writer
    writer.option.return_value = writer
    reference = TableReference("data/lakehouse/bronze/streaming/cdc_events", False)
    write_delta = Mock()
    monkeypatch.setattr(bronze, "_is_delta_path", Mock(return_value=False))
    monkeypatch.setattr(bronze, "write_delta", write_delta)

    bronze.ensure_bronze_target(df, reference)

    writer.option.assert_called_once_with(bronze.CHANGE_DATA_FEED_PROPERTY, "true")
    write_delta.assert_called_once_with(writer, reference)


def test_existing_bronze_target_enables_cdf_when_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    df = Mock()
    reference = TableReference("data/lakehouse/bronze/streaming/cdc_events", False)
    set_property = Mock()
    monkeypatch.setattr(bronze, "_is_delta_path", Mock(return_value=True))
    monkeypatch.setattr(bronze, "delta_table_properties", Mock(return_value={}))
    monkeypatch.setattr(bronze, "set_delta_table_property", set_property)

    bronze.ensure_bronze_target(df, reference)

    set_property.assert_called_once_with(
        df.sparkSession,
        reference,
        bronze.CHANGE_DATA_FEED_PROPERTY,
        "true",
    )
