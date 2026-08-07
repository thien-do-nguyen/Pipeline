from __future__ import annotations

import json
from dataclasses import dataclass

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

from ecommerce_pipeline.adapters.lakehouse import (
    read_delta,
    write_delta,
)
from ecommerce_pipeline.config.models import AppConfig, TableReference
from ecommerce_pipeline.contracts.cdc_tables import TYPED_CDC_TABLES, get_typed_cdc_contract
from ecommerce_pipeline.ingestion.streaming.cdc_decoder import decode_table_events, table_statistics

TYPED_BRONZE_PIPELINE_NAME = "cdc_to_typed_bronze"
QUARANTINE_TABLE_NAME = "quarantine"


@dataclass(frozen=True)
class MaterializedTypedBatch:
    tables: dict[str, DataFrame]
    statistics: dict[str, Row]
    quarantined_count: int


class TypedBronzeMaterializer:
    """Persist validated CDC rows before any downstream Silver transformation."""

    def __init__(self, spark: SparkSession, config: AppConfig) -> None:
        self.spark = spark
        self.config = config
        self.settings = config.streaming.silver
        self._tables_initialized = False

    def materialize(self, prepared: DataFrame, batch_id: int) -> MaterializedTypedBatch:
        self._ensure_physical_tables(prepared)
        quarantined_count = self._write_quarantine(prepared, batch_id)
        statistics = table_statistics(prepared)

        for table_name in statistics:
            valid = prepared.where((F.col("source_table") == table_name) & F.col("_decode_error").isNull())
            typed = decode_table_events(
                valid,
                get_typed_cdc_contract(table_name),
                batch_id=batch_id,
                query_name=self.settings.query_name,
                checkpoint_version=self.settings.checkpoint_version,
                source_system=self.config.application.source_system,
                timezone=self.config.application.timezone,
            )
            self._append_typed(typed, table_name, batch_id, statistics[table_name])

        batch_key = f"{self.settings.query_name}:{self.settings.checkpoint_version}:{batch_id}"
        tables = {
            table_name: read_delta(
                self.spark,
                self.config.lakehouse.streaming_typed_bronze_reference(table_name),
            ).where(F.col("_batch_id") == batch_key)
            for table_name in statistics
        }
        return MaterializedTypedBatch(tables, statistics, quarantined_count)

    def _ensure_physical_tables(self, prepared: DataFrame) -> None:
        if self._tables_initialized:
            return
        empty = prepared.where(F.lit(False))
        for table_name in TYPED_CDC_TABLES:
            reference = self.config.lakehouse.streaming_typed_bronze_reference(table_name)
            if self._exists(reference):
                continue
            typed = decode_table_events(
                empty.where(F.col("source_table") == table_name),
                get_typed_cdc_contract(table_name),
                batch_id=-1,
                query_name=self.settings.query_name,
                checkpoint_version=self.settings.checkpoint_version,
                source_system=self.config.application.source_system,
                timezone=self.config.application.timezone,
            )
            self._append(typed, reference, application_suffix=f"bootstrap-{table_name}", batch_id=0)

        quarantine_reference = self.config.lakehouse.streaming_typed_bronze_reference(QUARANTINE_TABLE_NAME)
        if not self._exists(quarantine_reference):
            self._append(
                self._quarantine_rows(empty, -1),
                quarantine_reference,
                application_suffix="bootstrap-quarantine",
                batch_id=0,
            )
        self._tables_initialized = True

    def _write_quarantine(self, prepared: DataFrame, batch_id: int) -> int:
        rows = self._quarantine_rows(prepared, batch_id).persist(StorageLevel.DISK_ONLY)
        try:
            reasons = rows.groupBy("_quarantine_reason").count().collect()
            count = sum(int(row["count"]) for row in reasons)
            if not count:
                return 0
            reference = self.config.lakehouse.streaming_typed_bronze_reference(QUARANTINE_TABLE_NAME)
            self._append(
                rows,
                reference,
                application_suffix="quarantine",
                batch_id=batch_id,
                metadata={
                    "pipeline": TYPED_BRONZE_PIPELINE_NAME,
                    "stream_batch_id": batch_id,
                    "record_count": count,
                    "output": "quarantine",
                },
            )
            summary = ",".join(f"{row['_quarantine_reason']}:{row['count']}" for row in reasons[:10])
            print(
                f"[cdc-quality-alert] batch={batch_id} quarantined={count} reasons={summary}",
                flush=True,
            )
            return count
        finally:
            rows.unpersist()

    def _quarantine_rows(self, prepared: DataFrame, batch_id: int) -> DataFrame:
        raw_columns = [
            name for name in prepared.columns if name not in {"_payload_json", "_payload_map", "_decode_error"}
        ]
        return prepared.where(F.col("_decode_error").isNotNull()).select(
            *raw_columns,
            F.col("_decode_error").alias("_quarantine_reason"),
            F.current_timestamp().alias("_quarantined_at"),
            F.lit(f"{self.settings.query_name}:{self.settings.checkpoint_version}:{batch_id}").alias("_batch_id"),
        )

    def _append_typed(
        self,
        dataframe: DataFrame,
        table_name: str,
        batch_id: int,
        statistics: Row,
    ) -> None:
        self._append(
            dataframe,
            self.config.lakehouse.streaming_typed_bronze_reference(table_name),
            application_suffix=table_name,
            batch_id=batch_id,
            metadata={
                "pipeline": TYPED_BRONZE_PIPELINE_NAME,
                "source_table": table_name,
                "stream_batch_id": batch_id,
                "record_count": int(statistics["record_count"]),
                "min_source_lsn": statistics["min_source_lsn"],
                "max_source_lsn": statistics["max_source_lsn"],
            },
        )

    def _append(
        self,
        dataframe: DataFrame,
        reference: TableReference,
        *,
        application_suffix: str,
        batch_id: int,
        metadata: dict[str, object] | None = None,
    ) -> None:
        application_id = f"{self.settings.query_name}-{self.settings.checkpoint_version}-typed-{application_suffix}"
        commit_metadata = metadata or {
            "pipeline": TYPED_BRONZE_PIPELINE_NAME,
            "stream_batch_id": batch_id,
            "record_count": 0,
            "output": "bootstrap",
        }
        writer = (
            dataframe.write.format("delta")
            .mode("append")
            .option("delta.enableChangeDataFeed", "true")
            .option("txnAppId", application_id)
            .option("txnVersion", batch_id)
            .option("userMetadata", json.dumps(commit_metadata, separators=(",", ":"), sort_keys=True))
        )
        write_delta(writer, reference)

    def _exists(self, reference: TableReference) -> bool:
        if reference.is_catalog:
            return self.spark.catalog.tableExists(reference.value)
        return DeltaTable.isDeltaTable(self.spark, reference.value)


__all__ = [
    "MaterializedTypedBatch",
    "QUARANTINE_TABLE_NAME",
    "TYPED_BRONZE_PIPELINE_NAME",
    "TypedBronzeMaterializer",
]
