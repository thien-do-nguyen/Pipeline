from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal
from urllib.parse import urlencode
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from psycopg.conninfo import make_conninfo
from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator, model_validator

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class FrozenConfigModel(BaseModel):
    """Base class shared by every validated configuration section."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class ApplicationConfig(FrozenConfigModel):
    name: str = Field(min_length=1)
    source_system: str = Field(min_length=1)
    timezone: str = Field(min_length=1)
    logs_path: str = Field(default="logs", min_length=1)

    @field_validator("timezone")
    @classmethod
    def validate_timezone(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except ZoneInfoNotFoundError as exc:
            raise ValueError(f"Unknown IANA timezone: {value}") from exc
        return value


class PostgresConfig(FrozenConfigModel):
    host: str = Field(min_length=1)
    port: int = Field(ge=1, le=65535)
    database: str = Field(min_length=1)
    user: str = Field(min_length=1)
    password: SecretStr
    source_schema: str
    jdbc_driver: str = Field(min_length=1)
    sslmode: str | None = None
    fetch_size: int = Field(default=10000, ge=1, le=1_000_000)
    query_timeout_seconds: int = Field(default=300, ge=1, le=86_400)
    connect_timeout_seconds: int = Field(default=15, ge=1, le=300)
    socket_timeout_seconds: int = Field(default=300, ge=1, le=86_400)
    max_jdbc_partitions: int = Field(default=4, ge=1, le=32)
    target_events_per_partition: int = Field(default=100_000, ge=1)
    max_events_per_batch: int = Field(default=500_000, ge=1)
    retry_attempts: int = Field(default=3, ge=1, le=10)
    retry_initial_backoff_seconds: float = Field(default=2, ge=0, le=300)

    @field_validator("source_schema")
    @classmethod
    def validate_schema(cls, value: str) -> str:
        if not _IDENTIFIER.fullmatch(value):
            raise ValueError("source_schema must be a safe PostgreSQL identifier")
        return value

    @property
    def jdbc_url(self) -> str:
        base = f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"
        parameters = {
            "connectTimeout": self.connect_timeout_seconds,
            "socketTimeout": self.socket_timeout_seconds,
            "tcpKeepAlive": "true",
        }
        if self.sslmode:
            parameters["sslmode"] = self.sslmode
        return f"{base}?{urlencode(parameters)}"

    @property
    def password_value(self) -> str:
        return self.password.get_secret_value()

    @property
    def psycopg_dsn(self) -> str:
        conninfo = make_conninfo(
            "",
            host=self.host,
            port=str(self.port),
            dbname=self.database,
            user=self.user,
            password=self.password_value,
        )
        if self.sslmode:
            conninfo = make_conninfo(conninfo, sslmode=self.sslmode)
        return conninfo


class SparkConfig(FrozenConfigModel):
    master: str | None = None
    app_name: str = Field(min_length=1)
    configure_delta_package: bool = True
    stop_session: bool = True
    max_parallel_tables: int = Field(default=1, ge=1, le=16)
    config: dict[str, str] = Field(default_factory=dict)


class KafkaSourceConfig(FrozenConfigModel):
    """Kafka protocol settings shared by local Kafka and Azure Event Hubs."""

    bootstrap_servers: str = Field(min_length=1)
    topic: str | None = Field(default=None, min_length=1)
    topic_pattern: str | None = Field(default=None, min_length=1)
    starting_offsets: str = Field(default="earliest", min_length=1)
    fail_on_data_loss: bool = True
    max_offsets_per_trigger: int = Field(default=100_000, ge=1)
    options: dict[str, str] = Field(default_factory=dict)
    secret_options: dict[str, SecretStr] = Field(default_factory=dict)

    @field_validator("topic")
    @classmethod
    def validate_topic(cls, value: str) -> str:
        if not re.fullmatch(r"[A-Za-z0-9._-]+", value):
            raise ValueError("topic contains unsupported characters")
        return value

    @model_validator(mode="after")
    def validate_subscription_mode(self) -> KafkaSourceConfig:
        if (self.topic is None) == (self.topic_pattern is None):
            raise ValueError("Configure exactly one of kafka.topic or kafka.topic_pattern")
        return self

    @property
    def subscribe_key(self) -> Literal["subscribe", "subscribePattern"]:
        return "subscribe" if self.topic is not None else "subscribePattern"

    @property
    def subscription(self) -> str:
        return self.topic if self.topic is not None else str(self.topic_pattern)

    def spark_options(self) -> dict[str, str]:
        options = {
            "kafka.bootstrap.servers": self.bootstrap_servers,
            self.subscribe_key: self.subscription,
            "startingOffsets": self.starting_offsets,
            "failOnDataLoss": str(self.fail_on_data_loss).lower(),
            "maxOffsetsPerTrigger": str(self.max_offsets_per_trigger),
            "includeHeaders": "true",
        }
        options.update(self.options)
        options.update({key: value.get_secret_value() for key, value in self.secret_options.items()})
        return options


class UnifiedSilverStreamingConfig(FrozenConfigModel):
    """Settings for the raw CDC to shared Silver/Gold query."""

    enabled: bool = False
    query_name: str = Field(default="ecommerce-cdc-to-silver", min_length=1)
    checkpoint_version: str = Field(default="v1", pattern=r"^v[1-9][0-9]*$")
    trigger_interval: str = Field(default="1 minute", min_length=1)
    max_files_per_trigger: int = Field(default=8, ge=1, le=10_000)
    max_bytes_per_trigger: str = Field(default="32m", pattern=r"^[1-9][0-9]*[kKmMgG]$")
    reconcile_gold_each_batch: bool = False
    max_gold_readiness_order_ids: int = Field(default=5_000, ge=1, le=1_000_000)
    gold_deferred_retry_seconds: int = Field(default=0, ge=0, le=600)
    gold_deferred_retry_interval_seconds: float = Field(default=2.0, gt=0, le=60)


class PipelineCoordinationConfig(FrozenConfigModel):
    """Single-writer policy shared by batch and streaming runtimes."""

    gold_owner: Literal["batch", "streaming"] = "batch"
    cloud_lock_name: str = Field(default="unified_silver_writer", min_length=1)
    cloud_lock_ttl_seconds: int = Field(default=21_600, ge=300, le=86_400)
    lock_wait_seconds: int = Field(default=600, ge=0, le=3600)

    @field_validator("cloud_lock_name")
    @classmethod
    def validate_cloud_lock_name(cls, value: str) -> str:
        if not _IDENTIFIER.fullmatch(value):
            raise ValueError("cloud_lock_name must be a safe identifier")
        return value


class StreamingConfig(FrozenConfigModel):
    """Validated runtime settings for PostgreSQL CDC ingestion."""

    enabled: bool = False
    query_name: str = Field(default="ecommerce-cdc-to-bronze", min_length=1)
    bronze_table: str = Field(default="cdc_events", min_length=1)
    checkpoint_root: str | None = Field(default=None, min_length=1)
    checkpoint_version: str = Field(default="v1", pattern=r"^v[1-9][0-9]*$")
    trigger_interval: str = Field(default="30 seconds", min_length=1)
    spark_packages: tuple[str, ...] = ()
    kafka: KafkaSourceConfig | None = None
    silver: UnifiedSilverStreamingConfig = Field(default_factory=UnifiedSilverStreamingConfig)

    @field_validator("bronze_table")
    @classmethod
    def validate_bronze_table(cls, value: str) -> str:
        if not _IDENTIFIER.fullmatch(value):
            raise ValueError("bronze_table must be a safe identifier")
        return value

    @model_validator(mode="after")
    def validate_enabled_settings(self) -> StreamingConfig:
        if self.enabled and (self.kafka is None or self.checkpoint_root is None):
            raise ValueError("Enabled streaming requires kafka and checkpoint_root")
        if self.silver.enabled and self.checkpoint_root is None:
            raise ValueError("Enabled streaming Silver requires streaming.checkpoint_root")
        return self

    @property
    def checkpoint_location(self) -> str:
        if self.checkpoint_root is None:
            raise ValueError("Streaming checkpoint_root is not configured")
        return "/".join((self.checkpoint_root.rstrip("/"), self.query_name, self.checkpoint_version))

    @property
    def silver_checkpoint_location(self) -> str:
        if self.checkpoint_root is None:
            raise ValueError("Streaming checkpoint_root is not configured")
        return "/".join(
            (
                self.checkpoint_root.rstrip("/"),
                self.silver.query_name,
                self.silver.checkpoint_version,
            )
        )


@dataclass(frozen=True)
class TableReference:
    value: str
    is_catalog: bool
    storage_path: str | None = None


class LakehouseConfig(FrozenConfigModel):
    base_path: str | None = Field(default=None, min_length=1)
    catalog: str | None = Field(default=None, min_length=1)
    schemas: dict[str, str] = Field(default_factory=dict)
    external_root: str | None = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def validate_storage_mode(self) -> LakehouseConfig:
        using_paths = self.base_path is not None
        using_catalog = self.catalog is not None
        if using_paths == using_catalog:
            raise ValueError("Configure exactly one of lakehouse.base_path or lakehouse.catalog")
        if using_catalog:
            if not _IDENTIFIER.fullmatch(self.catalog or ""):
                raise ValueError("lakehouse.catalog must be a safe Unity Catalog identifier")
            if set(self.schemas) != {"bronze", "silver", "gold"}:
                raise ValueError("lakehouse.schemas must define exactly bronze, silver, and gold")
            for layer, schema in self.schemas.items():
                if not _IDENTIFIER.fullmatch(schema):
                    raise ValueError(f"Unsafe Unity Catalog schema for {layer}: {schema}")
        elif self.schemas:
            raise ValueError("lakehouse.schemas is only valid with lakehouse.catalog")
        if self.external_root is not None and not using_catalog:
            raise ValueError("lakehouse.external_root requires lakehouse.catalog")
        return self

    def table_reference(self, layer: str, table_name: str) -> TableReference:
        if layer not in {"bronze", "silver", "gold"}:
            raise ValueError(f"Unsupported lakehouse layer: {layer}")
        for value, label in ((layer, "layer"), (table_name, "table_name")):
            if not _IDENTIFIER.fullmatch(value):
                raise ValueError(f"Unsafe {label}: {value}")
        if self.catalog is not None:
            return TableReference(
                value=f"{self.catalog}.{self.schemas[layer]}.{table_name}",
                is_catalog=True,
                storage_path=(
                    None
                    if self.external_root is None
                    else "/".join((self.external_root.rstrip("/"), layer, table_name))
                ),
            )
        assert self.base_path is not None
        parts = [self.base_path.rstrip("/"), layer]
        if layer == "bronze":
            parts.append("batch")
        parts.append(table_name)
        return TableReference(value="/".join(parts), is_catalog=False)

    def streaming_bronze_reference(self, table_name: str) -> TableReference:
        """Resolve the raw CDC table without changing the batch Bronze layout."""

        if not _IDENTIFIER.fullmatch(table_name):
            raise ValueError(f"Unsafe table_name: {table_name}")
        if self.catalog is not None:
            return TableReference(
                value=f"{self.catalog}.{self.schemas['bronze']}.{table_name}",
                is_catalog=True,
                storage_path=(
                    None
                    if self.external_root is None
                    else "/".join((self.external_root.rstrip("/"), "bronze", "streaming", table_name))
                ),
            )
        assert self.base_path is not None
        return TableReference(
            value="/".join((self.base_path.rstrip("/"), "bronze", "streaming", table_name)),
            is_catalog=False,
        )

    def streaming_typed_bronze_reference(self, table_name: str) -> TableReference:
        """Resolve typed CDC without colliding with batch Bronze in Unity Catalog."""

        if not _IDENTIFIER.fullmatch(table_name):
            raise ValueError(f"Unsafe table_name: {table_name}")
        if self.catalog is not None:
            return TableReference(
                value=f"{self.catalog}.{self.schemas['bronze']}.cdc_typed_{table_name}",
                is_catalog=True,
                storage_path=(
                    None
                    if self.external_root is None
                    else "/".join((self.external_root.rstrip("/"), "bronze", "streaming", table_name))
                ),
            )
        assert self.base_path is not None
        return TableReference(
            value="/".join((self.base_path.rstrip("/"), "bronze", "streaming", table_name)),
            is_catalog=False,
        )

    def coordination_reference(self) -> TableReference:
        """Resolve the Delta-backed cross-job advisory lock table."""

        if self.catalog is not None:
            return TableReference(
                value=f"{self.catalog}.{self.schemas['silver']}._pipeline_writer_locks",
                is_catalog=True,
                storage_path=(
                    None
                    if self.external_root is None
                    else "/".join((self.external_root.rstrip("/"), "_control", "pipeline_writer_locks"))
                ),
            )
        assert self.base_path is not None
        return TableReference(
            value="/".join((self.base_path.rstrip("/"), "_control", "pipeline_writer_locks")),
            is_catalog=False,
        )


class AppConfig(FrozenConfigModel):
    application: ApplicationConfig
    postgres: PostgresConfig
    spark: SparkConfig
    lakehouse: LakehouseConfig
    streaming: StreamingConfig = Field(default_factory=StreamingConfig)
    coordination: PipelineCoordinationConfig = Field(default_factory=PipelineCoordinationConfig)
    environment: str = Field(min_length=1)
