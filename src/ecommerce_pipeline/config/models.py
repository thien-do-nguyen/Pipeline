from __future__ import annotations

import re
from dataclasses import dataclass
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
    config: dict[str, str] = Field(default_factory=dict)


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


class AppConfig(FrozenConfigModel):
    application: ApplicationConfig
    postgres: PostgresConfig
    spark: SparkConfig
    lakehouse: LakehouseConfig
    environment: str = Field(min_length=1)
