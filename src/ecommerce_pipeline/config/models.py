from __future__ import annotations

import re
from typing import Literal
from urllib.parse import urlencode
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from psycopg.conninfo import make_conninfo
from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator

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
    fetch_size: int = Field(default=10000, ge=1)

    @field_validator("source_schema")
    @classmethod
    def validate_schema(cls, value: str) -> str:
        if not _IDENTIFIER.fullmatch(value):
            raise ValueError("source_schema must be a safe PostgreSQL identifier")
        return value

    @property
    def jdbc_url(self) -> str:
        base = f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"
        return f"{base}?{urlencode({'sslmode': self.sslmode})}" if self.sslmode else base

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
    config: dict[str, str] = Field(default_factory=dict)


class LakehouseConfig(FrozenConfigModel):
    base_path: str = Field(min_length=1)
    format: Literal["delta"] = "delta"

    def table_path(self, layer: str, table_name: str) -> str:
        if layer not in {"bronze", "silver", "gold"}:
            raise ValueError(f"Unsupported lakehouse layer: {layer}")
        for value, label in ((layer, "layer"), (table_name, "table_name")):
            if not _IDENTIFIER.fullmatch(value):
                raise ValueError(f"Unsafe {label}: {value}")
        return "/".join((self.base_path.rstrip("/"), layer, table_name))


class AppConfig(FrozenConfigModel):
    application: ApplicationConfig
    postgres: PostgresConfig
    spark: SparkConfig
    lakehouse: LakehouseConfig
    environment: str = Field(min_length=1)
