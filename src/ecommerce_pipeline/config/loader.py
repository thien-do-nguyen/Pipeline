from __future__ import annotations

import os
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv
from pydantic import ValidationError

from ecommerce_pipeline.config.models import AppConfig

_ENV_PATTERN = re.compile(r"\$\{([A-Z][A-Z0-9_]*)(?::-([^}]*))?\}")
PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_BASE_CONFIG = PROJECT_ROOT / "configs" / "base.yaml"


class ConfigError(ValueError):
    """Raised when configuration files or environment variables are invalid."""


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise ConfigError(f"Config file does not exist: {path}")
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, Mapping):
        raise ConfigError(f"Top-level YAML value must be a mapping: {path}")
    return dict(payload)


def _deep_merge(base: Mapping[str, Any], overlay: Mapping[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in overlay.items():
        existing = merged.get(key)
        if isinstance(existing, Mapping) and isinstance(value, Mapping):
            merged[key] = _deep_merge(existing, value)
        else:
            merged[key] = value
    return merged


def _expand_string(value: str) -> str:
    def replacement(match: re.Match[str]) -> str:
        variable, default = match.group(1), match.group(2)
        if variable == "AZURE_STORAGE_HADOOP_AUTH_TYPE":
            return _azure_storage_hadoop_auth_type(default)
        current = os.getenv(variable)
        if current is not None and current != "":
            return current
        if default is not None:
            return default
        raise ConfigError(f"Missing required environment variable: {variable}")

    return _ENV_PATTERN.sub(replacement, value)


def _azure_storage_hadoop_auth_type(default: str | None = None) -> str:
    value = os.getenv("AZURE_STORAGE_AUTH_TYPE") or default
    if value is None or value == "":
        raise ConfigError("Missing required environment variable: AZURE_STORAGE_AUTH_TYPE")
    normalized = value.lower().replace("-", "_")
    if normalized in {"account_key", "shared_key", "sharedkey"}:
        return "SharedKey"
    raise ConfigError(f"Unsupported AZURE_STORAGE_AUTH_TYPE: {value}")


def _expand_environment(value: Any) -> Any:
    if isinstance(value, str):
        return _expand_string(value)
    if isinstance(value, Mapping):
        return {_expand_string(str(key)): _expand_environment(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_expand_environment(item) for item in value]
    return value


def _resolve_overlay(environment_or_path: str | Path) -> Path:
    candidate = Path(environment_or_path)
    if candidate.suffix.lower() in {".yaml", ".yml"}:
        return candidate if candidate.is_absolute() else PROJECT_ROOT / candidate
    return PROJECT_ROOT / "configs" / f"{candidate}.yaml"


def load_config(
    environment_or_path: str | Path = "local",
    *,
    base_path: str | Path = DEFAULT_BASE_CONFIG,
    dotenv_path: str | Path | None = None,
) -> AppConfig:
    """Load base + environment YAML, expand environment variables, then validate."""

    env_file = Path(dotenv_path) if dotenv_path is not None else PROJECT_ROOT / ".env"
    load_dotenv(env_file, override=False)

    base = Path(base_path)
    if not base.is_absolute():
        base = PROJECT_ROOT / base
    overlay = _resolve_overlay(environment_or_path)
    raw = _read_yaml(base)
    if overlay.resolve() != base.resolve():
        raw = _deep_merge(raw, _read_yaml(overlay))

    try:
        return AppConfig.model_validate(_expand_environment(raw))
    except ValidationError as exc:
        raise ConfigError(f"Invalid configuration:\n{exc}") from exc
