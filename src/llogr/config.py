from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import structlog
import yaml

logger = structlog.get_logger(__name__)


def _find_config() -> Path:
    if env := os.environ.get("LLOGR_CONFIG"):
        return Path(env)
    src_relative = Path(__file__).resolve().parents[2] / "config.yaml"
    if src_relative.exists():
        return src_relative
    return Path("config.yaml")


CONFIG_PATH = _find_config()
VAULT_SECRETS_PATH = os.environ.get("VAULT_SECRETS_PATH", "/vault/secrets/env")


def _load_vault_secrets(path: str | Path) -> dict[str, str]:
    """Load secrets from a vault sidecar file.

    Supported formats (auto-detected):
        KEY=value
        export KEY=value
        KEY: value
    """
    p = Path(path)
    if not p.exists():
        return {}
    secrets = {}
    for line in p.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:]
        if ": " in line:
            key, _, value = line.partition(": ")
        elif "=" in line:
            key, _, value = line.partition("=")
        else:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        secrets[key.strip()] = value
    return secrets


def _resolve_vault_refs(text: str, secrets: dict[str, str]) -> str:
    """Replace vault:KEY references with values from the vault secrets file."""
    for key, value in secrets.items():
        text = text.replace(f"vault:{key}", value)
    return text


@dataclass(frozen=True)
class S3Config:
    bucket: str
    region: str
    endpoint: str | None
    access_key_id: str
    secret_access_key: str
    public_endpoint: str | None = None
    key_prefix: str = ""
    addressing_style: str = "virtual"
    presign_expiry: int = 3600
    cors_origins: tuple[str, ...] = ()


@dataclass(frozen=True)
class ClickstreamConfig:
    api_url: str = ""       # POST /2/httpapi (Amplitude format)
    api_key: str = ""


@dataclass(frozen=True)
class ClickHouseConfig:
    url: str = ""
    database: str = "default"
    table: str = "llogr_events"
    user: str = "default"
    password: str = ""


@dataclass(frozen=True)
class FeaturesConfig:
    # Store backends — where to send events on ingestion
    # Any combination of: "s3", "clickhouse", "clickstream"
    store_backends: tuple[str, ...] = ("s3",)
    # Search
    search_enabled: bool = False
    search_backend: str = "duckdb"  # "duckdb" or "clickhouse"


@dataclass(frozen=True)
class ServerConfig:
    root_path: str = ""
    host: str = "0.0.0.0"
    port: int = 8000
    workers: int = 1
    timeout_keep_alive: int = 65
    debug: bool = False
    silence_probes: bool = True


@dataclass(frozen=True)
class Settings:
    s3: S3Config
    clickstream: ClickstreamConfig = ClickstreamConfig()
    server: ServerConfig = ServerConfig()
    features: FeaturesConfig = FeaturesConfig()
    clickhouse: ClickHouseConfig = ClickHouseConfig()


def load_config(path: str | Path) -> Settings:
    text = Path(path).read_text()
    text = _resolve_vault_refs(text, _load_vault_secrets(VAULT_SECRETS_PATH))
    text = os.path.expandvars(text)
    raw = yaml.safe_load(text)
    return Settings(
        s3=S3Config(**{
            **raw["s3"],
            "cors_origins": tuple(raw["s3"].get("cors_origins", ())),
        }),
        clickstream=ClickstreamConfig(**raw.get("clickstream", {})),
        server=ServerConfig(**raw.get("server", {})),
        features=FeaturesConfig(**{
            **raw.get("features", {}),
            "store_backends": tuple(raw.get("features", {}).get("store_backends", ("s3",))),
        }),
        clickhouse=ClickHouseConfig(**raw.get("clickhouse", {})),
    )


@lru_cache
def get_settings() -> Settings:
    return load_config(CONFIG_PATH)
