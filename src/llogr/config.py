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


@dataclass(frozen=True)
class S3Config:
    bucket: str
    region: str
    endpoint: str | None
    access_key_id: str
    secret_access_key: str
    public_endpoint: str | None = None


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


@dataclass(frozen=True)
class Settings:
    s3: S3Config
    clickstream: ClickstreamConfig = ClickstreamConfig()
    server: ServerConfig = ServerConfig()
    features: FeaturesConfig = FeaturesConfig()
    clickhouse: ClickHouseConfig = ClickHouseConfig()


def load_config(path: str | Path) -> Settings:
    raw = yaml.safe_load(Path(path).read_text())
    return Settings(
        s3=S3Config(**raw["s3"]),
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
