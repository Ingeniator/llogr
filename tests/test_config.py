import tempfile
from pathlib import Path

import yaml

from llogr.config import S3Config, ClickbeatConfig, Settings, load_config


def _write_config(tmp_path: Path, data: dict) -> Path:
    p = tmp_path / "config.yaml"
    p.write_text(yaml.dump(data))
    return p


def test_load_config(tmp_path: Path) -> None:
    data = {
        "s3": {
            "bucket": "test-bucket",
            "region": "eu-west-1",
            "endpoint": None,
            "access_key_id": "AK",
            "secret_access_key": "SK",
        },
        "clickbeat": {
            "api_url": "https://cb.example.com/v1/events",
            "api_key": "key-123",
        },
    }
    settings = load_config(_write_config(tmp_path, data))

    assert isinstance(settings, Settings)
    assert isinstance(settings.s3, S3Config)
    assert isinstance(settings.clickbeat, ClickbeatConfig)

    assert settings.s3.bucket == "test-bucket"
    assert settings.s3.region == "eu-west-1"
    assert settings.s3.endpoint is None
    assert settings.s3.access_key_id == "AK"
    assert settings.s3.secret_access_key == "SK"

    assert settings.clickbeat.api_url == "https://cb.example.com/v1/events"
    assert settings.clickbeat.api_key == "key-123"


def test_settings_are_frozen(tmp_path: Path) -> None:
    data = {
        "s3": {
            "bucket": "b",
            "region": "r",
            "endpoint": None,
            "access_key_id": "a",
            "secret_access_key": "s",
        },
        "clickbeat": {"api_url": "http://x", "api_key": "k"},
    }
    settings = load_config(_write_config(tmp_path, data))
    try:
        settings.s3 = None  # type: ignore[misc]
        assert False, "Should have raised"
    except AttributeError:
        pass


def test_load_config_with_custom_endpoint(tmp_path: Path) -> None:
    data = {
        "s3": {
            "bucket": "b",
            "region": "r",
            "endpoint": "http://localhost:5000",
            "access_key_id": "a",
            "secret_access_key": "s",
        },
        "clickbeat": {"api_url": "http://x", "api_key": "k"},
    }
    settings = load_config(_write_config(tmp_path, data))
    assert settings.s3.endpoint == "http://localhost:5000"
