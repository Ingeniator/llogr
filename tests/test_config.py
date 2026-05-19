import tempfile
from pathlib import Path

import yaml

from llogr.config import S3Config, ClickstreamConfig, ForwardTargetConfig, Settings, load_config


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
        "clickstream": {
            "api_url": "https://cb.example.com/v1/events",
            "api_key": "key-123",
        },
    }
    settings = load_config(_write_config(tmp_path, data))

    assert isinstance(settings, Settings)
    assert isinstance(settings.s3, S3Config)
    assert isinstance(settings.clickstream, ClickstreamConfig)

    assert settings.s3.bucket == "test-bucket"
    assert settings.s3.region == "eu-west-1"
    assert settings.s3.endpoint is None
    assert settings.s3.access_key_id == "AK"
    assert settings.s3.secret_access_key == "SK"

    assert settings.clickstream.api_url == "https://cb.example.com/v1/events"
    assert settings.clickstream.api_key == "key-123"


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


_S3_STUB = {
    "bucket": "b",
    "region": "r",
    "endpoint": None,
    "access_key_id": "a",
    "secret_access_key": "s",
}
_CH_STUB = {"api_url": "http://x", "api_key": "k"}


def test_forward_targets_parsed(tmp_path: Path) -> None:
    data = {
        "s3": _S3_STUB,
        "clickbeat": _CH_STUB,
        "features": {
            "store_backends": ["s3", "clickhouse"],
            "forward": [
                {"url": "http://langfuse-web:3000", "pass_auth": True, "timeout": 10},
                {"url": "http://other:4000", "pass_auth": False, "timeout": 5},
            ],
        },
    }
    settings = load_config(_write_config(tmp_path, data))
    assert len(settings.features.forward) == 2
    assert isinstance(settings.features.forward[0], ForwardTargetConfig)
    assert settings.features.forward[0].url == "http://langfuse-web:3000"
    assert settings.features.forward[0].pass_auth is True
    assert settings.features.forward[1].pass_auth is False
    assert settings.features.forward[1].timeout == 5


def test_forward_targets_default_to_empty(tmp_path: Path) -> None:
    data = {"s3": _S3_STUB, "clickbeat": _CH_STUB}
    settings = load_config(_write_config(tmp_path, data))
    assert settings.features.forward == ()


def test_forward_target_defaults(tmp_path: Path) -> None:
    data = {
        "s3": _S3_STUB,
        "clickbeat": _CH_STUB,
        "features": {
            "forward": [{"url": "http://target:1234"}],
        },
    }
    settings = load_config(_write_config(tmp_path, data))
    target = settings.features.forward[0]
    assert target.pass_auth is True
    assert target.timeout == 10
