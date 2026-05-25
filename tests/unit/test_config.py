from pathlib import Path

import pytest
from pydantic import ValidationError

from core.config import RedisSettings, load_settings


def test_load_settings_merges_yaml_and_environment(tmp_path: Path) -> None:
    config_file = tmp_path / "settings.yaml"
    env_file = tmp_path / ".env"
    config_file.write_text(
        """
app_name: from-yaml
redis:
  stream_max_len: 250
logging:
  json: true
""",
        encoding="utf-8",
    )
    env_file.write_text("MICROTRADE_LOGGING__LEVEL=debug\n", encoding="utf-8")

    settings = load_settings(
        config_path=config_file,
        env_file=env_file,
        environ={"MICROTRADE_REDIS__STREAM_MAX_LEN": "500"},
    )

    assert settings.app_name == "from-yaml"
    assert settings.redis.stream_max_len == 500
    assert settings.logging.level == "DEBUG"


def test_redis_settings_rejects_future_phase_streams() -> None:
    with pytest.raises(ValidationError):
        RedisSettings(streams=("features:updates",))


def test_redis_settings_rejects_socket_timeout_below_block_timeout() -> None:
    with pytest.raises(ValidationError):
        RedisSettings(consumer_block_ms=1_000, socket_timeout_ms=1_000)
