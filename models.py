import logging
from pathlib import Path
from typing import Any, Sequence

from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    YamlConfigSettingsSource,
    SettingsConfigDict,
)

logger = logging.getLogger(__name__)

PathType = Path | str | Sequence[Path | str]


class _BaseSettings(BaseSettings):
    """
    사용자의 설정값을 저장하는 클래스
    """

    model_config = SettingsConfigDict(
        yaml_file=(
            Path(__file__).with_name("config.yaml"),
            Path.cwd() / "config.yaml",
            Path(__file__).with_name("settings.yaml"),
            Path.cwd() / "settings.yaml",
        ),
        yaml_file_encoding="utf-8",
        extra="ignore",
    )

    def __init__(
        self, *args: Any, user_yaml_file: str | None = None, **kwds: Any
    ) -> None:
        if user_yaml_file:
            self.model_config["yaml_file"] = user_yaml_file
        super().__init__(*args, **kwds)

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        # 설정값 적용 순서
        return (
            init_settings,
            YamlConfigSettingsSource(settings_cls),
            env_settings,
            dotenv_settings,
            file_secret_settings,
        )
