import os
import logging
from pathlib import Path
from typing import Any, Callable, cast, Sequence

from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    YamlConfigSettingsSource,
    SettingsConfigDict
)

from .helpers import deep_merge

logger = logging.getLogger(__name__)

PathType = Path | str | Sequence[Path | str]


class MergedYamlSettingsSource(YamlConfigSettingsSource):
    """
    사용자 yaml 설정값을 기본값과 병합하는 클래스
    """

    def __call__(self) -> dict[str, Any]:
        user_config = super().__call__()
        default_config = {}
        for field_name, field in self.settings_cls.model_fields.items():
            if field.default_factory is not None:
                factory = cast(Callable[[], Any], field.default_factory)
                default_config[field_name] = factory()
        if not user_config:
            return default_config
        return deep_merge(default_config, user_config)

    def _read_files(self, files: PathType | None) -> dict[str, Any]:
        if files is None:
            return {}
        if isinstance(files, (str, os.PathLike)):
            files = [files]
        vars: dict[str, Any] = {}
        for file in files:
            file_path = Path(file).expanduser()
            if file_path.is_file():
                vars.update(self._read_file(file_path))
                logger.warning(f"'{file_path.resolve()}' 파일을 불러왔습니다.")
                # 존재하는 첫번째 파일만 로딩
                break
        else:
            logger.error(f"설정 파일을 불러올 수 없습니다: {files}")
        return vars


class _BaseSettings(BaseSettings):
    """
    사용자의 설정값을 저장하는 클래스
    """

    model_config = SettingsConfigDict(
        yaml_file=(
            Path(__file__).with_name("settings.yaml"),
            Path.cwd() / "settings.yaml",
            Path(__file__).with_name("config.yaml"),
            Path.cwd() / "config.yaml",
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
        merged_yaml_settings = MergedYamlSettingsSource(settings_cls)
        # 설정값 적용 순서
        return (
            init_settings,
            merged_yaml_settings,
            env_settings,
            dotenv_settings,
            file_secret_settings,
        )
