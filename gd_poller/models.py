import os
import logging
import datetime
import functools
from urllib import parse
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field, ConfigDict
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    YamlConfigSettingsSource,
)

from .helpers import deep_merge

logger = logging.getLogger(__name__)


def get_default_logging_settings() -> dict:
    return {
        "level": "debug",
        "format": "%(asctime)s,%(msecs)03d|%(levelname)-8s %(message)s ... %(filename)s:%(lineno)d",
        "date_format": "%Y-%m-%dT%H:%M:%S",
        "redacted_patterns": (
            "apikey=(.{10,36})",
            "['\"]apikey['\"]: ['\"](.{10,36})['\"]",
            "['\"]X-Plex-Token['\"]: ['\"](.{20})['\"]",
            "['\"]X-Plex-Token=(.{20})['\"]",
            "webhooks/(.+)/(.+):\\s{",
        ),
        "redacted_substitute": "<REDACTED>",
    }


def get_default_google_drive_settings() -> dict:
    return {
        "scopes": ("drive.readonly", "drive.activity.readonly"),
        "token": {
            "token": "",
        },
        "cache_enable": False,
        "cache_ttl": 600,
        "cache_maxsize": 64,
    }


class LoggingConfig(BaseModel):
    level: str
    format: str
    date_format: str
    redacted_patterns: tuple[str, ...]
    redacted_substitute: str


class GoogleDriveTokenConfig(BaseModel):
    client_id: str
    client_secret: str
    refresh_token: str
    token: str = None


class GoogleDriveConfig(BaseModel):
    scopes: tuple[str, ...]
    token: GoogleDriveTokenConfig
    cache_enable: bool
    cache_ttl: int
    cache_maxsize: int

    def model_post_init(self, context: Any, /) -> None:
        """override"""
        self.scopes = tuple(
            parse.urljoin("https://www.googleapis.com/auth/", scope)
            for scope in self.scopes
        )


class GlobalConfig(BaseModel):
    polling_interval: int = 60
    polling_delay: int = 0
    dispatch_interval: int = 1
    task_check_interval: int = -1
    page_size: int = 100
    ignore_folder: bool = True
    patterns: tuple[str, ...] = (r".*",)
    ignore_patterns: tuple[str, ...] = ()
    actions: tuple[str, ...] = ()
    buffer_interval: int = 30


class DispatcherConfig(BaseModel):
    class_: str = Field(alias="class", default="DummyDispatcher")
    buffer_interval: int = None
    model_config = ConfigDict(extra="allow")

    def model_post_init(self, __context: Any) -> None:
        if self.__pydantic_extra__:
            for key, value in self.__pydantic_extra__.items():
                setattr(self, key, value)


class PollerConfig(GlobalConfig):
    targets: tuple[str, ...]
    name: str = None
    dispatchers: tuple[DispatcherConfig, ...] = (DispatcherConfig(),)
    polling_interval: int = None
    polling_delay: int = None
    dispatch_interval: int = None
    task_check_interval: int = None
    page_size: int = None
    ignore_folder: bool = None
    patterns: tuple[str, ...] = None
    ignore_patterns: tuple[str, ...] = None
    actions: tuple[str, ...] = None
    buffer_interval: int = None


class MergedYamlSettingsSource(YamlConfigSettingsSource):
    """
    사용자 yaml 설정값을 기본값과 병합하는 클래스
    """

    def __call__(self) -> dict[str, Any]:
        """override"""
        user_config = super().__call__()
        default_config = {}
        for field_name, field in self.settings_cls.model_fields.items():
            if field.default_factory:
                default_config[field_name] = field.default_factory()
        if not user_config:
            return default_config
        return deep_merge(default_config, user_config)

    def _read_files(self, files: str | os.PathLike | None) -> dict[str, Any]:
        """override"""
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

    model_config = None

    def __init__(
        self, *args: Any, user_yaml_file: str | os.PathLike | None = None, **kwds: Any
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
        """override"""
        merged_yaml_settings = MergedYamlSettingsSource(settings_cls)
        # 설정값 적용 순서
        return (
            init_settings,
            merged_yaml_settings,
            env_settings,
            dotenv_settings,
            file_secret_settings,
        )


class AppSettings(GlobalConfig, _BaseSettings):
    """
    앱 실행시 사용하는 설정값 클래스
    """

    google_drive: GoogleDriveConfig = Field(
        default_factory=get_default_google_drive_settings
    )
    pollers: tuple[PollerConfig, ...] = ()
    logging: LoggingConfig = Field(default_factory=get_default_logging_settings)
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

    def model_post_init(self, context: Any, /) -> None:
        """override"""
        super().model_post_init(context)
        self.pollers = tuple(self.pollers)
        global_filed_names = GlobalConfig.model_fields.keys()
        for idx, poller in enumerate(self.pollers):
            if poller.name is None:
                poller.name = f"poller-{idx}"
            for field_name in global_filed_names:
                local_value = getattr(poller, field_name)
                if local_value is None:
                    setattr(poller, field_name, getattr(self, field_name))
            for dispatcher in poller.dispatchers:
                if dispatcher.buffer_interval is None:
                    dispatcher.buffer_interval = poller.buffer_interval
        # logger.warning(self.model_dump_json(indent=2))


@functools.total_ordering
class ActivityData(BaseModel):
    activity: dict
    timestamp: datetime.datetime
    timestamp_text: str = None
    priority: float = 0.0  # timestamp()
    # title, name, tymimeType
    target: tuple[str, str, str]
    action: str
    action_detail: str | tuple | list | None = None
    ancestor: str = None
    root: str = None
    path: str | None = None
    removed_path: str | None = None
    link: str = None
    is_folder: bool = False
    poller: str = None

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ActivityData):
            return NotImplemented
        return self.activity == other.activity

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, ActivityData):
            return NotImplemented
        return self.priority < other.priority
