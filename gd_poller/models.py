import logging
import datetime
import functools
from urllib import parse
from typing import Any

from pydantic import BaseModel, Field, ConfigDict

from .helpers.models import _BaseSettings

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


class AppSettings(GlobalConfig, _BaseSettings):
    """
    앱 실행시 사용하는 설정값 클래스
    """

    google_drive: GoogleDriveConfig = Field(
        default_factory=get_default_google_drive_settings
    )
    pollers: tuple[PollerConfig, ...] = ()
    logging: LoggingConfig = Field(default_factory=get_default_logging_settings)

    def model_post_init(self, context: Any, /) -> None:
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
    activity: dict = {}
    # title, name, tymimeType
    target: tuple[str, str | None, str | None] = ()
    action: str = ""
    action_detail: str | tuple | list | None = None
    priority: float = 0.0  # timestamp()
    timestamp: datetime.datetime = datetime.datetime.now(datetime.timezone.utc)
    timestamp_text: str = ""
    ancestor: str = ""
    root: str  = ""
    path: str  = ""
    removed_path: str = ""
    link: str  = ""
    is_folder: bool = False
    poller: str = ""

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ActivityData):
            return NotImplemented
        return self.activity == other.activity

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, ActivityData):
            return NotImplemented
        return self.priority < other.priority
