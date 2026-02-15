import logging
import datetime
import functools
from urllib import parse
from typing import Any

from pydantic import BaseModel, Field, ConfigDict

from .helpers.models import _BaseSettings

logger = logging.getLogger(__name__)


class LoggingConfig(BaseModel):
    level: str = "debug"
    format: str = "%(asctime)s %(levelname)-8s %(message)s ... %(filename)s:%(lineno)d"
    date_format: str = "%Y-%m-%dT%H:%M:%S"
    redacted_patterns: tuple[str, ...] = (
        r"['\"]?(?:apikey|X-Plex-Token|token)['\"]?\s*[:=]\s*['\"]?([^'\"&\s,{}]+)['\"]?",
        r"webhooks/([^/\s]+)/([^/\s]+)",
    )
    redacted_substitute: str = "<REDACTED>"


class GoogleDriveTokenConfig(BaseModel):
    client_id: str
    client_secret: str
    refresh_token: str
    token: str | None = None


class GoogleDriveConfig(BaseModel):
    scopes: tuple[str, ...] = ("drive.readonly", "drive.activity.readonly")
    token: GoogleDriveTokenConfig = GoogleDriveTokenConfig(
        client_id="",
        client_secret="",
        refresh_token="",
        token="",
    )
    cache_enable: bool = False
    cache_ttl: int = 600
    cache_maxsize: int = 64

    def model_post_init(self, context: Any, /) -> None:
        self.scopes = tuple(
            parse.urljoin("https://www.googleapis.com/auth/", scope)
            for scope in self.scopes
        )


class GlobalConfig(BaseModel):
    polling_interval: int | None = 60
    polling_delay: int | None = 0
    dispatch_interval: int | None = 1
    task_check_interval: int | None = -1
    page_size: int | None = 100
    ignore_folder: bool | None = True
    patterns: tuple[str, ...] | None = (r".*",)
    ignore_patterns: tuple[str, ...] | None = ()
    actions: tuple[str, ...] | None = ()
    buffer_interval: int | None = 30


class DispatcherConfig(BaseModel):
    class_: str = Field(alias="class", default="DummyDispatcher")
    buffer_interval: int | None = None
    model_config = ConfigDict(extra="allow")

    def model_post_init(self, __context: Any) -> None:
        if self.__pydantic_extra__:
            for key, value in self.__pydantic_extra__.items():
                setattr(self, key, value)


class PollerConfig(GlobalConfig):
    targets: tuple[str, ...]
    name: str | None = None
    dispatchers: tuple[DispatcherConfig, ...] = (DispatcherConfig(),)
    polling_interval: int | None = None
    polling_delay: int | None = None
    dispatch_interval: int | None = None
    task_check_interval: int | None = None
    page_size: int | None = None
    ignore_folder: bool | None = None
    patterns: tuple[str, ...] | None = None
    ignore_patterns: tuple[str, ...] | None = None
    actions: tuple[str, ...] | None = None
    buffer_interval: int | None = None


class AppSettings(GlobalConfig, _BaseSettings):
    """
    앱 실행시 사용하는 설정값 클래스
    """

    google_drive: GoogleDriveConfig = GoogleDriveConfig()
    pollers: tuple[PollerConfig, ...] = ()
    logging: LoggingConfig = LoggingConfig()

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
    target: tuple[str | None, str | None, str | None] = (None, None, None)
    action: str = ""
    action_detail: str | tuple | list | None = None
    priority: float = 0.0  # timestamp()
    timestamp: datetime.datetime = datetime.datetime.now(datetime.timezone.utc)
    timestamp_text: str = ""
    ancestor: str = ""
    root: str | None = ""
    path: str | None = ""
    removed_path: str | None = ""
    link: str = ""
    is_folder: bool = False
    poller: str = ""
    parent: tuple[str | None, str | None] = (None, None)
    size: int = 0

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ActivityData):
            return NotImplemented
        return self.activity == other.activity

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, ActivityData):
            return NotImplemented
        return self.priority < other.priority
