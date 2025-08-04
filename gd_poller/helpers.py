import re
import copy
import time
import logging
import logging.config
import asyncio
import pathlib
import functools
import traceback
import threading
import subprocess
from typing import Any, Iterable, Callable, Sequence

import requests

logger = logging.getLogger(__name__)

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
}


def not_none(value: Any, default: Any) -> Any:
    return default if value is None else value


class RedactingFilter(logging.Filter):

    def __init__(
        self, patterns: Sequence = (), substitute: str = "<REDACTED>", **kwds: Any
    ) -> None:
        super().__init__(**kwds)
        self.patterns = tuple(re.compile(p, re.IGNORECASE) for p in patterns if p)
        self.substitute = substitute

    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = self.redact(record.getMessage())
        # getMessage() 결과에 이미 args가 반영되어 있음
        record.args = ()
        return True

    def redact(self, text: str) -> str:
        for pattern in self.patterns:
            if pattern.groups == 0:
                text = pattern.sub(self.substitute, text)
            else:
                text = pattern.sub(self.replace_match_groups, text)

        return text

    def replace_match_groups(self, match: re.Match) -> str:
        full_match_text = match.group(0)
        match_start_pos = match.start(0)
        group_spans = []
        for idx in range(1, match.re.groups + 1):
            if match.group(idx):
                group_spans.append(match.span(idx))
        group_spans.sort()
        result_parts = []
        last_end_in_match = 0
        for start, end in group_spans:
            start_in_match = start - match_start_pos
            end_in_match = end - match_start_pos
            result_parts.append(full_match_text[last_end_in_match:start_in_match])
            result_parts.append(self.substitute)
            last_end_in_match = end_in_match
        result_parts.append(full_match_text[last_end_in_match:])
        return "".join(result_parts)


def get_traceback_response(tb: str) -> requests.Response:
    logger.error(tb)
    response = requests.Response()
    response._content = bytes(tb, "utf-8")
    response.status_code = 0
    return response


class HelperSession(requests.Session):

    def __init__(
        self, headers: dict = None, auth: tuple = None, proxies: dict = None
    ) -> None:
        super().__init__()
        self.headers.update(DEFAULT_HEADERS)
        if headers:
            self.headers.update(headers)

    def request(self, method: str, url: str, **kwds: Any) -> requests.Response:
        return super().request(method, url, **kwds)


def request(method: str, url: str, **kwds: Any) -> requests.Response:
    return requests.request(method, url, **kwds)


async def request_async(method: str, url: str, **kwds: Any) -> requests.Response:
    try:
        return await await_sync(request, method, url, **kwds)
    except:
        return get_traceback_response(traceback.format_exc())


def parse_response(response: requests.Response) -> dict[str, Any]:
    result = {
        "status_code": response.status_code,
        "content": response.text.strip(),
        "exception": None,
        "json": None,
        "url": response.url,
    }
    try:
        result["json"] = response.json()
    except Exception as e:
        result["exception"] = repr(e)
    return result


def parse_mappings(mappings: Iterable[str]) -> list[tuple[str]]:
    mapped = []
    for mapping in mappings:
        splits = re.split(":", mapping, maxsplit=2)
        if len(splits) > 2:
            if len(splits[0]) < 2:
                source, target = ":".join(splits[:2]), splits[-1]
            else:
                source, target = splits[0], ":".join(splits[1:])
        else:
            source, target = splits
        mapped.append((source, target))
    return mapped


def map_path(target: str, mappings: Iterable[Iterable[str]]) -> str:
    for mapping in mappings:
        target = target.replace(mapping[0], mapping[1])
    return target


async def stop_event_loop() -> None:
    loop = asyncio.get_event_loop()
    loop.stop()
    loop.close()


async def await_sync(func: Callable, *args: Any, **kwds: Any) -> Any:
    return await asyncio.get_running_loop().run_in_executor(
        None, functools.partial(func, *args, **kwds)
    )


def get_last_dir(path_: str, is_dir: bool = False) -> str:
    return path_ if is_dir else str(pathlib.Path(path_).parent)


def apply_cache(func: Callable, maxsize: int = 64) -> Callable:
    @functools.lru_cache(maxsize=maxsize)
    def wrapper(*args: Any, ttl_hash: int = 3600, **kwds: Any):
        del ttl_hash
        return func(*args, **kwds)

    return wrapper


def get_ttl_hash(seconds: int = 3600) -> int:
    return round(time.time() / seconds)


async def watch_process(
    process: subprocess.Popen, stop_flag: threading.Event, timeout: int = 300
) -> None:
    for i in range(timeout):
        if process.poll() is not None or stop_flag.is_set():
            break
        await asyncio.sleep(1)
        if i >= timeout - 1:
            logger.warning(f"Timeout reached: {process.args}")
    try:
        if process.poll() is None:
            process.kill()
    except Exception as e:
        logger.exception(e)


async def check_tasks(tasks: list[asyncio.Task], interval: int = 60) -> None:
    last_time = time.time()
    while tasks:
        check = False
        if time.time() - last_time > interval:
            last_time = time.time()
            check = True
        done_tasks = []
        for task in tasks:
            name = task.get_name()
            if task.done():
                logger.debug(f'The task is done: "{name}"')
                done_tasks.append(task)
                if exception := task.exception():
                    logger.error(f"{name}: {exception}")
            else:
                if check:
                    logger.debug(f"{name}: {task.get_stack()}")
        for task in done_tasks:
            tasks.remove(task)
        await asyncio.sleep(1)


def set_logger(
    level: str = None,
    format: str = None,
    datefmt: str = None,
    redacted_patterns: Iterable = None,
    redacted_substitute: str = None,
) -> None:
    default_logging_config = {
        "version": 1,
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "default",
                "filters": ["redacted"],
            },
        },
        "formatters": {
            "default": {
                "format": format
                or "%(asctime)s,%(msecs)03d %(levelname)-8s %(message)s ... %(filename)s:%(lineno)d",
                "datefmt": datefmt or "%Y-%m-%d %H:%M:%S",
            },
        },
        "loggers": {
            __package__: {
                "level": getattr(logging, (level or "info").upper(), logging.INFO),
                "handlers": ["console"],
                "propagate": False,
            },
        },
        "filters": {
            "redacted": {
                "()": f"{RedactingFilter.__module__}.{RedactingFilter.__name__}",
                "patterns": (
                    redacted_patterns
                    if redacted_patterns is not None
                    else [
                        r"apikey=(.{10})",
                        r'["]apikey["]: ["](.{10})["]',
                        r'["]X-Plex-Token["]: ["](.{20})["]',
                        r'["]X-Plex-Token=(.{20})["]',
                        r"webhooks/(.+)/(.+):\s{",
                    ]
                ),
                "substitute": redacted_substitute or "<REDACTED>",
            },
        },
    }
    try:
        logging.config.dictConfig(default_logging_config)
    except Exception as e:
        logger.warning(f"로깅 설정 실패: {e}", exc_info=True)
        logging.basicConfig(
            level=level or logging.DEBUG,
            format=format
            or "%(asctime)s,%(msecs)03d|%(levelname)8s| %(message)s <%(filename)s:%(lineno)d#%(funcName)s>",
            datefmt=datefmt or "%Y-%m-%dT%H:%M:%S",
        )


def should_merge(base_value: Any, value: Any, merge_type: type) -> bool:
    return isinstance(base_value, merge_type) and isinstance(value, merge_type)


def deep_merge(base: dict, override: dict) -> dict:
    result = copy.deepcopy(base)
    for key, value in override.items():
        base_value = result.get(key)
        if should_merge(base_value, value, dict):
            result[key] = deep_merge(result[key], value)
        # elif should_merge(base_value, value, list):
        #    result[key] = base_value + value
        else:
            result[key] = value
    return result
