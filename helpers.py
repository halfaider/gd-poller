import traceback
import logging
import re
import asyncio
import functools
import pathlib
from dataclasses import dataclass, field
from typing import Any, Optional, Union, Iterable
from collections import deque

import requests

logger = logging.getLogger(__name__)


class RedactedFormatter(logging.Formatter):

    def __init__(self, *args, patterns: Iterable = [], substitute: str = '<REDACTED>', **kwds):
        super(RedactedFormatter, self).__init__(*args, **kwds)
        self.patterns = []
        self.substitute = substitute
        for pattern in patterns:
            self.patterns.append(re.compile(pattern))

    def format(self, record):
        msg = super().format(record)
        for pattern in self.patterns:
            match = pattern.search(msg)
            if match:
                if len(match.groups()) > 0:
                    groups = list(match.groups())
                else:
                    groups = [match.group(0)]
                for found in groups:
                    msg = self.redact(re.compile(found), msg)
        return msg

    def redact(self, pattern: re.Pattern, text: str) -> str:
        return pattern.sub(self.substitute, text)


@dataclass(order=True)
class PrioritizedItem:
    priority: float
    item: Any=field(compare=False)


@dataclass(init=True)
class PathItem:
    key: str
    path: str
    is_directory: bool = False
    is_removed: bool = False


class PathQueue:

    def __init__(self):
        self._set = set()
        self._queue = deque()

    @property
    def set(self) -> set:
        return self._set

    @property
    def queue(self) -> deque:
        return self._queue

    def put(self, item: PathItem) -> None:
        if item.key not in self.set:
            self.set.add(item.key)
            self.queue.appendleft(item)

    def get(self) -> PathItem:
        item: PathItem = self.queue.pop()
        self.set.remove(item.key)
        return item

    def is_empty(self) -> bool:
        return len(self) < 1

    def __len__(self):
        return len(self.queue)


def request(method: str, url: str, data: Optional[dict] = None, timeout: Union[int, tuple, None] = None, **kwds: dict) -> requests.Response:
    try:
        if method.upper() == 'JSON':
            return requests.request('POST', url, json=data or {}, timeout=timeout, **kwds)
        else:
            return requests.request(method, url, data=data, timeout=timeout, **kwds)
    except:
        tb = traceback.format_exc()
        logger.error(tb)
        response = requests.Response()
        response._content = bytes(tb, 'utf-8')
        response.status_code = 0
        return response


async def request_async(method: str, url: str, data: Optional[dict] = None, timeout: Union[int, tuple, None] = None, **kwds: dict) -> requests.Response:
    try:
        if method.upper() == 'JSON':
            return await await_sync(requests.request, 'POST', url, json=data or {}, timeout=timeout, **kwds)
        else:
            return await await_sync(requests.request, method, url, data=data, timeout=timeout, **kwds)
    except:
        tb = traceback.format_exc()
        logger.error(tb)
        response = requests.Response()
        response._content = bytes(tb, 'utf-8')
        response.status_code = 0
        return response


def parse_json_response(response: requests.Response) -> dict[str, Any]:
    try:
        result = response.json()
    except Exception as e:
        result = {
            'status_code': response.status_code,
            'content': response.text.strip(),
            'exception': f'{repr(e)}',
        }
    return result


def parse_mappings(mappings: Iterable[str]) -> list[tuple[str]]:
    return [tuple(mapping.split(':')) for mapping in mappings]


def map_path(target: str, mappings: Iterable[Iterable[str]]) -> str:
    for mapping in mappings:
        target = target.replace(mapping[0], mapping[1])
    return target


async def stop_event_loop() -> None:
    loop = asyncio.get_event_loop()
    loop.stop()
    loop.close()


async def await_sync(func: callable, *args, **kwds) -> Any:
    return await asyncio.get_running_loop().run_in_executor(None, functools.partial(func, *args, **kwds))


def get_last_dir(path_: str, is_dir: bool = False) -> str:
    return path_ if is_dir else pathlib.Path(path_).parent.as_posix()
