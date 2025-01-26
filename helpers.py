import traceback
import logging
import re
import asyncio
import functools
import pathlib
from dataclasses import dataclass, field
from typing import Any, Optional, Union, Iterable
from collections import OrderedDict

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


class FolderBuffer:

    def __init__(self) -> None:
        self.buffer = OrderedDict()

    def put(self, path: str, action: str = 'create', is_directory: bool = False) -> None:
        target = pathlib.Path(path)
        parent = str(target) if is_directory else str(target.parent)
        key = f'{action}|{parent}'
        if key in self.buffer:
            children: set[str] = self.buffer[key]['children']
            children.add(target.name)
        else:
            self.buffer[key] = {
                'children': set([target.name]),
            }

    def pop(self) -> tuple[str, dict]:
        if self.buffer:
            return self.buffer.popitem(last=False)

    def __len__(self) -> int:
        return len(self.buffer)

    def __getitem__(self, key: str) -> dict:
        return self.buffer.get(key)


@dataclass(order=True)
class PrioritizedItem:
    priority: float
    item: Any=field(compare=False)


def request(method: str, url: str, data: Optional[dict] = None, timeout: Union[int, tuple, None] = None, **kwds: dict) -> requests.Response:
    try:
        if method.upper() == 'JSON':
            response = requests.request('POST', url, json=data or {}, timeout=timeout, **kwds)
        else:
            response = requests.request(method, url, data=data, timeout=timeout, **kwds)
        return response
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


def parse_response(response: requests.Response) -> dict[str, Any]:
    result = {
        'status_code': response.status_code,
        'content': response.text.strip(),
        'exception': None,
        'json': None,
        'url': response.url,
    }
    try:
        result['json'] = response.json()
    except Exception as e:
        result['exception'] = repr(e)
    return result


def parse_mappings(mappings: Iterable[str]) -> list[tuple[str]]:
    mapped = []
    for mapping in mappings:
        source, _, target = mapping.partition(':')
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


async def await_sync(func: callable, *args, **kwds) -> Any:
    return await asyncio.get_running_loop().run_in_executor(None, functools.partial(func, *args, **kwds))


def get_last_dir(path_: str, is_dir: bool = False) -> str:
    return path_ if is_dir else str(pathlib.Path(path_).parent)
