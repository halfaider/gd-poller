import sys
import subprocess
import traceback
import logging
import re
import asyncio
import functools
import pathlib
import time
import threading
from dataclasses import dataclass, field
from typing import Any, Optional, Union, Iterable
from collections import OrderedDict

ARGS = ('-m', 'pip', 'install', '-U')

try:
    __import__('requests')
except:
    subprocess.check_call([sys.executable, *ARGS, 'requests'])

import requests

logger = logging.getLogger(__name__)


class RedactedFormatter(logging.Formatter):

    def __init__(self, *args, patterns: Iterable = [], substitute: str = '<REDACTED>', **kwds):
        super(RedactedFormatter, self).__init__(*args, **kwds)
        self.patterns = []
        self.substitute = substitute
        for pattern in patterns:
            self.patterns.append(re.compile(pattern, re.I))

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
                    msg = self.redact(re.compile(found, re.I), msg)
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
        splits = re.split(':', mapping, maxsplit=2)
        if len(splits) > 2:
            if len(splits[0]) < 2:
                source, target = ':'.join(splits[:2]), splits[-1]
            else:
                source, target = splits[0], ':'.join(splits[1:])
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


async def await_sync(func: callable, *args, **kwds) -> Any:
    return await asyncio.get_running_loop().run_in_executor(None, functools.partial(func, *args, **kwds))


def get_last_dir(path_: str, is_dir: bool = False) -> str:
    return path_ if is_dir else str(pathlib.Path(path_).parent)


def apply_cache(func: callable, maxsize: int = 64) -> callable:
    @functools.lru_cache(maxsize=maxsize)
    def wrapper(*args, ttl_hash: int = 3600, **kwds):
        del ttl_hash
        return func(*args, **kwds)
    return wrapper


def get_ttl_hash(seconds: int = 3600) -> int:
    return round(time.time() / seconds)


async def watch_process(process: subprocess.Popen, stop_flag: threading.Event, timeout: int = 300) -> None:
    for i in range(timeout):
        if process.poll() is not None or stop_flag.is_set():
            break
        await asyncio.sleep(1)
        if i >= timeout - 1:
            logger.warning(f'Timeout reached: {process.args}')
    try:
        if process.poll() is None:
            process.kill()
    except:
        logger.error(traceback.format_exc())
