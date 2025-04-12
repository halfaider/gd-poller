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


def check_packages(packages: list) -> None:
    for pkg in packages:
        try:
            __import__(pkg[0])
        except:
            subprocess.check_call([sys.executable, '-m', 'pip', 'install', '-U', pkg[1]])


check_packages([('requests', 'requests')])

import requests

logger = logging.getLogger(__name__)

DEFAULT_HEADERS = {
    "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
}


@dataclass(order=True)
class PrioritizedItem:
    priority: float
    item: Any=field(compare=False)


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
    '''
    'parent_path': {
        'action1': {('file' | 'folder', 'name1'), ('file' | 'folder', 'name2')},
        'action2': {('file' | 'folder', 'name3'), ('file' | 'folder', 'name4')}
    }
    '''

    def __init__(self) -> None:
        self.buffer = OrderedDict()

    def put(self, path: str, action: str = 'create', is_directory: bool = False) -> None:
        target = pathlib.Path(path)
        key = str(target.parent)
        if key in self.buffer:
            self.buffer[key].setdefault(action, set())
            self.buffer[key][action].add(('folder' if is_directory else 'file', target.name))
        else:
            self.buffer[key] = {action: set([('folder' if is_directory else 'file', target.name)])}

    def pop(self) -> tuple[str, dict[str, set[tuple[str, str]]]]:
        if self.buffer:
            return self.buffer.popitem(last=False)

    def __len__(self) -> int:
        return len(self.buffer)

    def __getitem__(self, key: str) -> dict:
        return self.buffer.get(key)


def request_json(func: callable) -> callable:
    @functools.wraps(func)
    def wrapper(*args, data: Optional[dict] = None, timeout: Union[int, tuple] = None, **kwds: dict) -> requests.Response:
        method = args[0] if type(args[0]) is str else args[1]
        try:
            if method.upper() == 'JSON':
                args = ('POST', *args[1:]) if type(args[0]) is str else (args[0], 'POST', *args[2:])
                response = func(*args, json=data, timeout=timeout, **kwds)
            else:
                response = func(*args, data=data, timeout=timeout, **kwds)
        except:
            response = get_traceback_response(traceback.format_exc())
        return response
    return wrapper


def get_traceback_response(tb: str) -> requests.Response:
    logger.error(tb)
    response = requests.Response()
    response._content = bytes(tb, 'utf-8')
    response.status_code = 0
    return response


class HelperSession(requests.Session):

    def __init__(self, headers: dict = None, auth: tuple = None, proxies: dict = None) -> None:
        super(HelperSession, self).__init__()
        self.headers.update(DEFAULT_HEADERS)
        if headers:
            self.headers.update(headers)

    @request_json
    def request(self, method: str, url: str, **kwds: dict) -> requests.Response:
        '''override'''
        return super(HelperSession, self).request(method, url, **kwds)


@request_json
def request(method: str, url: str, **kwds: dict) -> requests.Response:
    return requests.request(method, url, **kwds)


async def request_async(method: str, url: str, data: Optional[dict] = None, timeout: Union[int, tuple, None] = None, **kwds: dict) -> requests.Response:
    try:
        if method.upper() == 'JSON':
            return await await_sync(requests.request, 'POST', url, json=data or {}, timeout=timeout, **kwds)
        else:
            return await await_sync(requests.request, method, url, data=data, timeout=timeout, **kwds)
    except:
        return get_traceback_response(traceback.format_exc())


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
                    logger.error(f'{name}: {exception}')
            else:
                if check:
                    logger.debug(f'{name}: {task.get_stack()}')
        for task in done_tasks:
            tasks.remove(task)
        await asyncio.sleep(1)
