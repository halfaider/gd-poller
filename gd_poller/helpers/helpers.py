import re
import sys
import copy
import time
import logging
import asyncio
import functools
import threading
import subprocess
from pathlib import Path
from typing import Any, Iterable, Callable, Sequence

logger = logging.getLogger(__name__)


def check_packages(packages: Iterable[Sequence[str]]) -> None:
    for pkg, pi in packages:
        try:
            __import__(pkg)
        except Exception as e:
            print(repr(e))
            subprocess.check_call((sys.executable, "-m", "pip", "install", "-U", pi))


def not_none(value: Any, default: Any) -> Any:
    return default if value is None else value


def parse_mappings(mappings: Iterable[str]) -> list[tuple[str]]:
    mapped = []
    for mapping in mappings:
        splits = re.split(":", mapping, maxsplit=2)
        if len(splits) < 2:
            logger.warning(f"Invalid mapping format: {mapping}")
            continue
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


async def await_sync(func: Callable, *args: Any, **kwds: Any) -> Any:
    return await asyncio.get_running_loop().run_in_executor(
        None, functools.partial(func, *args, **kwds)
    )


def get_last_dir(path_: str, is_dir: bool = False) -> str:
    return path_ if is_dir else str(Path(path_).parent)


def apply_cache(func: Callable, maxsize: int = 64) -> Callable:
    @functools.lru_cache(maxsize=maxsize)
    def wrapper(*args: Any, ttl_hash: int = 3600, **kwds: Any):
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
            logger.warning(f"Timeout reached: {process.args}")
    try:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=60)
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
