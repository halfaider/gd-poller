import sys
import traceback
import datetime
import logging
import pathlib
import asyncio
import time

import dispatchers
from apis import GoogleDrive
from pollers import ActivityPoller
from helpers import RedactedFormatter, check_packages, check_tasks

check_packages([
    ('yaml', 'pyyaml'),
])

import yaml

logger = logging.getLogger(__name__)
LOCAL_TIMEZONE = datetime.datetime.now(datetime.timezone(datetime.timedelta(0))).astimezone().tzinfo


def set_logger(logger_: logging.Logger, level: str = 'DEBUG', format: str = None, redacted_patterns: list = None, redacted_substitute: str = '<REDACTED>') -> None:
    if logger_:
        level = logger_.level
        handlers = logger_.handlers
    else:
        level = getattr(logging, level.upper())
        format = format or '%(asctime)s|%(levelname).3s|%(message)s <%(filename)s:%(lineno)d#%(funcName)s>'
        redacted_patterns = redacted_patterns or ('apikey=(.{10})', "'apikey': '(.{10})'", "'X-Plex-Token': '(.{20})'", "'X-Plex-Token=(.{20})'", "webhooks/(.+)/(.+):\\s{")
        fomatter = RedactedFormatter(patterns=redacted_patterns, substitute=redacted_substitute, fmt=format)
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(fomatter)
        handlers = [
            stream_handler
        ]
    for logger_name in [__name__, 'dispatchers', 'apis', 'helpers', 'pollers']:
        logger_ = logging.getLogger(logger_name)
        logger_.setLevel(level)
        for handler in handlers:
            logger_.addHandler(handler)


async def async_main(*args: tuple, **kwds: dict) -> None:
    pollers = []
    tasks = []
    try:
        # ('LOAD', '/path/to/gd_poller/app.py', '/data-dev/src/gd-poller/config.test.yaml')
        # ('app.py', '/data-dev/src/gd-poller/config.test.yaml')
        if args[0] == 'LOAD' and len(args) > 2:
            CONFIG_FILE = pathlib.Path(args[2])
        elif args[0] != 'LOAD' and len(args) > 1:
            CONFIG_FILE = pathlib.Path(args[1])
        else:
            CONFIG_FILE = pathlib.Path(__file__).with_name('config.yaml')
        with CONFIG_FILE.open(mode='r', encoding='utf-8') as file:
            try:
                config = yaml.safe_load(file.read())
            except:
                logger.error(traceback.format_exc())
                logger.error(f'설정 파일을 불러올 수 없습니다. YAML 문법에 맞게 작성되었는지 확인해 보세요: {CONFIG_FILE.absolute()}')
                return

        if not config.get('logging'):
            config['logging'] = {
                'level': 'DEBUG',
                'format': '%(asctime)s|%(levelname).3s| %(message)s <%(filename)s:%(lineno)d#%(funcName)s>',
                'redacted_patterns': [
                    "apikey=(.{10,36})",
                    "'apikey': '(.{10,36})'",
                    "'X-Plex-Token': '(.{20})'",
                    "'X-Plex-Token=(.{20})'",
                    "webhooks/(.+)/(.+):\\s{",
                ],
                'redacted_substitute': '<REDACTED>',
            }

        set_logger(kwds.get('logger'), config['logging']['level'], config['logging']['format'], config['logging']['redacted_patterns'], config['logging']['redacted_substitute'])

        scopes = []
        for scope in config['google_drive']['scopes']:
            if 'http' in scope:
                scopes.append(scope)
            else:
                scopes.append(f'https://www.googleapis.com/auth/{scope}')

        drive = GoogleDrive(
            config['google_drive']['token'],
            scopes,
            cache_enable=config['google_drive'].get('cache_enable', False),
            cache_maxsize=config['google_drive'].get('cache_maxsize', 64),
            cache_ttl=config['google_drive'].get('cache_ttl', 600)
        )
        for poller in config['pollers']:
            dispatcher_list = []
            for dispatcher in poller.get('dispatchers', [{'class': 'DummyDispatcher'}]):
                # yaml의 앵커는 동일한 객체를 참조
                dispatcher_ = dispatcher.copy()
                class_ = getattr(dispatchers, dispatcher_.pop('class'))
                dispatcher_list.append(class_(**dispatcher_))
            activity_poller = ActivityPoller(
                drive,
                poller['targets'],
                dispatcher_list,
                name=poller['name'],
                polling_interval=poller.get('polling_interval', 60),
                page_size=poller.get('page_size', 50),
                actions=poller.get('actions'),
                patterns=poller.get('patterns'),
                ignore_patterns=poller.get('ignore_patterns'),
                ignore_folder=poller.get('ignore_folder', True),
                dispatch_interval=poller.get('dispatch_interval', 1))
            pollers.append(activity_poller)
        for poller in pollers:
            tasks.append(asyncio.create_task(poller.start(), name=poller.name))
        check_task = asyncio.create_task(check_tasks(tasks), name='check_tasks')
        try:
            await asyncio.gather(check_task, *tasks)
        except asyncio.CancelledError:
            logger.warning(f'Tasks are cancelled: {__name__}')
    except:
        print(traceback.format_exc())
        logger.error(traceback.format_exc())
    finally:
        logger.debug('Stopping pollers....')
        for poller in pollers:
            await poller.stop()
        for task in tasks:
            logger.debug(task)
            if not task.done():
                #task.print_stack()
                task.cancel()


def main(*args: tuple, **kwds: dict) -> None:
    #try:
    #    loop = asyncio.get_running_loop()
    #    if loop.is_running():
    #        print(f'Stopping running event loop...')
    #        asyncio.run_coroutine_threadsafe(stop_event_loop(), loop)
    #except Exception as e:
    #    print(e)
    try:
        asyncio.run(async_main(*args, **kwds))
    except KeyboardInterrupt:
        logger.debug('KeyboardInterrupt....')


if __name__ == '__main__':
    main(*sys.argv)
