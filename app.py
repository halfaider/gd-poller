import sys
import traceback
import datetime
import logging
import pathlib
import asyncio

import dispatchers
from apis import GoogleDrive
from pollers import ActivityPoller
from helpers import check_packages, check_tasks, set_logger

check_packages((
    ('yaml', 'pyyaml'),
))

import yaml

logger = logging.getLogger(__name__)
LOCAL_TIMEZONE = datetime.datetime.now(datetime.timezone(datetime.timedelta(0))).astimezone().tzinfo


async def async_main(*args: tuple, **kwds: dict) -> None:
    pollers = []
    tasks = []
    try:
        # ('LOAD', '/path/to/gd_poller/app.py', '/data-dev/src/gd-poller/config.test.yaml')
        # ('app.py', '/data-dev/src/gd-poller/config.test.yaml')
        if len(args) > 2 and args[0] == 'LOAD':
            CONFIG_FILE = pathlib.Path(args[2])
        elif len(args) > 1 and args[0] != 'LOAD':
            CONFIG_FILE = pathlib.Path(args[1])
        else:
            CONFIG_FILE = pathlib.Path(__file__).with_name('config.yaml')
        with CONFIG_FILE.open(mode='r', encoding='utf-8') as file:
            try:
                config = yaml.safe_load(file)
            except yaml.YAMLError:
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

        set_logger(
            kwds.get('logger'),
            config['logging']['level'],
            config['logging']['format'],
            config['logging']['redacted_patterns'],
            config['logging']['redacted_substitute'],
            (__name__, 'dispatchers', 'apis', 'helpers', 'pollers')
        )

        # global values
        polling_interval = config.get('polling_interval', 60)
        dispatch_interval = config.get('dispatch_interval', 1)
        task_check_interval = config.get('task_check_interval', -1)
        page_size = config.get('page_size', 100)
        ignore_folder = config.get('ignore_folder', True)
        patterns = config.get('patterns')
        ignore_patterns = config.get('ignore_patterns')
        actions = config.get('actions')
        buffer_interval = config.get('buffer_interval', 30)

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
                if not (bi := dispatcher_.get('buffer_interval')) or bi < 0:
                    dispatcher_['buffer_interval'] = buffer_interval
                dispatcher_list.append(class_(**dispatcher_))
            activity_poller = ActivityPoller(
                drive,
                poller['targets'],
                dispatcher_list,
                name=poller['name'],
                polling_interval=poller.get('polling_interval') or polling_interval,
                page_size=poller.get('page_size') or page_size,
                actions=poller.get('actions') or actions,
                patterns=poller.get('patterns') or patterns,
                ignore_patterns=poller.get('ignore_patterns') or ignore_patterns,
                ignore_folder=poller.get('ignore_folder') or ignore_folder,
                dispatch_interval=poller.get('dispatch_interval') or dispatch_interval,
                task_check_interval=poller.get('task_check_interval') or task_check_interval)
            pollers.append(activity_poller)
        for poller in pollers:
            tasks.append(asyncio.create_task(poller.start(), name=poller.name))
        try:
            gathers = [*tasks]
            if task_check_interval > 0:
                gathers.append(asyncio.create_task(check_tasks(tasks, task_check_interval), name='check_tasks'))
            await asyncio.gather(*gathers)
        except asyncio.CancelledError:
            logger.warning(f'Tasks are cancelled: {__name__}')
    except:
        logger.error(traceback.format_exc())
    finally:
        logger.debug('Stopping pollers....')
        for poller in pollers:
            try:
                await poller.stop()
            except:
                logger.error(traceback.format_exc())
        for task in tasks:
            logger.debug(task)
            if not task.done():
                task.cancel()


def main(*args: tuple, **kwds: dict) -> None:
    try:
        asyncio.run(async_main(*args, **kwds))
    except KeyboardInterrupt:
        logger.debug('KeyboardInterrupt....')


if __name__ == '__main__':
    main(*sys.argv)
