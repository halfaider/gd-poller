import sys
import logging
import pathlib
import asyncio
import datetime
import traceback
from typing import Any

import dispatchers
from apis import GoogleDrive
from pollers import ActivityPoller
from helpers import check_tasks, set_logger, not_none
from config import get_config

logger = logging.getLogger(__name__)
LOCAL_TIMEZONE = datetime.datetime.now(datetime.timezone(datetime.timedelta(0))).astimezone().tzinfo


async def async_main(*args: Any, **kwds: Any) -> None:
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
            CONFIG_FILE = None
        config = get_config(CONFIG_FILE)

        # logging
        modules = set(file.stem for file in pathlib.Path(__file__).parent.glob('*.py'))
        if '__main__' not in modules:
            modules.add('__main__')
        log_settings = not_none(config.pop('logging', None), {})
        set_logger(
            level=log_settings.get('level'),
            format=log_settings.get('format'),
            date_format=log_settings.get('date_format'),
            redacted_patterns=log_settings.get('redacted_patterns'),
            redacted_substitute=log_settings.get('redacted_substitute'),
            loggers=modules,
        )

        scopes = []
        for scope in config['google_drive']['scopes']:
            if 'http' in scope:
                scopes.append(scope)
            else:
                scopes.append(f'https://www.googleapis.com/auth/{scope}')

        drive = GoogleDrive(
            config['google_drive']['token'],
            scopes,
            cache_enable=config['google_drive']['cache_enable'],
            cache_maxsize=config['google_drive']['cache_maxsize'],
            cache_ttl=config['google_drive']['cache_ttl']
        )
        for poller in config['pollers']:
            dispatcher_list = []
            for dispatcher in poller.get('dispatchers', [{'class': 'DummyDispatcher'}]):
                # yaml의 앵커는 동일한 객체를 참조
                dispatcher_ = dispatcher.copy()
                class_ = getattr(dispatchers, dispatcher_.pop('class'))
                dispatcher_['buffer_interval'] = not_none(dispatcher_.get('buffer_interval'), config['buffer_interval'])
                dispatcher_list.append(class_(**dispatcher_))

            activity_poller = ActivityPoller(
                drive,
                poller['targets'],
                dispatcher_list=dispatcher_list,
                name=poller['name'],
                polling_interval=not_none(poller.get('polling_interval'), config['polling_interval']),
                page_size=not_none(poller.get('page_size'), config['page_size']),
                actions=poller.get('actions') or config['actions'],
                task_check_interval=not_none(poller.get('task_check_interval'), config['task_check_interval']),
                patterns=poller.get('patterns') or config['patterns'],
                ignore_patterns=poller.get('ignore_patterns') or config['ignore_patterns'],
                ignore_folder=not_none(poller.get('ignore_folder'), config['ignore_folder']),
                dispatch_interval=not_none(poller.get('dispatch_interval'), config['dispatch_interval']),
                polling_delay=not_none(poller.get('polling_delay'), config['polling_delay'])
            )
            pollers.append(activity_poller)

        for poller in pollers:
            tasks.append(asyncio.create_task(poller.start(), name=poller.name))

        if config['task_check_interval'] > 0:
            tasks.append(asyncio.create_task(check_tasks(tasks, config['task_check_interval']), name='check_tasks'))
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        logger.warning(f'Tasks are cancelled...')
    except Exception:
        logger.error(traceback.format_exc())
    finally:
        logger.info('Stopping pollers....')
        running_tasks = [task for task in tasks if not task.done()]
        if running_tasks:
            for task in running_tasks:
                task.cancel()
            await asyncio.gather(*running_tasks, return_exceptions=True)
        stop_tasks = []
        for poller in pollers:
            stop_tasks.append(asyncio.create_task(poller.stop(), name=poller.name))
        if stop_tasks:
            await asyncio.gather(*stop_tasks, return_exceptions=True)


def main(*args: Any, **kwds: Any) -> None:
    try:
        asyncio.run(async_main(*args, **kwds))
    except KeyboardInterrupt:
        logger.debug('KeyboardInterrupt....')


if __name__ == '__main__':
    main(*sys.argv)
