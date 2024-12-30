import sys
import traceback
import datetime
import logging
import pathlib
import subprocess
import asyncio

try:
    __import__('requests')
except:
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', '-U', 'requests'])

try:
    __import__('yaml')
except:
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', '--upgrade', 'pyyaml'])

try:
    __import__('googleapiclient')
except:
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', '--upgrade', 'google-api-python-client'])

try:
    __import__('google.oauth2')
except:
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', '--upgrade', 'google-auth'])

try:
    __import__('httplib2')
except:
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', '--upgrade', 'httplib2'])

import yaml

import dispatchers
from apis import GoogleDrive
from pollers import ActivityPoller
from helpers import RedactedFormatter, stop_event_loop

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
            config = yaml.safe_load(file.read())

        set_logger(kwds.get('logger'), config['logging']['level'], config['logging']['format'], config['logging']['redacted_patterns'], config['logging']['redacted_substitute'])

        drive = GoogleDrive(config['google_drive']['token'], config['google_drive']['scopes'])
        for poller in config['pollers']:
            dispatcher_list = []
            for dispatcher in poller.get('dispatchers', [{'class': 'DummyDispatcher'}]):
                class_ = getattr(dispatchers, dispatcher.pop('class'))
                dispatcher_list.append(class_(**dispatcher))
            activity_poller = ActivityPoller(
                drive,
                poller['targets'],
                dispatcher_list,
                name=poller['name'],
                polling_interval=poller.get('polling_interval'),
                page_size=poller.get('page_size'),
                actions=poller.get('actions'),
                patterns=poller.get('patterns'),
                ignore_patterns=poller.get('ignore_patterns'),
                ignore_folder=poller.get('ignore_folder'),
                dispatch_interval=poller.get('dispatch_interval'))
            pollers.append(activity_poller)
        for poller in pollers:
            tasks.append(asyncio.create_task(poller.start(), name=poller.name))
        try:
            await asyncio.gather(*tasks)
            while True:
                await asyncio.sleep(1)
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
