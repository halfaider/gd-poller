import sys
import traceback
import time
import datetime
import logging
import pathlib

import yaml

from gd_api import GoogleDrive
from pollers import ActivityPoller
from dispatchers import DummyDispatcher, DiscordDispatcher, PlexmateDispatcher, GDSToolDispatcher, RcloneDispatcher, KavitaDispatcher
from helpers import RedactedFormatter


logger = logging.getLogger()
LOCAL_TIMEZONE = datetime.datetime.now(datetime.timezone(datetime.timedelta(0))).astimezone().tzinfo
REDACTED_PATTERNS = (
    'apikey=(.{10})',
    "'apikey': '(.{10})'",
    "'X-Plex-Token': '(.{20})'",
    "'X-Plex-Token=(.{20})'",
    "webhooks/(.+)/(.+):\\s{",
)

def set_logger() -> None:
    global logger
    logger.setLevel(logging.DEBUG)
    if not logger.handlers:
        stream_handler = logging.StreamHandler()
        redacted_patterns = REDACTED_PATTERNS
        fomatter = RedactedFormatter(patterns=redacted_patterns, substitute='<REDACTED>', fmt='%(asctime)s %(levelname).3s %(message)s <%(module)s:%(lineno)d>')
        stream_handler.setFormatter(fomatter)
        logger.addHandler(stream_handler)


def main(*args: tuple, **kwds: dict):
    global logger
    pollers = []
    dispatcher_classes = {
        'DiscordDispatcher': DiscordDispatcher,
        'DummyDispatcher': DummyDispatcher,
        'PlexmateDispatcher': PlexmateDispatcher,
        'GDSToolDispatcher': GDSToolDispatcher,
        'KavitaDispatcher': KavitaDispatcher,
        'RcloneDispatcher': RcloneDispatcher,
    }
    try:
        # ('LOAD', '/path/to/gd_poller/app.py')
        # (['app.py', '/path/to/...'],)
        if args[0] == 'LOAD':
            # Flasfkarm의 로거를 사용
            logger = kwds.get('logger')
        set_logger()
        if args[0] != 'LOAD' and len(args[0]) > 1:
            CONFIG_FILE = pathlib.Path(args[0][1])
        else:
            CONFIG_FILE = pathlib.Path(__file__).with_name('config.yaml')

        with CONFIG_FILE.open(mode='r', encoding='utf-8') as file:
            config = yaml.safe_load(file.read())

        drive = GoogleDrive(config['google_drive']['token'], config['google_drive']['scopes'], {})

        for poller in config['pollers']:
            dispatchers = []
            for dispatcher in poller.get('dispatchers', {'class': 'DummyDispatcher'}):
                class_ = dispatcher_classes[dispatcher.pop('class')]
                dispatchers.append(class_(**dispatcher))
            activity_poller = ActivityPoller(
                drive,
                dispatchers,
                poller['targets'],
                name=poller['name'],
                polling_interval=poller.get('polling_interval', 60),
                page_size=poller.get('page_size', 100),
                actions=poller.get('actions'),
                patterns=poller.get('patterns'),
                ignore_patterns=poller.get('ignore_patterns'),
                ignore_folder=poller.get('ignore_folder', True),
                dispatch_interval=poller.get('dispatch_interval', 1))
            pollers.append(activity_poller)

        for poller in pollers:
            poller.start()

        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.debug('KeyboardInterrupt....')
    except:
        print(traceback.format_exc())
        logger.error(traceback.format_exc())
    finally:
        logger.debug('Stopping pollers....')
        for poller in pollers:
            poller.stop()
        for poller in pollers:
            poller.join()


if __name__ == '__main__':
    main(sys.argv)
