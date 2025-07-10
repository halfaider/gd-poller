import os
import pathlib
import logging

from helpers import check_packages, set_logger

check_packages((('yaml', 'pyyaml'),))

import yaml

logger = logging.getLogger(__name__)


def get_default_config() -> dict:
    return {
        'logging': {
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
        },
        'polling_interval': 60,
        'polling_delay': 60,
        'dispatch_interval': 1,
        'task_check_interval': -1,
        'page_size': 100,
        'ignore_folder': True,
        'patterns': [r'.*'],
        'ignore_patterns': [],
        'actions': [],
        'buffer_interval': 30,
        'google_drive': {
            'token': {
                'client_id': '',
                'client_secret': '',
                'refresh_token': '',
                'token': '',
            },
            'scopes': [
                'drive.readonly',
                'drive.activity.readonly',
            ],
            'cache_enable': False,
            'cache_maxsize': 64,
            'cache_ttl': 600,
        },
        'pollers': [],
    }


def update_config(original: dict, update: dict) -> dict:
    for key, value in update.items():
        if isinstance(value, dict):
            original[key] = update_config(original.get(key, {}), value)
        else:
            original[key] = value
    return original


def get_config(config_yaml: pathlib.Path = None) -> dict:
    yaml_config = None
    config_files = [pathlib.Path(__file__).with_name('config.yaml'), pathlib.Path(os.getcwd(), 'config.yaml')]
    if config_yaml:
        config_files.insert(0, config_yaml)
    for yaml_file in config_files:
        try:
            with open(yaml_file, 'r', encoding='utf-8') as file_stream:
                yaml_config = yaml.safe_load(file_stream)
                print(f'{yaml_file.resolve()} 파일을 불러왔습니다.')
                break
        except Exception as e:
            print(repr(e))
    else:
        raise Exception('config.yaml 파일을 불러오지 못 했습니다.')

    if not yaml_config:
        raise Exception('설정 값을 가져올 수 없습니다.')

    config = get_default_config()
    config = update_config(config, yaml_config)
    return config
