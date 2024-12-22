import logging
import functools
import urllib.parse
import traceback
import pathlib
import inspect
import threading
import asyncio
from typing import Any, Optional

import requests

from helpers import (
    PathItem, PathQueue,
    parse_mappings, request, map_path, parse_json_response, get_last_dir
)

logger = logging.getLogger(__name__)


class Dispatcher:

    def __init__(self, mappings: list = None) -> None:
        self.stop_event = threading.Event()
        self.mappings = parse_mappings(mappings) if mappings else None

    async def start(self) -> None:
        if self.stop_event.is_set():
            self.stop_event.clear()
        await self.on_start()

    async def on_start(self) -> None:
        pass

    async def stop(self) -> None:
        if not self.stop_event.is_set():
            self.stop_event.set()
        await self.on_stop()

    async def on_stop(self) -> None:
        pass

    def dispatch(self, data: dict) -> None:
        raise Exception('이 메소드를 구현하세요.')


class DummyDispatcher(Dispatcher):

    def dispatch(self, data: dict) -> None:
        '''override'''
        logger.info(f'DummyDispatcher: {data}')


class KavitaDispatcher(Dispatcher):

    def __init__(self, url: str, apikey: str, *args: tuple, **kwds: dict) -> None:
        super(KavitaDispatcher, self).__init__(*args, **kwds)
        self.url = url.strip().strip('/')
        self.apikey = apikey.strip()
        self.token = None
        self.refresh_token = None

    @property
    def headers(self) -> dict:
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json, */*'
        }
        if self.token:
            headers['Authorization'] = f'Bearer {self.token}'
        return headers

    def api(method: str = 'POST') -> callable:
        def decorator(func) -> callable:
            @functools.wraps(func)
            def wrapper(self, *args: tuple, **kwds: dict) -> requests.Response:
                data: dict = func(self, *args, **kwds)
                end_point = data.pop('endPoint', '/version')
                by_parameter = data.pop('by_parameter', False)
                data['apiKey'] = self.apikey
                logger.debug(f'{end_point}')
                if by_parameter or method == 'GET':
                    query = urllib.parse.urlencode(data)
                    return request(method, f'{self.url}{end_point}?{query}', headers=self.headers)
                else:
                    return request(method, f'{self.url}{end_point}', json=data, headers=self.headers)
            return wrapper
        return decorator

    @api('POST')
    def plugin_authenticate(self) -> requests.Response:
        return {
            'endPoint': '/api/Plugin/authenticate',
            'pluginName': 'GDPollers',
            'by_parameter': True,
        }

    @api('POST')
    def library_scan_folder(self, folder: str) -> requests.Response:
        return {
            'endPoint': '/api/Library/scan-folder',
            'folderPath': folder,
        }

    def dispatch(self, data: dict) -> None:
        '''override'''
        if not self.token:
            self.set_token()
        kavita_path = map_path(data['path'], self.mappings) if self.mappings else data['path']
        if not data.get('is_folder'):
            kavita_path = pathlib.Path(kavita_path).parent.as_posix()
        logger.debug(f'Kavita scan folder: {kavita_path}')
        response = self.library_scan_folder(kavita_path)
        if response.status_code != 200:
            logger.debug(response.text)

    def set_token(self) -> None:
        response = self.plugin_authenticate()
        if response.status_code == 200:
            json_ = response.json()
            self.token = json_.get('token')
            self.refresh_token = json_.get('refreshToken')
        else:
            logger.error(f'Could not retrieve the token: {response.text}')


class FlaskfarmDispatcher(Dispatcher):

    PACKAGE = 'system'

    def __init__(self, url: str, apikey: str, *args: tuple, **kwds: dict) -> None:
        super(FlaskfarmDispatcher, self).__init__(*args, **kwds)
        self.url = url.strip().strip('/')
        self.apikey = apikey.strip()

    def api(method: str = 'POST') -> callable:
        def decorator(func) -> callable:
            @functools.wraps(func)
            def wrapper(self, *args: tuple, **kwds: dict) -> dict[str, Any]:
                data: dict = func(self, *args, **kwds)
                data['apikey'] = self.apikey
                command = f'{self.PACKAGE}/api/' + '/'.join(func.__name__.split('__'))
                logger.debug(f'{command}: {data}')
                match method:
                    case 'POST':
                        return request('POST', f'{self.url}/{command}', data=data)
                    case 'GET':
                        query = urllib.parse.urlencode(data)
                        return request('GET', f'{self.url}/{command}?{query}')
            return wrapper
        return decorator


class GDSToolDispatcher(FlaskfarmDispatcher):

    PACKAGE = 'gds_tool'

    def dispatch(self, data: dict) -> None:
        '''override'''
        match (data.get('action'), data.get('is_folder')):
            case 'create' | 'move', _:
                self.fp__broadcast(data['path'], 'ADD')
            case 'delete', True:
                self.fp__broadcast(data['path'], 'REMOVE_FOLDER')
            case 'delete', False:
                self.fp__broadcast(data['path'], 'REMOVE_FILE')
            case 'edit', _:
                self.fp__broadcast(data['path'], 'REFRESH')

    @FlaskfarmDispatcher.api('GET')
    def fp__broadcast(self, path: str, mode: str) -> requests.Response:
        gds_path = map_path(path, self.mappings) if self.mappings else path
        if not gds_path.startswith('/ROOT/GDRIVE'):
            raise Exception(f'gds_path must start with "/ROOT/GDRIVE/": {gds_path}')
        else:
            return {
                'gds_path': gds_path,
                'scan_mode': mode
            }


class PlexmateDispatcher(FlaskfarmDispatcher):

    PACKAGE = 'plex_mate'

    def dispatch(self, data: dict) -> None:
        '''override'''
        remote_path = map_path(data['path'], self.mappings) if self.mappings else data['path']
        if data['action'] == 'delete':
            mode = 'REMOVE_FOLDER' if data['is_folder'] else 'REMOVE_FILE'
        else:
            mode = 'ADD'
        logger.info(f'Plexmate: {self.scan__do_scan(remote_path, mode=mode)}')
        if data.get('removed_path'):
            mode = 'REMOVE_FOLDER' if data['is_folder'] else 'REMOVE_FILE'
            logger.info(f"Plexmate: {self.scan__do_scan(data.get('removed_path'), mode=mode)}")

    @FlaskfarmDispatcher.api('POST')
    def scan__do_scan(self, dir: str, mode: str = 'ADD') -> requests.Response:
        return {
            'target': dir,
            'mode': mode
        }


class DiscordDispatcher(Dispatcher):

    API_URL = 'https://discord.com/api'
    COLORS = {
        'default': '0',
        'move': '3447003',
        'create': '5763719',
        'delete': '15548997',
        'edit': '16776960',
    }

    def __init__(self, webhook_id: str, webhook_token: str, *args: tuple, **kwds: dict) -> None:
        super(DiscordDispatcher, self).__init__(*args, **kwds)
        self._webhook_id = webhook_id
        self._webhook_token = webhook_token

    @property
    def webhook_id(self) -> str:
        return self._webhook_id

    @property
    def webhook_token(self) -> str:
        return self._webhook_token

    @property
    def headers(self) -> dict:
        return {
            'Content-Type': 'application/json',
            'Accept': 'application/json, */*'
        }

    def api(func) -> callable:
        @functools.wraps(func)
        def wrapper(self, *args, **kwds) -> dict:
            params = func(self, *args, ** kwds)
            api = params.pop('api')
            method = params.pop('method')
            return request(method, f'{self.API_URL}{api}', json=params, headers=self.headers)
        return wrapper

    @api
    def webhook(self, username: str = 'Activity Poller', content: str = None, embeds: list[dict] = None) -> requests.Response:
        params = {
            'api': f'/webhooks/{self.webhook_id}/{self.webhook_token}',
            'method': 'POST',
            'username': username,
        }
        if embeds:
            params['embeds'] = embeds
        if content:
            params['content'] = content
        return params

    def dispatch(self, data: dict) -> None:
        '''override'''
        embed = {
            'color': self.COLORS.get(data['action'], self.COLORS['default']),
            'author': {
                'name': data['poller'],
            },
            'title': data['target'][0],
            'description': f'# {data["action"].upper()}',
            'fields': []
        }
        embed['fields'].append({'name': 'Path', 'value': data['path']})
        if data['action'] == 'move':
            embed['fields'].append({'name': 'From', 'value': str(pathlib.Path(data["removed_path"], data['target'][0])) if data['removed_path'] else f'unknown'})
        elif data.get('action_detail'):
            embed['fields'].append({'name': 'Details', 'value': data["action_detail"]})
        embed['fields'].append({'name': 'ID', 'value': data['target'][1]})
        embed['fields'].append({'name': 'MIME', 'value': data['target'][2]})
        embed['fields'].append({'name': 'Link', 'value': data['url']})
        embed['fields'].append({'name': 'Occurred at', 'value': data['timestamp']})
        response = self.webhook(embeds=[embed])
        log_msg = f"Discord target=\"{data['target'][0]}\" status_code={response.status_code}"
        if str(response.status_code)[0] == '2':
            logger.debug(log_msg)
        else:
            logger.error(log_msg + f' content="{response.text}"')


class RcloneDispatcher(Dispatcher):

    def __init__(self, url: str, *args: tuple, **kwds: dict) -> None:
        super(RcloneDispatcher, self).__init__(*args, **kwds)
        url = urllib.parse.urlparse(url)
        if not url.netloc or not url.scheme:
            raise Exception(f'Rclone RC 리모트 주소를 입력하세요: {url}')
        if url.fragment:
            self.vfs = f'{url.fragment}:'
        else:
            self.vfs = None
        self.user = url.username
        self.password = url.password
        try:
            self.url = urllib.parse.urlunparse([url.scheme, url.netloc, '', '', '', ''])
            self.url_parts = urllib.parse.urlparse(self.url)
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error(f'url: {url}')
            raise e

    def api(path: str, method: str = 'GET') -> callable:
        def decorator(class_method: callable) -> callable:
            @functools.wraps(class_method)
            def wrapper(self, *args: tuple, **kwds: dict) -> dict:
                """
                params 및 data 값을 입력해야 할 경우 아래의 딕셔너리 형태로 리턴

                @api('/path/{sub_path}', method='POST')
                def test(self, sub_path: str, param1: str, param2: int) -> dict:
                    return {
                        'params': {
                            'a': param1,
                            'b': parma2,
                        },
                        'data': {
                            'c': 3,
                            'd': 4,
                        },
                    }

                `/path/{sub_path}` 문자열의 `{sub_path}`는 test 메소드에서 입력받은 동일한 이름의 `sub_path` 파라미터의 값으로 대체 됨

                test('login') -> /path/login

                params 및 data 값을 입력할 필요가 없을 경우 return 하지 않아도 무관

                @api('/ip')
                def no_return(self) -> dict:
                    pass
                """
                bound = inspect.signature(class_method).bind(self, *args, **kwds)
                api_path = path.format(**bound.arguments)
                api = class_method(self, *args, **kwds) or {}
                params = api.get('params')
                data = api.get('data')
                headers = api.get('headers')
                url = urllib.parse.urlunparse((
                    self.url_parts.scheme,
                    self.url_parts.netloc,
                    api_path,
                    self.url_parts.params,
                    self.url_parts.query,
                    self.url_parts.fragment
                ))
                return parse_json_response(request(
                    method,
                    url,
                    params=params,
                    data=data,
                    auth=(self.user, self.password),
                    headers=headers
                ))
            return wrapper
        return decorator

    @api('/vfs/stats', method='JSON')
    def api_vfs_stats(self, fs: str = None) -> dict:
        tmp = fs or self.vfs
        if tmp:
            return {'data': {'fs': fs}}

    @api('/vfs/refresh', method='JSON')
    def api_vfs_refresh(self, remote_path: str, recursive: bool = False, fs: str = None) -> dict:
        data = {
            'dir': remote_path,
            'recursive': str(recursive).lower()
        }
        fs_tmp = fs or self.vfs
        if fs_tmp:
            data['fs'] = fs_tmp
        return {'data': data}

    @api('/operations/stat', method='JSON')
    def api_operations_stat(self, remote_path: str, opts: Optional[dict] = None, fs: str = None) -> dict:
        data = {
            'remote': remote_path,
        }
        fs_tmp = fs or self.vfs
        if fs_tmp:
            data['fs'] = fs_tmp
        if opts:
            data['opt'] = opts
        return {'data': data}

    @api('/vfs/forget', method='JSON')
    def api_vfs_forget(self, local_path: str, is_directory: bool = False) -> dict:
        data = {}
        if is_directory:
            data['dir'] = local_path
        else:
            data['file'] = local_path
        return {'data': data}

    def dispatch(self, data: dict) -> None:
        '''override'''
        self.refresh(data['path'], is_directory=data['is_folder'])
        if data.get('removed_path'):
            self.api_vfs_forget(data['removed_path'], data['is_folder'])

    def get_metadata_cache(self) -> tuple[int, int]:
        result = self.api_vfs_stats(self.vfs).get("metadataCache", {})
        if not result:
            logger.error(f'No metadata cache statistics, assumed 0...')
        return result.get('dirs', 0), result.get('files', 0)

    def is_file(self, remote_path: str) -> bool:
        result: dict = self.api_operations_stat(remote_path, self.vfs)
        item = result.get('item', {})
        return (item.get('IsDir').lower() == 'true') if item else False

    def refresh(self, local_path: str, recursive: bool = False, is_directory: bool = False) -> None:
        local_path = pathlib.Path(local_path)
        remote_path = pathlib.Path(map_path(str(local_path), self.mappings)) if self.mappings else local_path
        parents: list[pathlib.Path] = list(remote_path.parents)
        to_be_tested = remote_path.as_posix() if is_directory else parents.pop(0).as_posix()
        not_exists_paths = []
        result = self.api_vfs_refresh(to_be_tested, recursive)
        while not result['result'].get(to_be_tested) == 'OK':
            if result['result'].get(to_be_tested) == 'file does not exist':
                not_exists_paths.insert(0, to_be_tested)
            if parents:
                to_be_tested = parents.pop(0).as_posix()
                result = self.api_vfs_refresh(to_be_tested, recursive)
            else:
                logger.warning(f'Hit the top-level path.')
                break
        for path in not_exists_paths:
            if local_path.exists():
                break
            result = self.api_vfs_refresh(path, recursive)
            if not result['result'].get(path) == 'OK':
                break
        logger.debug(f'vfs/refresh result: {result}')


class PlexDispatcher(Dispatcher):

    def __init__(self, url: str, token: str, *args: tuple, **kwds: dict) -> None:
        super(PlexDispatcher, self).__init__(*args, **kwds)
        self.url = url.strip().strip('/')
        self.url_parts = urllib.parse.urlparse(self.url)
        self.token = token.strip()

    def api(path: str, method: str = 'GET') -> callable:
        def decorator(class_method: callable) -> callable:
            @functools.wraps(class_method)
            def wrapper(self, *args: tuple, **kwds: dict) -> dict:
                bound = inspect.signature(class_method).bind(self, *args, **kwds)
                api_path = path.format(**bound.arguments)
                api = class_method(self, *args, **kwds) or {}
                params = api.get('params', {})
                params['X-Plex-Token'] = self.token
                data = api.get('data')
                url = urllib.parse.urlunparse((
                    self.url_parts.scheme,
                    self.url_parts.netloc,
                    api_path,
                    self.url_parts.params,
                    self.url_parts.query,
                    self.url_parts.fragment
                ))
                return parse_json_response(request(
                    method,
                    url,
                    params=params,
                    data=data,
                    headers={'Accept': 'application/json'}
                ))
            return wrapper
        return decorator

    @api('/library/sections/{section}/refresh')
    def api_refresh(self, section: int, path: Optional[str] = None, force: bool = False) -> dict:
        params = {}
        if force:
            params['force'] = 1
        if path:
            params['path'] = path
        return {'params': params}

    @api('/library/sections')
    def api_sections(self) -> dict:
        pass

    def dispatch(self, data: dict) -> None:
        '''override'''
        self.scan(data['path'], is_directory=data.get('is_folder'))
        if data.get('removed_path'):
            self.scan(data.get('removed_path'), is_directory=data.get('is_folder'))

    def get_section_by_path(self, path: str) -> int:
        plex_path = pathlib.Path(map_path(path, self.mappings)) if self.mappings else pathlib.Path(path)
        sections = self.api_sections()
        for directory in sections['MediaContainer']['Directory']:
            for location in directory['Location']:
                if plex_path.is_relative_to(location['path']) or \
                   pathlib.Path(location['path']).is_relative_to(plex_path):
                    return int(directory['key'])

    def scan(self, path: str, force: bool = False, is_directory: bool = False) -> None:
        scan_target = path if is_directory else pathlib.Path(path).parent.as_posix()
        section = self.get_section_by_path(scan_target) or -1
        logger.debug(f'Plex scan: {scan_target=} {section=}')
        self.api_refresh(section, scan_target, force)


class RclonePlexDispatcher(Dispatcher):

    def __init__(self, rclone_url, plex_url, plex_token, queue_interval: int = 30, rclone_mappings: dict = None, plex_mappings: dict = None, *args: tuple, **kwds: dict) -> None:
        super(RclonePlexDispatcher, self).__init__(*args, **kwds)
        self.rclone_dispatcher = RcloneDispatcher(rclone_url, mappings=rclone_mappings, *args, **kwds)
        self.plex_dispatcher = PlexDispatcher(plex_url, plex_token, mappings=plex_mappings, *args, **kwds)
        self.path_queue = PathQueue()
        self.queue_interval = queue_interval

    def dispatch(self, data: dict) -> None:
        '''override'''
        path_item = PathItem(get_last_dir(data['path'], data['is_folder']), data['path'], data['is_folder'])
        self.path_queue.put(path_item)
        if data.get('removed_path'):
            self.rclone_dispatcher.api_vfs_forget(data['removed_path'], data['is_folder'])

    async def on_start(self) -> None:
        '''override'''
        logger.debug(f'RclonePlexDispatcher starts...')
        while not self.stop_event.is_set():
            while not self.path_queue.is_empty():
                item = self.path_queue.get()
                logger.debug(item)
                self.rclone_dispatcher.refresh(item.key, is_directory=True)
                self.plex_dispatcher.scan(item.key, is_directory=True)
            for _ in range(self.queue_interval):
                await asyncio.sleep(1)
                if self.stop_event.is_set(): break
        logger.debug(f'RclonePlexDispatcher ends...')
