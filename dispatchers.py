import logging
import functools
import urllib.parse
import traceback
import pathlib
from typing import Any, Optional

import requests

from helpers import parse_mappings, request, map_path, parse_json_response

logger = logging.getLogger(__name__)


class Dispatcher:

    def __init__(self, mappings: dict = None) -> None:
        self.mappings = parse_mappings(mappings) if mappings else None

    async def dispatch(self, data: dict) -> None:
        raise Exception('이 메소드를 구현하세요.')


class DummyDispatcher(Dispatcher):

    async def dispatch(self, data: dict) -> None:
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
            async def wrapper(self, *args: tuple, **kwds: dict) -> requests.Response:
                data: dict = func(self, *args, **kwds)
                end_point = data.pop('endPoint', '/version')
                by_parameter = data.pop('by_parameter', False)
                data['apiKey'] = self.apikey
                logger.debug(f'{end_point}')
                if by_parameter or method == 'GET':
                    query = urllib.parse.urlencode(data)
                    return await request(method, f'{self.url}{end_point}?{query}', headers=self.headers)
                else:
                    return await request(method, f'{self.url}{end_point}', json=data, headers=self.headers)
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

    async def dispatch(self, data: dict) -> None:
        '''override'''
        await self.set_token()
        kavita_path = map_path(data['path'], self.mappings) if self.mappings else data['path']
        response = await self.library_scan_folder(kavita_path)
        if response.status_code != 200:
            logger.debug(response.text)

    async def set_token(self) -> None:
        response = await self.plugin_authenticate()
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
            async def wrapper(self, *args: tuple, **kwds: dict) -> dict[str, Any]:
                data: dict = func(self, *args, **kwds)
                data['apikey'] = self.apikey
                command = f'{self.PACKAGE}/api/' + '/'.join(func.__name__.split('__'))
                logger.debug(f'{command}: {data}')
                match method:
                    case 'POST':
                        return await request('POST', f'{self.url}/{command}', data=data)
                    case 'GET':
                        query = urllib.parse.urlencode(data)
                        return await request('GET', f'{self.url}/{command}?{query}')
            return wrapper
        return decorator


class GDSToolDispatcher(FlaskfarmDispatcher):

    PACKAGE = 'gds_tool'

    async def dispatch(self, data: dict) -> None:
        '''override'''
        match (data.get('action'), data.get('is_folder')):
            case 'create' | 'move', _:
                await self.fp__broadcast(data['path'], 'ADD')
            case 'delete', True:
                await self.fp__broadcast(data['path'], 'REMOVE_FOLDER')
            case 'delete', False:
                await self.fp__broadcast(data['path'], 'REMOVE_FILE')

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

    async def dispatch(self, data: dict) -> None:
        '''override'''
        remote_path = map_path(data['path'], self.mappings) if self.mappings else data['path']
        if data['action'] == 'delete':
            mode = 'REMOVE_FOLDER' if data['is_folder'] else 'REMOVE_FILE'
        else:
            mode = 'ADD'
        logger.info(f'Plexmate: {await self.scan__do_scan(remote_path, mode=mode)}')

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
        super(DiscordDispatcher).__init__(*args, **kwds)
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
        async def wrapper(self, *args, **kwds) -> dict:
            params = func(self, *args, ** kwds)
            api = params.pop('api')
            method = params.pop('method')
            logger.debug(params)
            return await request(method, f'{self.API_URL}{api}', json=params, headers=self.headers)
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

    async def dispatch(self, data: dict) -> None:
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
        if data.get('action_detail'):
            #embed['fields'].append({'name': 'Action detail', 'value': data['action_detail']})
            embed['description'] += f'\n{data["action_detail"]}'
        embed['fields'].append({'name': 'ID', 'value': data['target'][1]})
        embed['fields'].append({'name': 'MIME', 'value': data['target'][2]})
        embed['fields'].append({'name': 'Link', 'value': data['url']})
        embed['fields'].append({'name': 'Occurred at', 'value': data['timestamp']})
        response = await self.webhook(embeds=[embed])
        if not str(response.status_code)[0] == '2':
            logger.error(f'webhook status_code: {response.status_code}')


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
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error(f'url: {url}')
            raise e

    async def dispatch(self, data: dict) -> None:
        '''override'''
        if not data['is_folder']:
            await self.refresh(data['path'], data['is_folder'])
        else:
            logger.debug(f'RcloneDispatcher: It is a folder: {data["path"]}')

    def command(method: callable) -> callable:
        @functools.wraps(method)
        async def wrapper(self, *args: tuple, **kwds: dict) -> dict:
            command = '/'.join(method.__name__.split('__'))
            data: dict = method(self, *args, **kwds)
            logger.debug(f'{command}: {data}')
            # {'error': '', ...}
            # {'result': {'/path/to': 'Invalid...'}}
            # {'result': {'/path/to': 'OK'}}
            # {'forgotten': ['/path/to']}
            return parse_json_response(await request("JSON", f'{self.url}/{command}', data=data, auth=(self.user, self.password)))
        return wrapper

    async def get_metadata_cache(self) -> tuple[int, int]:
        result = await self.vfs__stats(self.vfs).get("metadataCache", {})
        if not result:
            logger.error(f'No metadata cache statistics, assumed 0...')
        return result.get('dirs', 0), result.get('files', 0)

    @command
    def vfs__stats(self, fs: str) -> dict:
        if self.vfs:
            return {'fs': fs}
        return {}

    @command
    def vfs__refresh(self, remote_path: str, recursive: bool = False) -> dict:
        data = {
            'dir': remote_path,
            'recursive': str(recursive).lower()
        }
        if self.vfs:
            data['fs'] = self.vfs
        return data

    @command
    def operations__stat(self, remote_path: str, opts: Optional[dict] = None) -> dict:
        data = {
            'remote': remote_path,
        }
        if self.vfs:
            data['fs'] = self.vfs
        if opts:
            data['opt'] = opts
        return data

    async def is_file(self, remote_path: str) -> bool:
        result: dict = await self.operations__stat(remote_path, self.vfs)
        item = result.get('item', {})
        return (item.get('IsDir').lower() == 'true') if item else False

    async def refresh(self, local_path: str, recursive: bool = False, is_directory: bool = False) -> None:
        local_path = pathlib.Path(local_path)
        remote_path = pathlib.Path(map_path(str(local_path), self.mappings)) if self.mappings else local_path
        parents: list[pathlib.Path] = list(remote_path.parents)
        to_be_tested = str(remote_path) if is_directory else str(parents.pop(0))
        not_exists_paths = []
        result = await self.vfs__refresh(to_be_tested, recursive)
        while result['result'].get(to_be_tested) == 'file does not exist':
            not_exists_paths.insert(0, to_be_tested)
            if parents:
                to_be_tested = parents.pop(0).as_posix()
                result = await self.vfs__refresh(to_be_tested, recursive)
            else:
                logger.warning(f'Hit the top-level path.')
                break
        for path in not_exists_paths:
            if local_path.exists():
                break
            result = await self.vfs__refresh(path, recursive)
            if not result['result'].get(path) == 'OK':
                break
        logger.debug(f'vfs/refresh result: {result}')


class PlexDispatcher(Dispatcher):

    def __init__(self, url: str, token: str, *args: tuple, **kwds: dict) -> None:
        super(PlexDispatcher, self).__init__(*args, **kwds)
        self.url = url.strip().strip('/')
        self.token = token.strip()

    async def dispatch(self, data: dict) -> None:
        '''override'''
        self.scan(data['path'])

    def api(func: callable) -> callable:
        @functools.wraps(func)
        async def wrapper(self, *args: tuple, **kwds: dict) -> dict[str, Any]:
            params: dict = func(self, *args, **kwds)
            key = params.pop('key', '/identity')
            method = params.pop('method', 'GET')
            params['X-Plex-Token'] = self.token
            headers = {'Accept': 'application/json'}
            #logger.debug(f'{key}: {params}')
            return parse_json_response(await request(method, f'{self.url}{key}', params=params, headers=headers))
        return wrapper

    @api
    def refresh(self, section: int, path: Optional[str] = None, force: bool = False) -> dict[str, str]:
        params = {
            'key': f'/library/sections/{section}/refresh',
            'method': 'GET',
        }
        if force:
            params['force'] = 1
        if path:
            params['path'] = path
        return params

    @api
    def sections(self) -> dict[str, str]:
        return {
            'key': '/library/sections',
            'method': 'GET'
        }

    async def get_section_by_path(self, path: str) -> int:
        plex_path = pathlib.Path(map_path(path, self.mappings)) if self.mappings else pathlib.Path(path)
        sections = await self.sections()
        for directory in sections['MediaContainer']['Directory']:
            for location in directory['Location']:
                if plex_path.is_relative_to(location['path']) or \
                   pathlib.Path(location['path']).is_relative_to(plex_path):
                    return int(directory['key'])

    async def scan(self, path: str, force: bool = False) -> None:
        section = self.get_section_by_path(path)
        await self.refresh(section, path, force)
