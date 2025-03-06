import pathlib
import logging
import traceback
import urllib.parse
import functools
import inspect
import time
import threading
from typing import Optional

from helpers import apply_cache, get_ttl_hash, parse_response, check_packages, HelperSession

check_packages([
    ('httplib2', 'httplib2'),
    ('googleapiclient', 'google-api-python-client'),
    ('google.oauth2', 'google-auth')
])

from httplib2 import Http
from google_auth_httplib2 import AuthorizedHttp
from google.oauth2 import credentials
from googleapiclient.discovery import build, Resource
from googleapiclient.http import HttpRequest

logger = logging.getLogger(__name__)


class Api:

    _url = None
    _url_parts = None
    _cache_enable = False
    _cache_ttl = 600 # seconds
    _cache_maxsize = 64 # each

    def __init__(self, url: str = '', cache_enable: bool = False, cache_maxsize: int = 64, cache_ttl: int = 600) -> None:
        self.url = url.strip().strip('/')
        self._cache_enable = cache_enable
        self._cache_ttl = cache_ttl
        self._cache_maxsize = cache_maxsize
        self._session = HelperSession()

    @property
    def url(self) -> str:
        return self._url

    @url.setter
    def url(self, url: str) -> None:
        self._url = url
        self._url_parts = urllib.parse.urlparse(self.url)

    @property
    def url_parts(self) -> urllib.parse.ParseResult:
        return self._url_parts

    @property
    def cache_enable(self) -> bool:
        return self._cache_enable

    @property
    def cache_ttl(self) -> int:
        return self._cache_ttl

    @property
    def cache_maxsize(self) -> int:
        return self._cache_maxsize

    @property
    def session(self) -> HelperSession:
        return self._session

    def http_api(path: str, method: str = 'GET') -> callable:
        """
        api에 추가적인 데이터가 필요한 경우 딕셔너리 형태로 리턴

            @Api.http_api('/path/{sub_path}/{extra_path}', method='POST')
            def test(self, sub_path: str, param1: str, param2: int, data1: str, data2: str) -> dict:
                return {
                    'params': {
                        'a': param1,
                        'b': parma2,
                    },
                    'data': {
                        'c': data1,
                        'd': data2,
                    },
                    'headers': {
                        'Accept': 'application/json'
                    },
                    'auth: ('user', 'password'),
                    'format': {
                        'extra_path': 'additonal_path',
                    }
                }

        params, data, headers는 requests.session 모듈의 request(parmas=params, data=data, headers=headers)로 전달 됨.
        (method='json'일 경우 request(json=data)로 전달)

        api에 추가적인 데이터가 필요하지 않은 경우 리턴하지 않음

            @Api.http_api('/version')
            def no_return(self) -> dict:
                pass

        api 경로는 python 포멧 형식으로 작성할 수 있고 포멧 키워드는 메소드에서 입력받은 동일한 이름의 파라미터 값으로 대체 됨

            @Api.http_api('/path/{sub_path}', method='POST')
            def test(self, sub_path: str) -> dict:
                pass

            test('login') -> '/path/login'

        혹은 'format' 값을 직접 return 하여 동적으로 api 경로를 생성할 수 있음

            @Api.http_api('/path/{sub_path}/{extra_path}')
            def test(self, sub_path: str) -> dict:
                return {
                    'format': {
                        'extra_path': 'users',
                    }
                }

            test('group') -> '/path/group/users'
        """
        def decorator(class_method: callable) -> callable:
            @functools.wraps(class_method)
            def wrapper(self: Api, *args: tuple, **kwds: dict) -> dict:
                api: dict = class_method(self, *args, **kwds) or {}
                self.adjust_api(api)
                bound = inspect.signature(class_method).bind(self, *args, **kwds)
                api_path: str = path.format(**api.get('format', {}), **bound.arguments)
                params: dict = api.get('params')
                data: dict = api.get('data')
                headers: dict = api.get('headers')
                auth: tuple = api.get('auth')
                url: str = urllib.parse.urlunparse((
                    self.url_parts.scheme,
                    self.url_parts.netloc,
                    self.url_parts.path + api_path,
                    self.url_parts.params,
                    self.url_parts.query,
                    self.url_parts.fragment
                ))
                '''
                {
                    'status_code': 200,
                    'content': '...',
                    'exception': None,
                    'json': {...},
                    'url': 'https://...',
                }
                '''
                return parse_response(self.session.request(
                    method,
                    url,
                    params=params,
                    data=data,
                    auth=auth,
                    headers=headers
                ))
            return wrapper
        return decorator

    def adjust_api(self, api_data: dict) -> None:
        pass


class GoogleDrive(Api):

    _token = None
    _scopes = None
    _credentials = None
    _api_drive = None
    _api_activity = None

    def __init__(self, token: dict, scopes: tuple, cache_enable: bool = False, cache_maxsize: int = 64, cache_ttl: int = 600):
        super(GoogleDrive, self).__init__(cache_enable=cache_enable, cache_maxsize=cache_maxsize, cache_ttl=cache_ttl)
        self._token = token
        self._scopes = scopes
        self._credentials: credentials.Credentials = credentials.Credentials.from_authorized_user_info(self.token, self.scopes)
        authorized_http = AuthorizedHttp(self.credentials, http=Http())
        self._api_drive: Resource = build('drive', 'v3', requestBuilder=self.build_google_request, http=authorized_http)
        self._api_activity: Resource = build('driveactivity', 'v2', requestBuilder=self.build_google_request, http=authorized_http)
        if self.cache_enable:
            self.get_file = apply_cache(self.get_file, self.cache_maxsize)
            #def check_cache():
            #    while True:
            #        try:
            #            logger.debug(self.get_file.cache_info())
            #        except:
            #            logger.error(traceback.format_exc())
            #        time.sleep(60)
            #threading.Thread(target=check_cache).start()

    @property
    def token(self) -> str:
        return self._token

    @property
    def scopes(self) -> tuple:
        return self._scopes

    @property
    def credentials(self) -> credentials.Credentials:
        return self._credentials

    @property
    def api_drive(self) -> Resource:
        return self._api_drive

    @property
    def api_activity(self) -> Resource:
        return self._api_activity

    def build_google_request(self, http: AuthorizedHttp, *args, **kwargs):
        # https://googleapis.github.io/google-api-python-client/docs/thread_safety.html
        new_http = AuthorizedHttp(self.credentials, http=Http())
        return HttpRequest(new_http, *args, **kwargs)

    def get_full_path(self, item_id: str, ancestor: str = '') -> tuple[str, tuple[str, str], str]:
        if not item_id:
            raise Exception(f'ID를 확인하세요: "{item_id}"')
        ancestor_id, _, root = ancestor.partition('#')
        # do not use cache
        file = self.get_file(item_id, ttl_hash=time.time())
        web_view = file.get('webViewLink')
        if root and item_id == ancestor_id:
            current_path = [(root, ancestor_id)]
        else:
            current_path = [(file['name'], file['id'])]
            break_conuter = 100
            while file.get('parents') and break_conuter > 0:
                ttl_hash = get_ttl_hash(self.cache_ttl) if self.cache_enable else time.time()
                file = self.get_file(file.get('parents')[0], ttl_hash=ttl_hash)
                if root and file['id'] == ancestor_id:
                    current_path.append((root, ancestor_id))
                    break
                else:
                    current_path.append((file['name'], file['id']))
                break_conuter -= 1
        if len(current_path[-1][1]) < 20:
            current_path[-1] = (f'/{current_path[-1][1]}', current_path[-1][1])
        full_path = pathlib.Path(*[p[0] for p in current_path[::-1] if p[0]])
        parent = current_path[1] if len(current_path) > 1 else current_path[0]
        if self.cache_enable:
            logger.debug(self.get_file.cache_info())
        return str(full_path), parent, web_view

    def get_file(self, item_id: str, fields: str = 'id, name, parents, mimeType, webViewLink', ttl_hash: int | float = 3600) -> dict:
        del ttl_hash
        try:
            result = self.api_drive.files().get(
                fileId=item_id,
                fields=fields,
                supportsAllDrives=True,
            ).execute()
            #logger.debug(f'file={result}')
        except:
            logger.error(traceback.format_exc())
            result = {'id': item_id, 'name': None}
        return result

    def get_files(self, query: str) -> dict:
        result = self.api_drive.files().list(
            q=query,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        ).execute()
        return result


class Rclone(Api):

    vfs = None
    user = None
    password = None

    def __init__(self, url: str) -> None:
        super(Rclone, self).__init__(url)
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
            logger.error(f'Rclone: {url=}')
            raise e

    def adjust_api(self, api_data: dict) -> None:
        '''override'''
        api_data['auth'] = (self.user, self.password) if self.user and self.password else None

    @Api.http_api('/vfs/stats', method='JSON')
    def api_vfs_stats(self, fs: str = None) -> dict:
        data = {}
        data = self.set_vfs(fs, data)
        return {'data': data}

    @Api.http_api('/vfs/refresh', method='JSON')
    def api_vfs_refresh(self, remote_path: str, recursive: bool = False, fs: str = None) -> dict:
        data = {
            'dir': remote_path,
            'recursive': str(recursive).lower()
        }
        data = self.set_vfs(fs, data)
        return {'data': data}

    @Api.http_api('/operations/stat', method='JSON')
    def api_operations_stat(self, remote_path: str, opts: Optional[dict] = None, fs: str = None) -> dict:
        data = {
            'remote': remote_path,
        }
        data = self.set_vfs(fs, data)
        if opts:
            data['opt'] = opts
        return {'data': data}

    @Api.http_api('/vfs/forget', method='JSON')
    def api_vfs_forget(self, local_path: str, is_directory: bool = False, fs: str = None) -> dict:
        data = {
            'dir' if is_directory else 'file': local_path
        }
        data = self.set_vfs(fs, data)
        return {'data': data}

    def set_vfs(self, vfs: str, data: dict) -> dict:
        fs = vfs or self.vfs
        if fs:
            data['fs'] = fs
        return data

    def get_metadata_cache(self) -> tuple[int, int]:
        result: dict = self.api_vfs_stats(self.vfs).get('json', {}).get("metadataCache")
        if not result:
            logger.error(f'Rclone: No metadata cache statistics, assumed 0...')
        return result.get('dirs', 0), result.get('files', 0)

    def is_file(self, remote_path: str) -> bool:
        result: dict = self.api_operations_stat(remote_path, self.vfs).get('json', {})
        item: dict = result.get('item', {})
        return item.get('IsDir', 'None').lower() == 'true'

    def refresh(self, remote_path: str, recursive: bool = False) -> None:
        target = pathlib.Path(remote_path)
        result = self.api_vfs_refresh(target.as_posix(), recursive).get('json', {})
        logger.debug(f'Rclone: {result}')
        if result.get('result', {}).get(target.as_posix()) == 'OK':
            return
        for parent in target.parents:
            result: dict[str, dict] = self.api_vfs_refresh(parent.as_posix(), recursive).get('json', {})
            logger.debug(f'Rclone: {result}')
            if result.get('result', {}).get(parent.as_posix()) == 'OK':
                return
        logger.warning(f'Rclone: It has hit the top-level path.')

    def forget(self, local_path: str, is_directory: bool = False) -> None:
        result = self.api_vfs_forget(local_path, is_directory).get('json', {})
        logger.debug(f'Rclone: {result}')


class Plex(Api):

    token = None

    def __init__(self, url: str, token: str) -> None:
        super(Plex, self).__init__(url)
        self.token = token.strip()

    def adjust_api(self, api_data: dict) -> None:
        '''override'''
        if 'params' not in api_data:
            api_data['params'] = {}
        api_data['params']['X-Plex-Token'] = self.token
        api_data['headers'] = {'Accept': 'application/json'}

    @Api.http_api('/library/sections/{section}/refresh')
    def api_refresh(self, section: int, path: Optional[str] = None, force: bool = False) -> dict:
        params = {}
        if force:
            params['force'] = 1
        if path:
            params['path'] = path
        return {'params': params}

    @Api.http_api('/library/sections')
    def api_sections(self) -> dict:
        pass

    @Api.http_api('/library/metadata/{metadata_id}/refresh')
    def api_metadata_refresh(self, metadata_id: int) -> dict:
        pass

    def get_section_by_path(self, path: str) -> int:
        path_ = pathlib.Path(path)
        result = self.api_sections()
        sections = result.get('json')
        if not sections:
            logger.error(f'No section information, status_code={result.get("status_code", 0)}')
            return -1
        for directory in sections['MediaContainer']['Directory']:
            for location in directory['Location']:
                if path_.is_relative_to(location['path']) or \
                   pathlib.Path(location['path']).is_relative_to(path_):
                    return int(directory['key'])

    def scan(self, path: str, force: bool = False, is_directory: bool = True) -> None:
        scan_target = path if is_directory else str(pathlib.Path(path).parent)
        section = self.get_section_by_path(scan_target) or -1
        logger.debug(f'Plex: {scan_target=} {section=}')
        self.api_refresh(section, scan_target, force)


class Kavita(Api):

    apikey = None
    token = None
    refresh_token = None

    def __init__(self, url: str, apikey: str) -> None:
        super(Kavita, self).__init__(url)
        self.apikey = apikey.strip()
        self.set_token()

    def adjust_api(self, api_data: dict) -> None:
        '''override'''
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json, */*'
        }
        if self.token:
            headers['Authorization'] = f'Bearer {self.token}'
        api_data['headers'] = headers

    @Api.http_api('/api/Plugin/authenticate', method='POST')
    def api_plugin_authenticate(self) -> dict:
        return {'params': {'pluginName': 'GDPoller', 'apiKey': self.apikey}}

    @Api.http_api('/api/Library/scan-folder', method='JSON')
    def api_library_scan_folder(self, folder: str) -> dict:
        return {'data': {'folderPath': folder, 'apiKey': self.apikey}}

    def set_token(self) -> None:
        result = self.api_plugin_authenticate()
        if not 199 < result.get('status_code', 0) < 300:
            logger.error(f'kavita: {result}')
        auth = result.get('json', {})
        self.token = auth.get('token') or ''
        self.refresh_token = auth.get('refreshToken') or ''


class Discord(Api):

    webhook_id = None
    webhook_token = None

    def __init__(self, url: str, webhook_id: str, webhook_token: str) -> None:
        super(Discord, self).__init__(url)
        self.webhook_id = webhook_id
        self.webhook_token = webhook_token

    def adjust_api(self, api_data: dict) -> None:
        '''override'''
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json, */*'
        }
        api_data['headers'] = headers
        api_data['format'] = {
            'webhook_id': self.webhook_id,
            'webhook_token': self.webhook_token,
        }

    @Api.http_api('/webhooks/{webhook_id}/{webhook_token}', method='JSON')
    def api_webhook(self, username: str = 'Activity Poller', content: str = None, embeds: list[dict] = None) -> dict:
        data = {
            'username': username
        }
        if embeds:
            data['embeds'] = embeds
        if content:
            data['content'] = content
        return {'data': data}


class Flaskfarm(Api):

    apikey = None

    def __init__(self, url: str, apikey: str) -> None:
        super(Flaskfarm, self).__init__(url)
        self.apikey = apikey.strip()

    @Api.http_api('/gds_tool/api/fp/broadcast')
    def api_gds_tool_fp_broadcast(self, gds_path: str, scan_mode: str) -> dict:
        if not gds_path.startswith('/ROOT/GDRIVE'):
            raise Exception(f'The path must start with "/ROOT/GDRIVE/": {gds_path}')
        return {
            'params': {
                'gds_path': gds_path,
                'scan_mode': scan_mode,
                'apikey': self.apikey
            }
        }

    @Api.http_api('/plex_mate/api/scan/do_scan', method='POST')
    def api_plex_mate_scan_do_scan(self, target: str, mode: str) -> dict:
        return {
            'data': {
                'target': target,
                'mode': mode,
                'apikey': self.apikey
            }
        }

    def gds_tool_fp_broadcast(self, gds_path: str, scan_mode: str) -> dict:
        self.api_gds_tool_fp_broadcast(gds_path, scan_mode)
        logger.info(f'gds_tool: mode={scan_mode} target="{gds_path}"')
