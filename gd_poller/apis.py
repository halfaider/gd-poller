import time
import pathlib
import logging
import inspect
import asyncio
import datetime
import functools
import urllib.parse
from typing import Any, Callable, Sequence, cast

import httpx

from . import __version__
from .helpers.helpers import get_bool, get_int
from .http import (
    parse_response,
    get_default_headers,
    get_default_timeout,
    async_apply_cache,
)

logger = logging.getLogger(__name__)


def http_api(path: str, method: str = "GET", interval: float = 0.0) -> Callable:
    """
    api에 추가적인 데이터가 필요한 경우 딕셔너리 형태로 리턴

        @http_api('/path/{sub_path}/{extra_path}', method='POST')
        def test(self, sub_path: str, param1: str, param2: int, data1: str, data2: str) -> dict:
            return {
                'params': {
                    'a': param1,
                    'b': param2,
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

    params, data, headers는 httpx.AsyncClient의 request로 전달 됨.

    api에 추가적인 데이터가 필요하지 않은 경우 리턴하지 않음

        @http_api('/version')
        def no_return(self) -> dict:
            pass

    api 경로는 python 포멧 형식으로 작성할 수 있고 포멧 키워드는 메소드에서 입력받은 동일한 이름의 파라미터 값으로 대체 됨

        @http_api('/path/{sub_path}', method='POST')
        def test(self, sub_path: str) -> dict:
            pass

        test('login') -> '/path/login'

    혹은 'format' 값을 직접 return 하여 동적으로 api 경로를 생성할 수 있음

        @http_api('/path/{sub_path}/{extra_path}')
        def test(self, sub_path: str) -> dict:
            return {
                'format': {
                    'extra_path': 'users',
                }
            }

        test('group') -> '/path/group/users'
    """

    def decorator(class_method: Callable) -> Callable:
        @functools.wraps(class_method)
        async def wrapper(self: Api, *args: Any, **kwds: Any) -> dict:
            api: dict = class_method(self, *args, **kwds) or {}
            self.adjust_api(api)
            bound = inspect.signature(class_method).bind(self, *args, **kwds)
            api_path: str = path.format(**(api.get("format") or {}), **bound.arguments)
            params: dict | None = api.get("params")
            data: dict | None = api.get("data")
            json_: dict | None = api.get("json")
            headers = dict(api.get("headers") or {})
            auth: tuple | None = api.get("auth")
            url: str = urllib.parse.urlunparse(
                (
                    self.url_parts.scheme,
                    self.url_parts.netloc,
                    self.url_parts.path + api_path,
                    self.url_parts.params,
                    self.url_parts.query,
                    self.url_parts.fragment,
                )
            )
            has_ua = any(k.lower() == "user-agent" for k in headers)
            if not has_ua:
                headers["user-agent"] = f"gd-poller/{__version__}"
            await self.get_sleep_enough(interval)
            self.last_executed_timestamp = time.time()
            """
            {
                'status_code': 200,
                'content': '...',
                'exception': None,
                'json': {...},
                'url': 'https://...',
            }
            """
            response = await self.client.request(
                method,
                url,
                params=params,
                data=data,
                json=json_,
                auth=auth,
                headers=headers,
            )
            return parse_response(response)

        return wrapper

    return decorator


class Api:

    _cache_enable = False
    _cache_ttl = 600  # seconds
    _cache_maxsize = 64  # each
    _last_executed_timestamp = time.time()

    def __init__(
        self,
        url: str = "",
        cache_enable: bool = False,
        cache_maxsize: int = 64,
        cache_ttl: int = 600,
        headers: dict | None = None,
        timeout: float | None = None,
    ) -> None:
        self.url = url.strip().strip("/")
        self._cache_enable = cache_enable
        self._cache_ttl = cache_ttl
        self._cache_maxsize = cache_maxsize
        client_headers = dict(get_default_headers())
        if headers:
            client_headers.update(headers)
        client_timeout = timeout if timeout is not None else get_default_timeout()
        self._client = httpx.AsyncClient(timeout=client_timeout, headers=client_headers)
        self._semaphore = asyncio.Semaphore(5)

    @property
    def url(self) -> str:
        return self._url

    @url.setter
    def url(self, url: str | None) -> None:
        self._url = url or ""
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
    def client(self) -> httpx.AsyncClient:
        return self._client

    @property
    def last_executed_timestamp(self) -> float:
        return self._last_executed_timestamp

    @last_executed_timestamp.setter
    def last_executed_timestamp(self, value: float) -> None:
        self._last_executed_timestamp = value

    def adjust_api(self, api_data: dict) -> None: ...

    async def get_sleep_enough(self, interval: float) -> None:
        sleep_time = interval - (time.time() - self.last_executed_timestamp)
        if sleep_time > 0:
            logger.debug(
                f"Sleep for {sleep_time} seconds to complete a {interval}-second interval..."
            )
            await asyncio.sleep(sleep_time)

    async def aclose(self) -> None:
        await self._client.aclose()


class GoogleDrive(Api):

    def __init__(
        self,
        token: dict,
        scopes: tuple = (),
        cache_enable: bool = False,
        cache_maxsize: int = 64,
        cache_ttl: int = 600,
        headers: dict | None = None,
    ):
        super().__init__(
            cache_enable=cache_enable,
            cache_maxsize=cache_maxsize,
            cache_ttl=cache_ttl,
            headers=headers,
        )
        self._token = token
        self._scopes = scopes
        self._client_id = token.get("client_id") or ""
        self._client_secret = token.get("client_secret") or ""
        self._refresh_token = token.get("refresh_token") or ""
        self._access_token = token.get("token") or ""
        self._expiry: datetime.datetime | None = None
        self._refresh_lock = asyncio.Lock()
        if self.cache_enable:
            # 메소드에 직접 데코레이터를 사용하는 대신 __init__ 에서 캐시를 씌우면 각 객체별로 독립적인 캐시를 갖게 됨
            self.get_file = async_apply_cache(self._get_file, self.cache_maxsize)
            self.get_files = async_apply_cache(self._get_files, self.cache_maxsize)
        else:
            self.get_file = self._get_file
            self.get_files = self._get_files

    @property
    def token(self) -> dict[str, Any]:
        return self._token

    @property
    def scopes(self) -> tuple:
        return self._scopes

    async def get_access_token(self) -> str:
        now = datetime.datetime.now(datetime.timezone.utc)
        if self._access_token and self._expiry and now < self._expiry:
            return self._access_token
        async with self._refresh_lock:
            if self._access_token and self._expiry and now < self._expiry:
                return self._access_token
            res = await self.client.post(
                "https://oauth2.googleapis.com/token",
                data={
                    "client_id": self._client_id,
                    "client_secret": self._client_secret,
                    "refresh_token": self._refresh_token,
                    "grant_type": "refresh_token",
                },
            )
            if not res.is_success:
                logger.error(
                    f"Google OAuth2 Token Refresh Failed: status_code={res.status_code} body={res.text}"
                )
                raise Exception(f"Google OAuth2 Refresh Failed: {res.status_code}")
            data = res.json()
            self._access_token = data.get("access_token") or ""
            expires_in = int(data.get("expires_in") or 3600)
            self._expiry = now + datetime.timedelta(seconds=max(expires_in - 60, 60))
            return self._access_token

    async def get_auth_headers(self) -> dict[str, str]:
        token = await self.get_access_token()
        return {"Authorization": f"Bearer {token}"}

    async def query_activity(self, body_data: dict[str, Any]) -> dict[str, Any] | None:
        try:
            auth_headers = await self.get_auth_headers()
            res = await self.client.post(
                "https://driveactivity.googleapis.com/v2/activity:query",
                json=body_data,
                headers=auth_headers,
            )
            if not res.is_success:
                logger.error(
                    f"Google Activity query: status_code={res.status_code} body={res.text}"
                )
                return None
            return res.json()
        except Exception as e:
            self.handle_error(e)
            return None

    async def get_full_path(
        self, item_id: str, ancestor_id: str = "", root: str = ""
    ) -> tuple[str, tuple[str, str], str, int] | None:
        if not item_id:
            logger.error(f'ID를 확인하세요: "{item_id}"')
            return None
        async with self._semaphore:
            # do not use cache
            file = await self._get_file(item_id)
        if not file:
            return None
        web_view = file.get("webViewLink") or ""
        size = get_int(file.get("size"))
        current_path: list[tuple[str, str]]
        if root and item_id == ancestor_id:
            current_path = [(root, ancestor_id)]
        else:
            current_path = [(file.get("name") or "", file.get("id") or "")]
            break_counter = 100
            while (parents := file.get("parents")) and break_counter > 0:
                async with self._semaphore:
                    file = await self.get_file(
                        parents[0], ttl_hash=self.get_ttl_hash()
                    )
                if not file:
                    return None
                if root and file.get("id") == ancestor_id:
                    current_path.append((root, ancestor_id))
                    break
                else:
                    current_path.append((file.get("name") or "", file.get("id") or ""))
                break_counter -= 1
        last_ancestor_id = current_path[-1][1]
        if last_ancestor_id and len(last_ancestor_id) < 20:
            current_path[-1] = (f"/{last_ancestor_id}", last_ancestor_id)
        full_path = pathlib.Path(*(p[0] for p in current_path[::-1] if p[0]))
        parent = current_path[1] if len(current_path) > 1 else current_path[0]
        if self.cache_enable:
            logger.debug(f"get_file(): {cast(Any, self.get_file).cache_info()}")
        return str(full_path), parent, web_view, size

    async def _get_file(
        self,
        item_id: str,
        fields: str = "id, name, parents, mimeType, webViewLink, size, shortcutDetails",
        **kwds: Any,
    ) -> dict[str, Any] | None:
        try:
            auth_headers = await self.get_auth_headers()
            res = await self.client.get(
                f"https://www.googleapis.com/drive/v3/files/{item_id}",
                params={"fields": fields, "supportsAllDrives": "true"},
                headers=auth_headers,
            )
            if not res.is_success:
                logger.error(
                    f"Google Drive get_file: item_id={item_id} status_code={res.status_code} body={res.text}"
                )
                return None
            return res.json()
        except Exception as e:
            self.handle_error(e)

    async def _get_files(
        self,
        query: str,
        order_by: str = "folder,modifiedTime desc,name",
        page_token: str | None = None,
        page_size: int = 100,
        **kwds: Any,
    ) -> dict[str, Any] | None:
        try:
            auth_headers = await self.get_auth_headers()
            params: dict[str, Any] = {
                "q": query,
                "supportsAllDrives": "true",
                "includeItemsFromAllDrives": "true",
                "orderBy": order_by,
                "pageSize": page_size,
                "fields": "nextPageToken, files(id, name, mimeType, size)",
            }
            if page_token:
                params["pageToken"] = page_token
            res = await self.client.get(
                "https://www.googleapis.com/drive/v3/files",
                params=params,
                headers=auth_headers,
            )
            if not res.is_success:
                logger.error(
                    f"Google Drive get_files: status_code={res.status_code} body={res.text}"
                )
                return None
            return res.json()
        except Exception as e:
            self.handle_error(e)

    async def get_children(
        self, folder_id: str, limit: int = 100, is_shortcut: bool = False
    ) -> list[tuple[str, str, str, int]]:
        """
        Returns:
            tuple: id, name, mime_type, size
        """
        file_list = []
        try:
            try:
                if is_shortcut and (
                    real_target := await self.get_real_target(folder_id)
                ):
                    if real_target[1]:
                        folder_id = real_target[1]
            except Exception as e:
                self.handle_error(e)
            # 캐시 없이 검색
            async with self._semaphore:
                files = await self._get_files(
                    f"'{folder_id}' in parents and trashed = false",
                    page_size=limit,
                )
            if not files:
                return file_list
            for file in files.get("files") or ():
                file_mime = file.get("mimeType") or ""
                file_id = file.get("id") or ""
                file_name = file.get("name") or ""
                try:
                    file_size = int(file.get("size") or 0)
                except Exception:
                    file_size = 0
                file_list.append((file_id, file_name, file_mime, file_size))
            if self.cache_enable:
                logger.debug(f"get_files(): {cast(Any, self.get_files).cache_info()}")
                logger.debug(f"get_file(): {cast(Any, self.get_file).cache_info()}")
        except Exception as e:
            self.handle_error(e)
        return file_list

    async def get_real_target(self, shortcut_id: str) -> tuple[str, str, str] | None:
        async with self._semaphore:
            shortcut_file = await self.get_file(
                shortcut_id, ttl_hash=self.get_ttl_hash()
            )
            if shortcut_file:
                if real_target := shortcut_file.get("shortcutDetails"):
                    return (
                        shortcut_file.get("name") or "",
                        real_target.get("targetId") or "",
                        real_target.get("targetMimeType") or "",
                    )
        logger.warning(f"Could not find the target for: {shortcut_id}")

    def handle_error(self, error: Exception) -> None:
        logger.exception(error)

    def get_ttl_hash(self) -> int | float:
        if self.cache_enable:
            return round(time.time() / self.cache_ttl)
        return time.time()


class Rclone(Api):

    vfs = None
    user = None
    password = None

    def __init__(self, url: str) -> None:
        super().__init__(url)
        url_parsed = urllib.parse.urlparse(url)
        if not url_parsed.netloc or not url_parsed.scheme:
            raise Exception(f"Rclone RC 리모트 주소를 입력하세요: {url}")
        if url_parsed.fragment:
            self.vfs = f"{url_parsed.fragment}:"
        else:
            self.vfs = None
        self.user = url_parsed.username
        self.password = url_parsed.password
        try:
            self.url = urllib.parse.urlunparse(
                (url_parsed.scheme, url_parsed.netloc, "", "", "", "")
            )
        except Exception as e:
            logger.exception(f"Rclone: {url=}")
            raise

    def adjust_api(self, api_data: dict) -> None:
        api_data["auth"] = (
            (self.user, self.password) if self.user and self.password else None
        )

    @http_api("/vfs/stats", method="POST")
    def api_vfs_stats(self, fs: str | None = None) -> dict:
        data = {}
        data = self.set_vfs(fs, data)
        return {"json": data}

    @http_api("/vfs/refresh", method="POST")
    def api_vfs_refresh(
        self,
        remote_path: str | None = None,
        recursive: bool = False,
        fs: str | None = None,
    ) -> dict:
        data = {"recursive": str(recursive).lower()}
        if remote_path:
            data["dir"] = remote_path
        data = self.set_vfs(fs, data)
        return {"json": data}

    @http_api("/operations/stat", method="POST")
    def api_operations_stat(
        self, remote_path: str, opts: dict | None = None, fs: str | None = None
    ) -> dict:
        data = {
            "remote": remote_path,
        }
        data = self.set_vfs(fs, data)
        if opts:
            data["opt"] = opts
        return {"json": data}

    @http_api("/vfs/forget", method="POST")
    def api_vfs_forget(
        self, local_path: str, is_directory: bool = False, fs: str | None = None
    ) -> dict:
        data = {"dir" if is_directory else "file": local_path}
        data = self.set_vfs(fs, data)
        return {"json": data}

    def set_vfs(self, vfs: str | None, data: dict) -> dict:
        fs = vfs or self.vfs
        if fs:
            data["fs"] = fs
        return data

    async def get_metadata_cache(self) -> tuple[int, int]:
        res = await self.api_vfs_stats(self.vfs)
        result: dict = (res.get("json") or {}).get(
            "metadataCache"
        ) or {}
        if not result:
            logger.error(f"Rclone: No metadata cache statistics, assumed 0...")
        return result.get("dirs") or 0, result.get("files") or 0

    async def is_dir(self, remote_path: str) -> bool:
        res = await self.api_operations_stat(remote_path, fs=self.vfs)
        item: dict = (res.get("json") or {}).get("item") or {}
        return get_bool(item.get("IsDir"))

    async def refresh(self, remote_path: str, recursive: bool = False) -> None:
        target = pathlib.Path(remote_path)
        for parent in target.parents:
            if parent == parent.parent:
                result = (await self.api_vfs_refresh()).get("json") or {}
            else:
                result = (await self.api_vfs_refresh(parent.as_posix())).get("json") or {}
            logger.debug(f"Rclone: {result}")
            if (
                (result.get("result") or {}).get(parent.as_posix()) or ""
            ).lower() == "ok":
                break
            if (result.get("result") or {}).get("error"):
                return
        else:
            logger.error(f"It has hit the root path: {str(target)}")
            return
        result = (await self.api_vfs_refresh(target.as_posix(), recursive)).get("json")
        logger.info(f"Rclone: {result}")

    async def forget(self, remote_path: str, is_directory: bool = False) -> None:
        res = await self.api_vfs_forget(remote_path, is_directory)
        logger.info(
            f'Rclone: {res.get("json")}'
        )


class Plex(Api):

    token = None

    def __init__(self, url: str, token: str) -> None:
        super().__init__(url)
        self.token = token.strip()
        self._sections: dict | None = None

    def adjust_api(self, api_data: dict) -> None:
        if "params" not in api_data:
            api_data["params"] = {}
        api_data["params"]["X-Plex-Token"] = self.token
        api_data["headers"] = {"Accept": "application/json"}

    @http_api("/library/sections/{section}/refresh")
    def api_refresh(
        self, section: int, path: str | None = None, force: bool = False
    ) -> dict:
        params = {}
        if force:
            params["force"] = 1
        if path:
            params["path"] = path
        return {"params": params}

    @http_api("/library/sections")
    def api_sections(self) -> dict: ...

    @http_api("/library/metadata/{metadata_id}/refresh")
    def api_metadata_refresh(self, metadata_id: int) -> dict: ...

    async def get_sections(self, refresh: bool = False) -> dict | None:
        if self._sections is None or refresh:
            result = await self.api_sections()
            self._sections = result.get("json")
            if not self._sections:
                logger.error(
                    f'No section information, status_code={result.get("status_code")}'
                )
        return self._sections

    async def get_section_by_path(self, path: str) -> int:
        path_ = pathlib.Path(path)
        for retry in (False, True):
            sections = await self.get_sections(refresh=retry)
            if not sections:
                continue
            for directory in sections.get("MediaContainer", {}).get("Directory", []):
                for location in directory.get("Location", []):
                    loc_path = pathlib.Path(location.get("path", ""))
                    if path_.is_relative_to(loc_path) or loc_path.is_relative_to(path_):
                        return int(directory["key"])
            if not retry:
                continue
        return -1

    async def scan(self, path: str, force: bool = False, is_directory: bool = True) -> None:
        scan_target = path if is_directory else str(pathlib.Path(path).parent)
        section = (await self.get_section_by_path(scan_target)) or -1
        result = await self.api_refresh(section, scan_target, force)
        logger.info(
            f"Plex: {scan_target=} {section=} status_code='{result.get('status_code')}'"
        )


class Kavita(Api):

    apikey = None
    token = None
    refresh_token = None

    def __init__(self, url: str, apikey: str) -> None:
        super().__init__(url)
        self.apikey = apikey.strip()
        # self.set_token()

    def adjust_api(self, api_data: dict) -> None:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, */*",
        }
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        api_data["headers"] = headers

    @http_api("/api/Plugin/authenticate", method="POST")
    def api_plugin_authenticate(self) -> dict:
        return {"params": {"pluginName": "GDPoller", "apiKey": self.apikey}}

    @http_api("/api/Library/scan-folder", method="POST")
    def api_library_scan_folder(self, folder: str) -> dict:
        return {"json": {"folderPath": folder, "apiKey": self.apikey}}

    @http_api("/api/Library/libraries", method="GET")
    def api_libraries(self) -> dict: ...

    @http_api("/api/Series/scan", method="POST")
    def api_series_scan(
        self,
        series_id: int,
        library_id: int = -1,
        force: bool = False,
        colorscape: bool = False,
    ) -> dict:
        return {
            "json": {
                "libraryId": library_id,
                "seriesId": series_id,
                "forceUpdate": force,
                "forceColorscape": colorscape,
            }
        }

    @http_api("/api/Series/{series_id}", method="GET")
    def api_series(self, series_id: int) -> dict: ...

    async def set_token(self) -> None:
        result = await self.api_plugin_authenticate()
        if not 199 < (result.get("status_code") or 0) < 300:
            logger.error(f"kavita: {result}")
        auth = result.get("json") or {}
        self.token = auth.get("token") or ""
        self.refresh_token = auth.get("refreshToken") or ""


class Discord(Api):

    webhook_id = None
    webhook_token = None

    def __init__(self, url: str, webhook_id: str, webhook_token: str) -> None:
        super().__init__(url)
        self.webhook_id = webhook_id
        self.webhook_token = webhook_token

    def adjust_api(self, api_data: dict) -> None:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, */*",
        }
        api_data["headers"] = headers
        api_data["format"] = {
            "webhook_id": self.webhook_id,
            "webhook_token": self.webhook_token,
        }

    @http_api("/webhooks/{webhook_id}/{webhook_token}", method="POST", interval=1.5)
    def api_webhook(
        self,
        username: str = "Activity Poller",
        content: str | None = None,
        embeds: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        data: dict[str, Any] = {"username": username}
        if embeds:
            data["embeds"] = embeds
        if content:
            data["content"] = content
        return {"json": data}


class Flaskfarm(Api):

    apikey = None

    def __init__(self, url: str, apikey: str) -> None:
        super().__init__(url)
        self.apikey = apikey.strip()

    @http_api("/gds_tool/api/fp/broadcast", interval=1.5)
    def api_gds_tool_fp_broadcast(self, gds_path: str, scan_mode: str) -> dict:
        if not gds_path.startswith("/ROOT/GDRIVE"):
            raise Exception(f'The path must start with "/ROOT/GDRIVE/": {gds_path}')
        return {
            "params": {
                "gds_path": gds_path,
                "scan_mode": scan_mode,
                "apikey": self.apikey,
            }
        }

    @http_api("/plex_mate/api/scan/do_scan", method="POST")
    def api_plex_mate_scan_do_scan(self, target: str, mode: str) -> dict:
        return {"data": {"target": target, "mode": mode, "apikey": self.apikey}}

    async def gds_tool_fp_broadcast(self, gds_path: str, scan_mode: str) -> None:
        await self.api_gds_tool_fp_broadcast(gds_path, scan_mode)
        logger.info(f'gds_tool: mode={scan_mode} target="{gds_path}"')


class FlaskfarmaiderBot(Api):

    apikey = None

    def __init__(self, url: str, apikey: str) -> None:
        super().__init__(url)
        self.apikey = apikey.strip()

    @http_api("/api/broadcasts/gds", method="POST")
    def api_broadcast_gds(self, path: str, mode: str) -> dict:
        if not path.startswith("/ROOT/GDRIVE"):
            raise Exception(f'The path must start with "/ROOT/GDRIVE/": {path}')
        return {"data": {"path": path, "mode": mode, "apikey": self.apikey}}

    @http_api("/api/broadcasts/downloader", method="POST")
    def api_broadcast_downloader(
        self, path: str, item: str, file_count: int = 0, total_size: int = 0
    ) -> dict:
        return {
            "data": {
                "path": path,
                "item": item,
                "file_count": file_count,
                "total_size": total_size,
                "apikey": self.apikey,
            }
        }


class Jellyfin(Api):

    apikey = None

    def __init__(self, url: str, apikey: str) -> None:
        super().__init__(url)
        self.apikey = apikey.strip()

    @http_api("/Library/Media/Updated", method="POST")
    def api_library_media_updated(
        self,
        path: str | None = None,
        update_type: str | None = None,
        updates: Sequence[dict] = (),
    ) -> dict:
        if path and update_type:
            updates = ({"Path": path, "UpdateType": update_type},)
        return {"json": {"Updates": updates}}

    def adjust_api(self, api_data: dict) -> None:
        api_data["headers"] = {"Authorization": f"MediaBrowser Token={self.apikey}"}


class Stash(Api):

    apikey = None

    def __init__(self, url: str, apikey: str) -> None:
        super().__init__(url)
        self.apikey = apikey.strip()

    def adjust_api(self, api_data: dict) -> None:
        api_data["headers"] = {"ApiKey": self.apikey}

    @http_api("/graphql", method="POST")
    def api_gql(self, payload: dict) -> dict:
        return {"json": payload}

    async def metadata_scan(
        self,
        paths: Sequence[str],
        rescan: bool = False,
        preview: bool = False,
        cover: bool = True,
        image_preview: bool = False,
        hash: bool = False,
        clip_preview: bool = False,
        sprite: bool = False,
        thumbnail: bool = False,
    ) -> dict:
        return await self.api_gql(
            {
                "operationName": "MetadataScan",
                "variables": {
                    "input": {
                        "rescan": rescan,
                        "scanGenerateClipPreviews": clip_preview,
                        "scanGenerateCovers": cover,
                        "scanGenerateImagePreviews": image_preview,
                        "scanGeneratePhashes": hash,
                        "scanGeneratePreviews": preview,
                        "scanGenerateSprites": sprite,
                        "scanGenerateThumbnails": thumbnail,
                        "paths": paths,
                    }
                },
                "query": "mutation MetadataScan($input: ScanMetadataInput!){metadataScan(input: $input)}",
            }
        )

    async def metadata_clean(self, paths: Sequence[str], dry_run: bool = True) -> dict:
        return await self.api_gql(
            {
                "operationName": "MetadataClean",
                "variables": {
                    "input": {
                        "paths": paths,
                        "dryRun": dry_run,
                    }
                },
                "query": "mutation MetadataClean($input: CleanMetadataInput!){metadataClean(input: $input)}",
            }
        )
