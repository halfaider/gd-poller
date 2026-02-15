import time
import html
import pathlib
import logging
import inspect
import functools
import urllib.parse
from typing import Any, Optional, Callable, Sequence, TYPE_CHECKING, cast

from httplib2 import Http
from google_auth_httplib2 import AuthorizedHttp
from google.oauth2 import credentials
from googleapiclient.discovery import build, Resource
from googleapiclient.http import HttpRequest
from googleapiclient import errors

from .helpers.helpers import apply_cache, get_ttl_hash
from .helpers.sessions import HelperSession, parse_response

if TYPE_CHECKING:
    from googleapiclient._apis.drive.v3 import DriveResource  # type: ignore[import-not-found]
    from googleapiclient._apis.driveactivity.v2 import DriveActivityResource  # type: ignore[import-not-found]
    from googleapiclient._apis.drive.v3.schemas import File  # type: ignore[import-not-found]
    from googleapiclient._apis.drive.v3.schemas import FileList  # type: ignore[import-not-found]

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

    params, data, headers는 requests.session 모듈의 request(parmas=params, data=data, headers=headers)로 전달 됨.

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

    def decorator(class_method: Callable) -> Callable:
        @functools.wraps(class_method)
        def wrapper(self: Api, *args: Any, **kwds: Any) -> dict:
            api: dict = class_method(self, *args, **kwds) or {}
            self.adjust_api(api)
            bound = inspect.signature(class_method).bind(self, *args, **kwds)
            api_path: str = path.format(**(api.get("format") or {}), **bound.arguments)
            params: dict | None = api.get("params")
            data: dict | None = api.get("data")
            json_: dict | None = api.get("json")
            headers: dict | None = api.get("headers")
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
            """
                {
                    'status_code': 200,
                    'content': '...',
                    'exception': None,
                    'json': {...},
                    'url': 'https://...',
                }
                """
            self.get_sleep_enough(interval)
            self.last_executed_timestamp = time.time()
            return parse_response(
                self.session.request(
                    method,
                    url,
                    params=params,
                    data=data,
                    json=json_,
                    auth=auth,
                    headers=headers,
                )
            )

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
    ) -> None:
        self.url = url.strip().strip("/")
        self._cache_enable = cache_enable
        self._cache_ttl = cache_ttl
        self._cache_maxsize = cache_maxsize
        self._session = HelperSession()

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
    def session(self) -> HelperSession:
        return self._session

    @property
    def last_executed_timestamp(self) -> float:
        return self._last_executed_timestamp

    @last_executed_timestamp.setter
    def last_executed_timestamp(self, value: float) -> None:
        self._last_executed_timestamp = value

    def adjust_api(self, api_data: dict) -> None:
        pass

    def get_sleep_enough(self, interval: float) -> None:
        sleep_time = interval - (time.time() - self.last_executed_timestamp)
        if sleep_time > 0:
            logger.debug(
                f"Sleep for {sleep_time} seconds to complete a {interval}-second interval..."
            )
            time.sleep(sleep_time)


class GoogleDrive(Api):

    def __init__(
        self,
        token: dict,
        scopes: tuple,
        cache_enable: bool = False,
        cache_maxsize: int = 64,
        cache_ttl: int = 600,
    ):
        super().__init__(
            cache_enable=cache_enable, cache_maxsize=cache_maxsize, cache_ttl=cache_ttl
        )
        self._token = token
        self._scopes = scopes
        self._credentials: credentials.Credentials = (
            credentials.Credentials.from_authorized_user_info(self.token, self.scopes)
        )
        authorized_http = AuthorizedHttp(self.credentials, http=Http())
        self._api_drive: "DriveResource" = cast(Any, build)(
            "drive",
            "v3",
            requestBuilder=self.build_google_request,
            http=authorized_http,
        )
        self._api_activity: "DriveActivityResource" = cast(Any, build)(
            "driveactivity",
            "v2",
            requestBuilder=self.build_google_request,
            http=authorized_http,
        )
        if self.cache_enable:
            self.get_file = apply_cache(self.get_file, self.cache_maxsize)

    @property
    def token(self) -> dict[str, Any]:
        return self._token

    @property
    def scopes(self) -> tuple:
        return self._scopes

    @property
    def credentials(self) -> credentials.Credentials:
        return self._credentials

    @property
    def api_drive(self) -> "DriveResource":
        return self._api_drive

    @property
    def api_activity(self) -> "DriveActivityResource":
        return self._api_activity

    def build_google_request(self, http: Any, *args: Any, **kwargs: Any) -> HttpRequest:
        # https://googleapis.github.io/google-api-python-client/docs/thread_safety.html
        new_http = AuthorizedHttp(self.credentials, http=Http())
        return HttpRequest(cast(Http, new_http), *args, **kwargs)

    def get_full_path(
        self, item_id: str, ancestor_id: str = "", root: str = ""
    ) -> tuple[str, tuple[str | None, str | None], str, str | int] | None:
        if not item_id:
            logger.error(f'ID를 확인하세요: "{item_id}"')
            return None
        # do not use cache
        file = self.get_file(item_id, ttl_hash=time.time())
        if not file:
            return None
        web_view = file.get("webViewLink") or ""
        size = file.get("size") or 0
        current_path: list[tuple[str | None, str | None]]
        if root and item_id == ancestor_id:
            current_path = [(root, ancestor_id)]
        else:
            current_path = [(file.get("name"), file.get("id"))]
            break_counter = 100
            while (parents := file.get("parents")) and break_counter > 0:
                ttl_hash = (
                    get_ttl_hash(self.cache_ttl) if self.cache_enable else time.time()
                )
                file = self.get_file(parents[0], ttl_hash=ttl_hash)
                if not file:
                    return None
                if root and file.get("id") == ancestor_id:
                    current_path.append((root, ancestor_id))
                    break
                else:
                    current_path.append((file.get("name"), file.get("id")))
                break_counter -= 1
        last_ancestor_id = current_path[-1][1]
        if last_ancestor_id and len(last_ancestor_id) < 20:
            current_path[-1] = (f"/{last_ancestor_id}", last_ancestor_id)
        full_path = pathlib.Path(*(p[0] for p in current_path[::-1] if p[0]))
        parent = current_path[1] if len(current_path) > 1 else current_path[0]
        if self.cache_enable:
            logger.debug(cast(Any, self.get_file).cache_info())
        return str(full_path), parent, web_view, size

    def get_file(
        self,
        item_id: str,
        fields: str = "id, name, parents, mimeType, webViewLink, size",
        ttl_hash: int | float = 3600,
    ) -> "File | None":
        try:
            result = (
                self.api_drive.files()
                .get(
                    fileId=item_id,
                    fields=fields,
                    supportsAllDrives=True,
                )
                .execute()
            )
            # logger.debug(f'file={result}')
            return result
        except Exception as e:
            self.handle_error(e)
            return None

    def get_files(self, query: str) -> "FileList":
        result = (
            self.api_drive.files()
            .list(
                q=query,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
            .execute()
        )
        return result

    def handle_error(self, error: Exception) -> None:
        if isinstance(error, errors.HttpError):
            logger.error(
                f'Google: error=HttpError status_code={error.resp.status} reason="{html.escape(error.reason.strip())}" uri="{error.uri}"'
            )
        else:
            logger.exception(error)


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

    def get_metadata_cache(self) -> tuple[int, int]:
        result: dict = (self.api_vfs_stats(self.vfs).get("json") or {}).get(
            "metadataCache"
        ) or {}
        if not result:
            logger.error(f"Rclone: No metadata cache statistics, assumed 0...")
        return result.get("dirs") or 0, result.get("files") or 0

    def is_dir(self, remote_path: str) -> bool:
        item: dict = (
            self.api_operations_stat(remote_path, fs=self.vfs).get("json") or {}
        ).get("item") or {}
        return (item.get("IsDir") or "").lower() == "true"

    def refresh(self, remote_path: str, recursive: bool = False) -> None:
        target = pathlib.Path(remote_path)
        for parent in target.parents:
            if parent == parent.parent:
                result = self.api_vfs_refresh().get("json") or {}
            else:
                result = self.api_vfs_refresh(parent.as_posix()).get("json") or {}
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
        result = self.api_vfs_refresh(target.as_posix(), recursive).get("json")
        logger.info(f"Rclone: {result}")

    def forget(self, remote_path: str, is_directory: bool = False) -> None:
        logger.info(
            f'Rclone: {self.api_vfs_forget(remote_path, is_directory).get("json")}'
        )


class Plex(Api):

    token = None

    def __init__(self, url: str, token: str) -> None:
        super().__init__(url)
        self.token = token.strip()

    def adjust_api(self, api_data: dict) -> None:
        if "params" not in api_data:
            api_data["params"] = {}
        api_data["params"]["X-Plex-Token"] = self.token
        api_data["headers"] = {"Accept": "application/json"}

    @http_api("/library/sections/{section}/refresh")
    def api_refresh(
        self, section: int, path: Optional[str] = None, force: bool = False
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

    def get_section_by_path(self, path: str) -> int:
        path_ = pathlib.Path(path)
        result = self.api_sections()
        sections = result.get("json")
        if not sections:
            logger.error(
                f'No section information, status_code={result.get("status_code")}'
            )
            return -1
        for directory in sections["MediaContainer"]["Directory"]:
            for location in directory["Location"]:
                if path_.is_relative_to(location["path"]) or pathlib.Path(
                    location["path"]
                ).is_relative_to(path_):
                    return int(directory["key"])
        return -1

    def scan(self, path: str, force: bool = False, is_directory: bool = True) -> None:
        scan_target = path if is_directory else str(pathlib.Path(path).parent)
        section = self.get_section_by_path(scan_target) or -1
        result = self.api_refresh(section, scan_target, force)
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
                "forceUPdate": force,
                "forceColoerscape": colorscape,
            }
        }

    @http_api("/api/Series/{series_id}", method="GET")
    def api_series(self, series_id: int) -> dict: ...

    def set_token(self) -> None:
        result = self.api_plugin_authenticate()
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

    def gds_tool_fp_broadcast(self, gds_path: str, scan_mode: str) -> None:
        self.api_gds_tool_fp_broadcast(gds_path, scan_mode)
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
        updates: Sequence = (),
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

    def metadata_scan(
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
        return self.api_gql(
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

    def metadata_clean(self, paths: Sequence[str], dry_run: bool = True) -> dict:
        return self.api_gql(
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
