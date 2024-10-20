import pathlib
import logging

from httplib2 import Http
from google_auth_httplib2 import AuthorizedHttp
from google.oauth2 import credentials
from googleapiclient.discovery import build, Resource
from googleapiclient.http import HttpRequest

logger = logging.getLogger(__name__)


class GoogleDrive:

    def __init__(self, token: str = None, scopes: tuple = None, shortcuts: dict = None):
        self._token = token
        self._scopes = scopes
        self._shortcuts = shortcuts
        self._credentials: credentials.Credentials = credentials.Credentials.from_authorized_user_info(self.token, self.scopes)
        authorized_http = AuthorizedHttp(self.credentials, http=Http())
        self._api_drive: Resource = build('drive', 'v3', requestBuilder=self.build_google_request, http=authorized_http)
        self._api_activity: Resource = build('driveactivity', 'v2', requestBuilder=self.build_google_request, http=authorized_http)

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

    @property
    def shortcuts(self) -> dict:
        return self._shortcuts

    def build_google_request(self, http: AuthorizedHttp, *args, **kwargs):
        # https://googleapis.github.io/google-api-python-client/docs/thread_safety.html
        new_http = AuthorizedHttp(self.credentials, http=Http())
        return HttpRequest(new_http, *args, **kwargs)

    def get_full_path(self, item_id: str, ancestor: str = '') -> tuple:
        if not item_id:
            raise Exception(f'ID를 확인하세요: "{item_id}"')
        ancestor_id, _, root = ancestor.partition('#')
        file = self.get_file(item_id)
        if root and item_id == ancestor_id:
            current_path = [(root, ancestor_id)]
        else:
            current_path = [(file['name'], file['id'])]
            while file.get('parents'):
                file = self.get_file(file.get('parents')[0])
                if root and file['id'] == ancestor_id:
                    current_path.append((root, ancestor_id))
                    break
                else:
                    current_path.append((file['name'], file['id']))
        if len(current_path[-1][1]) < 20:
            current_path[-1] = (f'/{current_path[-1][1]}', current_path[-1][1])
        full_path = pathlib.Path(*[p[0] for p in current_path[::-1]])
        parent = current_path[1] if len(current_path) > 1 else current_path[0]
        return full_path.as_posix(), parent

    def get_file(self, item_id: str, fields: str = '*') -> dict:
        result = self.api_drive.files().get(
            fileId=item_id,
            fields=fields,
            supportsAllDrives=True,
        ).execute()
        return result

    def get_files(self, query: str) -> dict:
        result = self.api_drive.files().list(
            q=query,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        ).execute()
        return result
