import logging
import traceback
from typing import Any

import requests

from .helpers import await_sync

logger = logging.getLogger(__name__)

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
}


class HelperSession(requests.Session):

    def __init__(self, headers: dict = None, auth: tuple = None, proxies: dict = None) -> None:
        super().__init__()
        self.headers.update(DEFAULT_HEADERS)
        if headers:
            self.headers.update(headers)

    def request(self, method: str, url: str, **kwds: Any) -> requests.Response:
        return super().request(method, url, **kwds)


def get_traceback_response(tb: str) -> requests.Response:
    logger.error(tb)
    response = requests.Response()
    response._content = bytes(tb, "utf-8")
    response.status_code = 0
    return response


def request(method: str, url: str, **kwds: Any) -> requests.Response:
    return requests.request(method, url, **kwds)


async def request_async(method: str, url: str, **kwds: Any) -> requests.Response:
    try:
        return await await_sync(request, method, url, **kwds)
    except:
        return get_traceback_response(traceback.format_exc())


def parse_response(response: requests.Response) -> dict[str, Any]:
    result = {
        "status_code": response.status_code,
        "content": response.text.strip(),
        "exception": None,
        "json": None,
        "url": response.url,
    }
    try:
        result["json"] = response.json()
    except Exception as e:
        result["exception"] = repr(e)
    return result
