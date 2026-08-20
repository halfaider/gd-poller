import logging
import functools
from collections import OrderedDict
from typing import Any, Callable
import httpx

logger = logging.getLogger(__name__)

DEFAULT_HEADERS: dict[str, str] = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
}


def set_default_headers(headers: dict[str, str] | None) -> None:
    global DEFAULT_HEADERS
    if headers:
        DEFAULT_HEADERS = dict(headers)


def get_default_headers() -> dict[str, str]:
    return DEFAULT_HEADERS


def parse_response(response: httpx.Response) -> dict[str, Any]:
    result: dict[str, Any] = {
        "status_code": response.status_code,
        "content": response.text.strip(),
        "exception": None,
        "json": None,
        "url": str(response.url),
    }
    try:
        result["json"] = response.json()
    except Exception as e:
        result["exception"] = repr(e)
    return result


def async_apply_cache(func: Callable, maxsize: int = 64) -> Callable:
    cache: OrderedDict = OrderedDict()

    @functools.wraps(func)
    async def wrapper(*args: Any, ttl_hash: int | float = 3600, **kwds: Any) -> Any:
        key = (args, tuple(sorted(kwds.items())), ttl_hash)
        if key in cache:
            cache.move_to_end(key)
            return cache[key]
        result = await func(*args, **kwds)
        cache[key] = result
        if len(cache) > maxsize:
            cache.popitem(last=False)
        return result

    def cache_info() -> str:
        return f"AsyncCache(size={len(cache)}, maxsize={maxsize})"

    wrapper.cache_info = cache_info  # type: ignore
    return wrapper
