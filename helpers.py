import traceback
import logging
import re
from typing import Any, Optional, Union, Iterable

import requests


logger = logging.getLogger(__name__)


class RedactedFormatter(logging.Formatter):

    def __init__(self, *args, patterns: Iterable = [], substitute: str = '<REDACTED>', **kwds):
        super(RedactedFormatter, self).__init__(*args, **kwds)
        self.patterns = []
        self.substitute = substitute
        for pattern in patterns:
            self.patterns.append(re.compile(pattern))

    def format(self, record):
        msg = super().format(record)
        for pattern in self.patterns:
            match = pattern.search(msg)
            if match:
                if len(match.groups()) > 0:
                    groups = list(match.groups())
                else:
                    groups = [match.group(0)]
                for found in groups:
                    msg = self.redact(re.compile(found), msg)
        return msg

    def redact(self, pattern: re.Pattern, text: str) -> str:
        return pattern.sub(self.substitute, text)


def request(method: str, url: str, data: Optional[dict] = None, timeout: Union[int, tuple, None] = None, **kwds: dict) -> requests.Response:
    try:
        if method.upper() == 'JSON':
            return requests.request('POST', url, json=data or {}, timeout=timeout, **kwds)
        else:
            return requests.request(method, url, data=data, timeout=timeout, **kwds)
    except:
        tb = traceback.format_exc()
        logger.error(tb)
        response = requests.Response()
        response._content = bytes(tb, 'utf-8')
        response.status_code = 0
        return response


def parse_json_response(response: requests.Response) -> dict[str, Any]:
    try:
        result = response.json()
    except Exception as e:
        result = {
            'status_code': response.status_code,
            'content': response.text.strip(),
            'exception': f'{repr(e)}',
        }
    return result


def parse_mappings(mappings: Iterable[str]) -> list[tuple[str]]:
    return [tuple(mapping.split(':')) for mapping in mappings]


def map_path(target: str, mappings: Iterable[Iterable[str]]) -> str:
    for mapping in mappings:
        target = target.replace(mapping[0], mapping[1])
    return target
