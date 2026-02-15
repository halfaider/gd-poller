import re
import logging
import logging.config
from typing import Any, Sequence

logger = logging.getLogger(__name__)


class RedactingFilter(logging.Filter):

    def __init__(
        self, patterns: Sequence = (), substitute: str = "<REDACTED>", **kwds: Any
    ) -> None:
        super().__init__(**kwds)
        self.patterns = tuple(re.compile(p, re.IGNORECASE) for p in patterns if p)
        self.substitute = substitute

    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = self.redact(record.getMessage())
        # getMessage() 결과에 이미 args가 반영되어 있음
        record.args = ()
        return True

    def redact(self, text: str) -> str:
        for pattern in self.patterns:
            if pattern.groups == 0:
                text = pattern.sub(self.substitute, text)
            else:
                text = pattern.sub(self.replace_match_groups, text)

        return text

    def replace_match_groups(self, match: re.Match) -> str:
        full_match_text = match.group(0)
        match_start_pos = match.start(0)
        group_spans = []
        for idx in range(1, match.re.groups + 1):
            if match.group(idx):
                group_spans.append(match.span(idx))
        group_spans.sort()
        result_parts = []
        last_end_in_match = 0
        for start, end in group_spans:
            start_in_match = start - match_start_pos
            end_in_match = end - match_start_pos
            result_parts.append(full_match_text[last_end_in_match:start_in_match])
            result_parts.append(self.substitute)
            last_end_in_match = end_in_match
        result_parts.append(full_match_text[last_end_in_match:])
        return "".join(result_parts)


def set_logger(
    level: str | None = None,
    format: str | None = None,
    datefmt: str | None = None,
    redacted_patterns: Sequence | None = None,
    redacted_substitute: str | None = None,
) -> None:
    default_logging_config = {
        "version": 1,
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "default",
                "filters": ["redacted"],
            },
        },
        "formatters": {
            "default": {
                "format": format
                or "%(asctime)s,%(msecs)03d %(levelname)-8s %(message)s ... %(filename)s:%(lineno)d",
                "datefmt": datefmt or "%Y-%m-%d %H:%M:%S",
            },
        },
        "loggers": {
            (__package__ or __name__).split(".")[0]: {
                "level": getattr(logging, (level or "info").upper(), logging.INFO),
                "handlers": ["console"],
                "propagate": False,
            },
        },
        "filters": {
            "redacted": {
                "()": f"{RedactingFilter.__module__}.{RedactingFilter.__name__}",
                "patterns": (
                    redacted_patterns
                    if redacted_patterns is not None
                    else [
                        r"apikey=([^&\s'\"]+)",
                        r"['\"]apikey['\"]\s*:\s*['\"]([^\"']+)['\"]",
                        r"['\"]X-Plex-Token['\"]\s*:\s*['\"]([^\"']+)['\"]",
                        r"X-Plex-Token=([^&\s'\"]+)",
                        r"webhooks/([^/]+)/([^/]+):\s{",
                    ]
                ),
                "substitute": redacted_substitute or "<REDACTED>",
            },
        },
    }
    try:
        logging.config.dictConfig(default_logging_config)
    except Exception as e:
        logger.warning(f"로깅 설정 실패: {e}", exc_info=True)
        logging.basicConfig(
            level=level or logging.DEBUG,
            format=format
            or "%(asctime)s,%(msecs)03d|%(levelname)8s| %(message)s <%(filename)s:%(lineno)d#%(funcName)s>",
            datefmt=datefmt or "%Y-%m-%dT%H:%M:%S",
        )
