import re
import pathlib
import importlib.metadata


def _get_version() -> str:
    pyproject = pathlib.Path(__file__).resolve().parent.parent / "pyproject.toml"
    if pyproject.is_file():
        try:
            content = pyproject.read_text(encoding="utf-8")
            if match := re.search(r'version\s*=\s*["\']([^"\']+)["\']', content):
                return match.group(1)
        except Exception:
            pass
    try:
        return importlib.metadata.version("gd_poller")
    except Exception:
        return "0.0.0"


__version__ = _get_version()
