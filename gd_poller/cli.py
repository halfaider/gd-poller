import sys
import argparse
from textwrap import dedent
from pathlib import Path
from typing import Any

from .main import main as app_main


def main(*args: Any) -> None:
    # ('LOAD', '/path/to/gd-poller/app.py', '/path/to/config.yaml')
    # ('app.py', '/path/to/config.yaml')
    if not args:
        args = tuple(sys.argv)
    if len(args) > 2 and args[0] == "LOAD":
        args = args[2:]
    else:
        args = args[1:]
    package_path = Path(__file__).parent
    parser = argparse.ArgumentParser(
        description="Google Drive Activity Poller",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "settings_yaml",
        metavar="settings.yaml",
        nargs="?",
        type=str,
        help=dedent(
            f"""\
            설정 파일의 경로.
            지정하지 않으면 아래 순서대로 로딩되며 나중에 읽은 파일이 이전 값을 덮어씁니다.
            - {package_path / 'config.yaml'}
            - {Path.cwd() / 'config.yaml'}
            - {package_path / 'settings.yaml'}
            - {Path.cwd() / 'settings.yaml'}
            """
        ),
        default=None,
    )
    parsed_args = parser.parse_args(sys.argv[1:])
    app_main(parsed_args.settings_yaml)


if __name__ == "__main__":
    main()
