import sys
from pathlib import Path

from gd_poller import cli


def main(*args):
    cli.main(*args)


if __name__ == "__main__":
    print(f"설치:")
    print(f'  pip install -e "{Path(__file__).parent}"')
    print(f"실행:")
    print(f"  gd-poller -h")
    print(f"  gd-poller")
    print(f"  gd-poller /path/to/settings.yaml")
    print(f"  {sys.executable} -m gd_poller.cli /path/to/config.yaml")
    main(*sys.argv)
