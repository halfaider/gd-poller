import sys

from gd_poller import cli


def main(*args):
    cli.main(*args)


if __name__ == "__main__":
    print(f"설치:")
    print(f"  python -m pip install --upgrade pip setuptools wheel")
    print(
        f'  pip install --src . -e "git+https://github.com/halfaider/gd-poller.git#egg=gd_poller"'
    )
    print(f"실행:")
    print(f"  gd-poller -h")
    print(f"  gd-poller")
    print(f"  gd-poller /path/to/settings.yaml")
    print(f"  {sys.executable} -m gd_poller.cli /path/to/config.yaml")
    main(*sys.argv)
