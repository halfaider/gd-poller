import shlex
import logging
import asyncio
import traceback
import threading
import subprocess
from abc import ABC, abstractmethod
from typing import Any
from pathlib import Path

from .apis import Rclone, Plex, Kavita, Discord, Flaskfarm, FlaskfarmaiderBot
from .helpers import FolderBuffer, parse_mappings, map_path, watch_process
from .models import ActivityData


logger = logging.getLogger(__name__)


class Dispatcher(ABC):

    def __init__(self, *, mappings: list = None, buffer_interval: int = 30) -> None:
        self.stop_event = threading.Event()
        self.mappings = parse_mappings(mappings) if mappings else None
        self.buffer_interval = buffer_interval

    async def start(self) -> None:
        if self.stop_event.is_set():
            self.stop_event.clear()
        await self.on_start()

    async def on_start(self) -> None:
        pass

    async def stop(self) -> None:
        if not self.stop_event.is_set():
            self.stop_event.set()
        await self.on_stop()

    async def on_stop(self) -> None:
        pass

    @abstractmethod
    async def dispatch(self, data: ActivityData) -> None:
        pass

    def get_mapping_path(self, target_path: str) -> str:
        return map_path(target_path, self.mappings) if self.mappings else target_path


class BufferedDispatcher(Dispatcher):

    def __init__(self, **kwds: Any) -> None:
        super().__init__(**kwds)
        self.folder_buffer = FolderBuffer()

    async def dispatch(self, data: ActivityData) -> None:
        if removed_path := data.removed_path:
            self.folder_buffer.put(removed_path, "delete", data.is_folder)
        self.folder_buffer.put(data.path, data.action, data.is_folder)

    async def buffered_dispatch(self, item: tuple[str, dict]) -> None:
        """
        ```
        item = (
            "/parent/path",
            {
                "action": {
                    ("file", "name"),
                    ("folder", "name"),
                }
            }
        )
        ```
        """
        logger.debug(item)

    async def on_start(self) -> None:
        while not self.stop_event.is_set():
            """
            처리중에 데이터가 계속 put되면 interval 의미가 없어짐
            현재 버퍼 크기를 기준으로 데이터 처리량을 제한
            """
            for _ in range(len(self.folder_buffer)):
                if self.stop_event.is_set():
                    break
                try:
                    await self.buffered_dispatch(self.folder_buffer.pop())
                except:
                    logger.error(traceback.format_exc())
            for _ in range(self.buffer_interval):
                if self.stop_event.is_set():
                    break
                await asyncio.sleep(1)


class DummyDispatcher(Dispatcher):

    async def dispatch(self, data: ActivityData) -> None:
        logger.info(f"DummyDispatcher: {data}")


class KavitaDispatcher(BufferedDispatcher):

    def __init__(self, url: str = None, apikey: str = None, **kwds: Any) -> None:
        super().__init__(**kwds)
        self.kavita = Kavita(url, apikey)

    async def buffered_dispatch(self, item: tuple[str, dict]) -> None:
        logger.debug(f"Kavita buffer: {item}")
        parent = Path(item[0])
        types, names = zip(
            *(each for values in item[1].values() for each in values), strict=True
        )
        if "file" in types:
            folders = (str(parent),)
        else:
            folders = (str(parent / name) for name in names)
        for target in folders:
            kavita_path = self.get_mapping_path(target)
            for _ in range(5):
                if (status := await self.scan_folder(kavita_path)) == 401:
                    self.kavita.set_token()
                else:
                    if not 300 > status > 199:
                        logger.warning(
                            f'Kavita: Returned status {status} for "{kavita_path}"'
                        )
                    break
            else:
                logger.error(f"Kavita: Failed to login after 5 times...")
                break

    async def scan_folder(self, path: str) -> int:
        result = self.kavita.api_library_scan_folder(path)
        status_code = result.get("status_code")
        logger.info(f'Kavita: scan_target="{path}" status_code={status_code}')
        return status_code


class FlaskfarmDispatcher(Dispatcher):

    def __init__(self, url: str, apikey: str, **kwds: Any) -> None:
        super().__init__(**kwds)
        self.flaskfarm = Flaskfarm(url, apikey)


class GDSBroadcastDispatcher(BufferedDispatcher):

    ALLOWED_ACTIONS = ("create", "move", "rename", "restore")
    INFO_EXTENSIONS = (".json", ".yaml", ".yml")

    async def buffered_dispatch(self, item: tuple[str, dict]) -> None:
        logger.debug(f"GDSTool buffer: {item}")
        parent = Path(item[0])
        targets: list[tuple[str, str]] = []
        # REMOVE 처리
        if deletes := item[1].pop("delete", None):
            types, names = zip(*deletes, strict=True)
            if "file" in types and len(types) > 1:
                targets.append((str(parent), "REMOVE_FOLDER"))
                for name in names:
                    logger.debug(
                        f'Skipped: {str(parent / name)} reason="Multiple items"'
                    )
            else:
                for type_, name in deletes:
                    targets.append(
                        (
                            str(parent / name),
                            "REMOVE_FILE" if type_ == "file" else "REMOVE_FOLDER",
                        )
                    )
        # ADD 처리
        for action in item[1]:
            if action not in self.ALLOWED_ACTIONS:
                logger.warning(f'No applicable action: {action} in "{str(parent)}"')
                continue
            # files, folders, info_files 순서로 처리
            files = []
            folders = []
            info_files = []
            for type_, name in item[1][action]:
                mode = "ADD"
                target = Path(parent / name)
                if type_ == "file" and target.suffix.lower() in self.INFO_EXTENSIONS:
                    bucket = info_files
                    mode = "REFRESH"
                elif type_ == "folder":
                    bucket = folders
                else:
                    bucket = files
                bucket.append((str(target), mode))
            for idx, target in enumerate(files + folders + info_files):
                if idx > 0:
                    logger.debug(f'Skipped: {target[0]} reason="Multiple items"')
                    continue
                targets.append(target)
        for target in targets:
            await self.broadcast(self.get_mapping_path(target[0]), target[1])

    @abstractmethod
    async def broadcast(self, path: str, mode: str) -> None:
        pass


class GDSToolDispatcher(FlaskfarmDispatcher, GDSBroadcastDispatcher):

    def __init__(self, url: str, apikey: str, **kwds: Any) -> None:
        super().__init__(url, apikey, **kwds)

    async def broadcast(self, path: str, mode: str) -> None:
        self.flaskfarm.gds_tool_fp_broadcast(path, mode)


class FlaskfarmaiderDispatcher(GDSBroadcastDispatcher):

    def __init__(self, url: str, apikey: str, **kwds: Any) -> None:
        super().__init__(**kwds)
        self.bot = FlaskfarmaiderBot(url, apikey)

    async def broadcast(self, path: str, mode: str) -> None:
        self.bot.api_broadcast(path, mode)


class PlexmateDispatcher(FlaskfarmDispatcher):

    async def dispatch(self, data: ActivityData) -> None:
        scan_targets = []
        target_path = self.get_mapping_path(data.path)
        if Path(target_path).suffix.lower() in (".json", ".yaml", ".yml"):
            mode = "REFRESH"
        else:
            if data.action == "delete":
                mode = "REMOVE_FOLDER" if data.is_folder else "REMOVE_FILE"
            else:
                mode = "ADD"
        scan_targets.append((target_path, mode))
        if removed := data.removed_path:
            mode = "REMOVE_FOLDER" if data.is_folder else "REMOVE_FILE"
            scan_targets.append((self.get_mapping_path(removed), mode))
        for st in scan_targets:
            logger.info(
                f"plex_mate: {self.flaskfarm.api_plex_mate_scan_do_scan(st[0], mode=st[1])}"
            )


class DiscordDispatcher(Dispatcher):

    COLORS = {
        "default": "0",
        "move": "3447003",
        "create": "5763719",
        "delete": "15548997",
        "edit": "16776960",
    }
    MAX_FIELD_LEN = 1024

    def __init__(
        self,
        url: str = "https://discord.com/api",
        webhook_id: str = None,
        webhook_token: str = None,
        colors: dict = None,
        **kwds: Any,
    ) -> None:
        super().__init__(**kwds)
        if colors:
            self.COLORS.update(colors)
        self.discord = Discord(url, webhook_id, webhook_token)

    async def dispatch(self, data: ActivityData) -> None:
        embed = {
            "color": self.COLORS.get(data.action, self.COLORS["default"]),
            "author": {
                "name": data.poller,
            },
            "title": data.target[0],
            "description": f"# {data.action.upper()}",
            "fields": [],
        }
        embed["fields"].append({"name": "Path", "value": self.get_truncated(data.path)})
        if data.action == "move":
            embed["fields"].append(
                {
                    "name": "From",
                    "value": self.get_truncated(
                        data.removed_path if data.removed_path else "unknown"
                    ),
                }
            )
        elif data.action_detail and type(data.action_detail) in (
            str,
            int,
        ):
            embed["fields"].append(
                {"name": "Details", "value": self.get_truncated(data.action_detail)}
            )
        embed["fields"].append(
            {"name": "ID", "value": self.get_truncated(data.target[1])}
        )
        embed["fields"].append(
            {"name": "MIME", "value": self.get_truncated(data.target[2])}
        )
        embed["fields"].append({"name": "Link", "value": self.get_truncated(data.link)})
        embed["fields"].append(
            {"name": "Occurred at", "value": self.get_truncated(data.timestamp_text)}
        )
        result = self.discord.api_webhook(embeds=[embed])
        logger.info(
            f"Discord: target=\"{data.target[0]}\" status_code={result.get('status_code')}"
        )

    def get_truncated(self, content: str) -> str:
        if len(content) > self.MAX_FIELD_LEN:
            content = content[: self.MAX_FIELD_LEN - 3] + "..."
        return content


class RcloneDispatcher(Dispatcher):

    def __init__(self, url: str = None, **kwds: Any) -> None:
        super().__init__(**kwds)
        self.rclone = Rclone(url)

    async def dispatch(self, data: ActivityData) -> None:
        remote_path = Path(self.get_mapping_path(data.path))
        if data.action == "delete":
            self.rclone.forget(str(remote_path), data.is_folder)
            return
        if removed_path := data.removed_path:
            removed_remote_path = Path(self.get_mapping_path(removed_path))
            self.rclone.forget(str(removed_remote_path), data.is_folder)
        target_path = str(remote_path) if data.is_folder else str(remote_path.parent)
        self.rclone.forget(target_path, True)
        self.rclone.refresh(target_path)


class PlexDispatcher(Dispatcher):

    def __init__(self, url: str = None, token: str = None, **kwds: Any) -> None:
        super().__init__(**kwds)
        self.plex = Plex(url, token)

    async def dispatch(self, data: ActivityData) -> None:
        targets = set()
        plex_path = Path(self.get_mapping_path(data.path))
        targets.add(str(plex_path) if data.is_folder else str(plex_path.parent))
        if removed_path := data.removed_path:
            removed_plex_path = Path(self.get_mapping_path(removed_path))
            targets.add(
                str(removed_plex_path)
                if data.is_folder
                else str(removed_plex_path.parent)
            )
        for p_ in targets:
            self.plex.scan(p_, is_directory=True)


class MultiPlexRcloneDispatcher(BufferedDispatcher):

    def __init__(self, rclones: list = [], plexes: list = [], **kwds: Any) -> None:
        super().__init__(**kwds)
        self.rclones = tuple(RcloneDispatcher(**rclone) for rclone in rclones)
        self.plexes = tuple(PlexDispatcher(**plex) for plex in plexes)

    async def buffered_dispatch(self, item: tuple[str, dict]) -> None:
        logger.debug(f"PlexRclone buffer: {item}")
        parent = Path(item[0])
        if self.rclones and (deletes := item[1].get("delete")):
            types, names = zip(*deletes, strict=True)
            if "file" in types and len(types) > 1:
                deleted_targets = (str(parent),)
            else:
                deleted_targets = (str(parent / name) for name in names)
        else:
            deleted_targets = tuple()
        for dispatcher in self.rclones:
            for target in deleted_targets:
                await dispatcher.dispatch(
                    ActivityData(action="delete", path=str(target), is_folder=True)
                )
            await dispatcher.dispatch(ActivityData(path=str(parent), is_folder=True))
        if not self.plexes:
            return
        types, names = zip(
            *(each for values in item[1].values() for each in values), strict=True
        )
        if "file" in types:
            folders = (str(parent),)
        else:
            folders = (str(parent / name) for name in names)
        for dispatcher in self.plexes:
            for target in folders:
                await dispatcher.dispatch(ActivityData(path=target, is_folder=True))


class PlexRcloneDispatcher(MultiPlexRcloneDispatcher):
    """DEPRECATED"""

    def __init__(
        self,
        url: str = None,
        mappings: list = None,
        plex_url: str = None,
        plex_token: str = None,
        plex_mappings: list = None,
        **kwds: Any,
    ) -> None:
        rclones = [{"url": url, "mappings": mappings}]
        plexes = [{"url": plex_url, "token": plex_token, "mappings": plex_mappings}]
        super().__init__(rclones=rclones, plexes=plexes, **kwds)


class CommandDispatcher(Dispatcher):

    def __init__(
        self,
        command: str,
        wait_for_process: bool = False,
        drop_during_process=False,
        timeout: int = 300,
        **kwds: Any,
    ) -> None:
        super().__init__(**kwds)
        self.command = command
        self.wait_for_process = wait_for_process
        self.drop_during_process = drop_during_process
        self.timeout = timeout
        self.process_watchers = set()

    async def dispatch(self, data: ActivityData) -> None:
        if self.drop_during_process and bool(self.process_watchers):
            logger.warning(f"Already running: {self.process_watchers}")
            return

        cmd_parts = shlex.split(self.command)
        cmd_parts.append(data.action)
        cmd_parts.append("directory" if data.is_folder else "file")
        cmd_parts.append(self.get_mapping_path(data.path))
        if removed_path := data.removed_path:
            cmd_parts.append(self.get_mapping_path(removed_path))
        logger.info(f"Command: {cmd_parts}")

        process = subprocess.Popen(cmd_parts)
        if self.wait_for_process:
            try:
                process.wait(timeout=self.timeout)
            except:
                logger.exception(data.path)
        else:
            task = asyncio.create_task(
                watch_process(process, self.stop_event, timeout=self.timeout)
            )
            task.set_name(data.path)
            self.process_watchers.add(task)
            task.add_done_callback(self.process_watchers.discard)
