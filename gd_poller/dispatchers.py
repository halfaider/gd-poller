import shlex
import logging
import asyncio
import subprocess
from abc import ABC, abstractmethod
from typing import Any, Sequence
from pathlib import Path
from collections import OrderedDict

from .apis import Rclone, Plex, Kavita, Discord, Flaskfarm, FlaskfarmaiderBot, Jellyfin, Stash
from .helpers.helpers import parse_mappings, map_path, watch_process
from .models import ActivityData


logger = logging.getLogger(__name__)


class Dispatcher(ABC):

    def __init__(self, *, mappings: list = None, buffer_interval: int = 30) -> None:
        self.stop_event = asyncio.Event()
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

    def __init__(self,  **kwds: Any) -> None:
        super().__init__(**kwds)
        self.folder_buffer: OrderedDict[str, list[ActivityData]] = OrderedDict()

    async def dispatch(self, data: ActivityData) -> None:
        data_list = [data]
        # removed_path와 move를 분리
        if data.removed_path:
            data_delete = data.model_copy()
            data_delete.action = "delete"
            data_delete.removed_path = ""
            data_list.append(data_delete)
            data.removed_path = ""
        for data_ in data_list:
            target = Path(data_.path)
            key = str(target.parent)
            self.folder_buffer.setdefault(key, list())
            self.folder_buffer[key].append(data_)

    @abstractmethod
    async def buffered_dispatch(self, item: tuple[str, list[ActivityData]]) -> None:
        pass

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
                    await self.buffered_dispatch(self.folder_buffer.popitem(last=False))
                except Exception as e:
                    logger.exception(e)
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

    async def buffered_dispatch(self, item: tuple[str, list[ActivityData]]) -> None:
        parent, activities = item
        logger.debug(f"Kavita: {parent}")
        is_folders, paths = zip(
            *((act.is_folder, act.path) for act in activities), strict=True
        )
        if False in is_folders:
            paths = (parent,)
        for path in paths:
            kavita_path = self.get_mapping_path(path)
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

    ALLOWED_ACTIONS = ("create", "move", "rename", "restore", "delete")
    INFO_EXTENSIONS = (".json", ".yaml", ".yml", ".nfo")

    async def buffered_dispatch(self, item: tuple[str, list[ActivityData]]) -> None:
        parent, activities = item
        logger.debug(f"Broadcast: {parent}")
        # 한번에 처리되기 때문에 파일의 상태는 마지막 activity로 결정
        acts_by_path = {act.path: act for act in activities}
        deletes = {"file": [], "folder": []}
        files = []
        folders = []
        info_files = []
        for path in acts_by_path:
            act: ActivityData = acts_by_path[path]
            if act.action not in self.ALLOWED_ACTIONS:
                logger.warning(f"No applicable action: {act.action} in '{parent}'")
                continue
            if act.action == "create" and act.is_folder:
                logger.warning(
                    f"Skipped: name='{act.target[0]}' reason='Folder created'"
                )
                continue
            target = Path(path)
            suffix = target.suffix.lower()
            mode = "ADD"
            if act.action == "delete":
                if suffix in self.INFO_EXTENSIONS:
                    logger.debug(
                        f"Ignore deletion of an info file: {act.action} - {target.name} in '{parent}'"
                    )
                else:
                    # 폴더를 delete/move할 경우 자식 파일/폴더의 activity가 발생되지 않을 수 있음
                    deletes["folder" if act.is_folder else "file"].append(act)
                continue
            elif not act.is_folder and suffix in self.INFO_EXTENSIONS:
                bucket = info_files
                mode = "REFRESH"
            elif act.is_folder:
                bucket = folders
            else:
                bucket = files
            bucket.append((mode, act))
        targets: list[tuple[str, str]] = []
        # deletes, files, folders, info_files 순서로 처리
        length = len(deletes["file"]) + len(deletes["folder"])
        if deletes["file"] and length > 1:
            targets.append((parent, "REMOVE_FOLDER"))
            for act_list in deletes.values():
                for act in act_list:
                    logger.debug(
                        f"Skipped: action='{act.action}' name='{act.target[0]}' reason='Multiple items'"
                    )
        else:
            for act_list in deletes.values():
                for act in act_list:
                    targets.append(
                        (act.path, "REMOVE_FOLDER" if act.is_folder else "REMOVE_FILE")
                    )
        for idx, target in enumerate(files + folders + info_files):
            mode, act = target
            if idx > 0:
                logger.debug(f'Skipped: {act.target[0]} reason="Multiple items"')
                continue
            targets.append((act.path, mode))
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


class MultiServerDispatcher(BufferedDispatcher):

    def __init__(
        self,
        rclones: Sequence = (),
        plexes: Sequence = (),
        jellyfins: Sequence = (),
        kavitas: Sequence = (),
        stashes: Sequence = (),
        **kwds: Any,
    ) -> None:
        super().__init__(**kwds)
        self.rclones = tuple(RcloneDispatcher(**rclone) for rclone in rclones)
        self.plexes = tuple(PlexDispatcher(**plex) for plex in plexes)
        self.jellyfins = tuple(JellyfinDispatcher(**jellyfin) for jellyfin in jellyfins)
        self.kavitas = tuple(KavitaDispatcher(**kavita) for kavita in kavitas)
        self.stashes = tuple(StashDispatcher(**stash) for stash in stashes)

    async def buffered_dispatch(self, item: tuple[str, list[ActivityData]]) -> None:
        parent, activities = item
        logger.debug(f"MultiServer: {parent}")
        acts_by_path = {act.path: act for act in activities}
        act_list = list(acts_by_path.values())
        acts_by_type = {"file": [], "folder": []}
        deletes = {"file": [], "folder": []}
        parent_activity = ActivityData(path=parent, is_folder=True)
        for path in acts_by_path:
            act: ActivityData = acts_by_path[path]
            acts_by_type["folder" if act.is_folder else "file"].append(act)
            if not act.action == "delete":
                continue
            deletes["folder" if act.is_folder else "file"].append(act)
        deletes_length = len(deletes["file"]) + len(deletes["folder"])
        if deletes["file"] and deletes_length > 1:
            deleted_targets = (
                ActivityData(path=parent, is_folder=True, action="delete"),
            )
        else:
            deleted_targets = tuple(
                act for act_list in deletes.values() for act in act_list
            )

        rclone_tasks = []
        for dispatcher in self.rclones:
            for act in deleted_targets:
                rclone_tasks.append(dispatcher.dispatch(act))
            rclone_tasks.append(dispatcher.dispatch(parent_activity))
        await asyncio.gather(*rclone_tasks)

        plex_tasks = []
        if self.plexes:
            if acts_by_type["file"]:
                plex_targets = (parent_activity,)
            else:
                plex_targets = act_list
            for dispatcher in self.plexes:
                for act in plex_targets:
                    plex_tasks.append(dispatcher.dispatch(act))

        jellyfin_tasks = tuple(
            dispatcher.buffered_dispatch((parent, act_list))
            for dispatcher in self.jellyfins
        )
        kavita_tasks = tuple(
            dispatcher.buffered_dispatch((parent, act_list))
            for dispatcher in self.kavitas
        )
        stash_tasks = tuple(
            dispatcher.buffered_dispatch((parent, act_list))
            for dispatcher in self.stashes
        )

        await asyncio.gather(*plex_tasks, *jellyfin_tasks, *kavita_tasks, *stash_tasks)


class MultiPlexRcloneDispatcher(MultiServerDispatcher):
    """DEPRECATED"""

    def __init__(self, *args, **kwds) -> None:
        super().__init__(*args, **kwds)
        logger.warning("DEPRECATED: Use MultiServerDispatcher instead.")


class PlexRcloneDispatcher(MultiServerDispatcher):
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
        logger.warning("DEPRECATED: Use MultiServerDispatcher instead.")


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

        if self.wait_for_process:
            process = await asyncio.create_subprocess_exec(*cmd_parts)
            try:
                await asyncio.wait_for(process.wait(), timeout=self.timeout)
            except Exception:
                logger.exception(data.path)
            finally:
                process.kill()
        else:
            process = subprocess.Popen(cmd_parts)
            task = asyncio.create_task(
                watch_process(process, self.stop_event, timeout=self.timeout)
            )
            task.set_name(data.path)
            self.process_watchers.add(task)
            task.add_done_callback(self.process_watchers.discard)


class JellyfinDispatcher(BufferedDispatcher):

    def __init__(self, url: str, apikey: str, **kwds: Any) -> None:
        super().__init__(**kwds)
        self.jellyfin = Jellyfin(url, apikey)

    async def buffered_dispatch(self, item: tuple[str, list[ActivityData]]) -> None:
        parent, activities = item
        logger.debug(f"Jellyfin: {parent}")
        acts_by_path = {act.path: act for act in activities}

        updates = []
        for act in acts_by_path.values():
            if act.is_folder:
                logger.warning(f"Skipped: name='{act.target[0]}' reason='Folder'")
                continue
            """
            Jellyfin : action

            Created : create, move
            Modified : edit
            Deleted : delete
            """
            match act.action:
                case "delete":
                    update_type = "Deleted"
                case "create" | "move":
                    update_type = "Created"
                case "edit":
                    update_type = "Modified"
                case _:
                    logger.warning(f"This action is not supported: {act.action}")
                    continue
            updates.append(
                {
                    "Path": self.get_mapping_path(act.path),
                    "UpdateType": update_type,
                }
            )
        if updates:
            result = self.jellyfin.api_library_media_updated(updates=updates)
            status_code = result.get("status_code")
            logger.info(f"Jellyfin: updates={updates} {status_code=}")
        else:
            logger.info("No updates to send...")


class StashDispatcher(BufferedDispatcher):

    def __init__(self, url: str, apikey: str, **kwds: Any) -> None:
        super().__init__(**kwds)
        self.stash = Stash(url, apikey)

    async def buffered_dispatch(self, item: tuple[str, list[ActivityData]]) -> None:
        parent, activities = item
        logger.debug(f"Stash: {parent}")
        updates = []
        deletes = []
        acts_by_path = {act.path: act for act in activities}
        for act in acts_by_path.values():
            if act.is_folder:
                logger.warning(f"Skipped: name='{act.target[0]}' reason='Folder'")
                continue
            if act.action == "delete":
                deletes.append(self.get_mapping_path(act.path))
            else:
                updates.append(self.get_mapping_path(act.path))
        if deletes:
            result = self.stash.metadata_clean(paths=(self.get_mapping_path(parent),), dry_run=False)
            status_code = result.get("status_code")
            logger.info(f"Stash: deleted_parent={parent} {status_code=}")
        if updates:
            result = self.stash.metadata_scan(paths=(self.get_mapping_path(parent),))
            status_code = result.get("status_code")
            logger.info(f"Stash: updated_parent='{parent}' {status_code=}")
