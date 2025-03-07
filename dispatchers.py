import logging
import pathlib
import threading
import asyncio
import traceback
import subprocess
import shlex

from apis import Rclone, Plex, Kavita, Discord, Flaskfarm
from helpers import FolderBuffer, parse_mappings, map_path, watch_process

logger = logging.getLogger(__name__)


class Dispatcher:

    def __init__(self, mappings: list = None) -> None:
        self.stop_event = threading.Event()
        self.mappings = parse_mappings(mappings) if mappings else None

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

    async def dispatch(self, data: dict) -> None:
        '''
        data = {
            'ancestor': str,
            'action': str,
            'action_detail': str | tuple | list | None,
            'target': tuple[str, str, str],
            'is_folder': bool,
            'path': str,
            'removed_path': str | None,
            'link': str,
            'timestamp': str,
            'poller': str,
        }
        '''
        raise Exception('이 메소드를 구현하세요.')

    def get_mapping_path(self, target_path: str) -> str:
        return map_path(target_path, self.mappings) if self.mappings else target_path


class BufferedDispatcher(Dispatcher):

    def __init__(self, *args, interval: int = 30, **kwds) -> None:
        super(BufferedDispatcher, self).__init__(*args, **kwds)
        self.interval = interval
        self.folder_buffer = FolderBuffer()

    async def dispatch(self, data: dict) -> None:
        '''override'''
        self.folder_buffer.put(data['path'], data['action'], data['is_folder'])
        if data.get('removed_path'):
            self.folder_buffer.put(data['removed_path'], 'delete', data['is_folder'])

    async def buffered_dispatch(self, item: tuple[str, dict]) -> None:
        logger.debug(item)

    async def on_start(self) -> None:
        '''override'''
        while not self.stop_event.is_set():
            while len(self.folder_buffer) > 0:
                item: tuple[str, dict] = self.folder_buffer.pop()
                try:
                    await self.buffered_dispatch(item)
                except:
                    logger.error(traceback.format_exc())
            for _ in range(self.interval):
                await asyncio.sleep(1)
                if self.stop_event.is_set(): break


class DummyDispatcher(Dispatcher):

    async def dispatch(self, data: dict) -> None:
        '''override'''
        logger.info(f'DummyDispatcher: {data}')


class KavitaDispatcher(BufferedDispatcher):

    def __init__(self, url: str = None, apikey: str = None, mappings: list = None, interval: int = 30) -> None:
        super(KavitaDispatcher, self).__init__(interval=interval, mappings=mappings)
        self.kavita = Kavita(url, apikey)

    async def buffered_dispatch(self, item: tuple[str, dict]) -> None:
        '''override'''
        logger.debug(item)
        parent = pathlib.Path(item[0])
        has_file = False
        folders = []
        for action_value in item[1].values():
            for type_, name in action_value:
                if type_ == 'file':
                    has_file = True
                else:
                    folders.append((str(parent / name)))
        for target in [str(parent)] if has_file else folders:
            kavita_path = self.get_mapping_path(target)
            if await self.scan_folder(kavita_path) == 401:
                self.kavita.set_token()
                await self.scan_folder(kavita_path)

    async def scan_folder(self, path: str) -> int:
        '''override'''
        result = self.kavita.api_library_scan_folder(path)
        logger.info(f'Kavita: scan_target="{path}" status_code={result.get("status_code", 0)}')
        return result.get('status_code', 0)


class FlaskfarmDispatcher(Dispatcher):

    def __init__(self, url: str, apikey: str, *args, mappings: list = None, **kwds) -> None:
        super(FlaskfarmDispatcher, self).__init__(*args, mappings=mappings, **kwds)
        self.flaskfarm = Flaskfarm(url, apikey)


class GDSToolDispatcher(FlaskfarmDispatcher, BufferedDispatcher):

    ADD_ACTIONS = ('create', 'move', 'rename')
    INFO_EXTENSIONS = ('.json', '.yaml', '.yml')

    def __init__(self, url: str, apikey: str, *args, mappings: list = None, interval: int = 30, **kwds) -> None:
        super(GDSToolDispatcher, self).__init__(url, apikey, *args, mappings=mappings, interval=interval, **kwds)

    async def dispatch(self, data: dict) -> None:
        '''override'''
        removed_path = pathlib.Path(data.get('removed_path')) if data.get('removed_path') else None
        path = pathlib.Path(data['path'])
        deletes = []
        if removed_path and removed_path.suffix.lower() not in self.INFO_EXTENSIONS:
            deletes.append(data['removed_path'])
        if data.get('action', '') == 'delete' and path.suffix.lower() not in self.INFO_EXTENSIONS:
            deletes.append(str(path))
        elif data['action'] not in self.ADD_ACTIONS:
            logger.warning(f'No applicable action: {data["action"]} on "{str(path)}"')
        else:
            if data['is_folder']:
                self.flaskfarm.gds_tool_fp_broadcast(self.get_mapping_path(str(path)), 'ADD')
            else:
                self.folder_buffer.put(str(path), data['action'], data['is_folder'])
        for idx, deleted in enumerate(deletes, start=1):
            # plex_mate에서 파일 존재 여부 체크하기 때문에 각각 처리
            self.flaskfarm.gds_tool_fp_broadcast(self.get_mapping_path(deleted), 'REMOVE_FOLDER' if data['is_folder'] else 'REMOVE_FILE')
            if idx < len(deletes):
                await asyncio.sleep(1.0)

    async def buffered_dispatch(self, item: tuple[str, dict]) -> None:
        '''override'''
        logger.debug(item)
        parent = pathlib.Path(item[0])
        targets: list[tuple[str, str]] = []
        for action in item[1]:
            if action not in self.ADD_ACTIONS:
                logger.warning(f'No applicable action: {action} in "{str(parent)}')
                continue
            info_files = []
            files = []
            for _, name in item[1][action]:
                target: pathlib.Path = parent / name
                if target.suffix.lower() in self.INFO_EXTENSIONS:
                    info_files.append((str(target), 'REFRESH'))
                else:
                    files.append((str(target), 'ADD'))
            files.extend(info_files)
            for idx, target in enumerate(files, start=1):
                if idx > 1:
                    logger.debug(f'Skipped: {target[0]} reason="Multiple items"')
                    continue
                targets.append(target)
        for idx, target in enumerate(targets, start=1):
            self.flaskfarm.gds_tool_fp_broadcast(self.get_mapping_path(target[0]), target[1])
            if idx < len(item[1][action]):
                await asyncio.sleep(1.0)


class PlexmateDispatcher(FlaskfarmDispatcher):

    async def dispatch(self, data: dict) -> None:
        '''override'''
        scan_targets = []
        target_path = self.get_mapping_path(data['path'])
        tp = pathlib.Path(target_path)
        if tp.suffix.lower() in ['.json', '.yaml', '.yml']:
            mode = 'REFRESH'
        else:
            if data['action'] == 'delete':
                mode = 'REMOVE_FOLDER' if data['is_folder'] else 'REMOVE_FILE'
            else:
                mode = 'ADD'
        scan_targets.append((target_path, mode))
        if data.get('removed_path'):
            mode = 'REMOVE_FOLDER' if data['is_folder'] else 'REMOVE_FILE'
            removed_path = self.get_mapping_path(data['removed_path'])
            scan_targets.append((removed_path, mode))
        for st in scan_targets:
            logger.info(f'plex_mate: {self.flaskfarm.api_plex_mate_scan_do_scan(st[0], mode=st[1])}')


class DiscordDispatcher(Dispatcher):

    colors = {
        'default': '0',
        'move': '3447003',
        'create': '5763719',
        'delete': '15548997',
        'edit': '16776960'
    }

    def __init__(
            self,
            url: str = 'https://discord.com/api',
            webhook_id: str = None,
            webhook_token: str = None,
            colors: dict = None,
            mappings: list = None
        ) -> None:
        super(DiscordDispatcher, self).__init__(mappings=mappings)
        if colors:
            for action in colors:
                self.colors[action] = colors[action]
        self.discord = Discord(url, webhook_id, webhook_token)

    async def dispatch(self, data: dict) -> None:
        '''override'''
        embed = {
            'color': self.colors.get(data['action'], self.colors['default']),
            'author': {
                'name': data['poller'],
            },
            'title': data['target'][0],
            'description': f'# {data["action"].upper()}',
            'fields': []
        }
        embed['fields'].append({'name': 'Path', 'value': data['path']})
        if data['action'] == 'move':
            embed['fields'].append({'name': 'From', 'value': data['removed_path'] if data['removed_path'] else f'unknown'})
        elif data.get('action_detail') and type(data.get('action_detail')) in (str, int):
            embed['fields'].append({'name': 'Details', 'value': data["action_detail"]})
        embed['fields'].append({'name': 'ID', 'value': data['target'][1]})
        embed['fields'].append({'name': 'MIME', 'value': data['target'][2]})
        embed['fields'].append({'name': 'Link', 'value': data['link']})
        embed['fields'].append({'name': 'Occurred at', 'value': data['timestamp']})
        result = self.discord.api_webhook(embeds=[embed])
        logger.info(f"Discord: target=\"{data['target'][0]}\" status_code={result.get('status_code', 0)}")


class RcloneDispatcher(Dispatcher):

    def __init__(self, url: str = None, mappings: list = None) -> None:
        super(RcloneDispatcher, self).__init__(mappings=mappings)
        self.rclone = Rclone(url)

    async def dispatch(self, data: dict) -> None:
        '''override'''
        if data.get('action', '') == 'delete':
            self.rclone.forget(data['removed_path'], data['is_folder'])
            return
        remote_path = pathlib.Path(self.get_mapping_path(data['path']))
        self.rclone.refresh(str(remote_path) if data['is_folder'] else str(remote_path.parent))
        if data.get('removed_path'):
            self.rclone.forget(data['removed_path'], data['is_folder'])


class PlexDispatcher(Dispatcher):

    def __init__(self, url: str = None, token: str = None, mappings: list = None) -> None:
        super(PlexDispatcher, self).__init__(mappings=mappings)
        self.plex = Plex(url, token)

    async def dispatch(self, data: dict) -> None:
        '''override'''
        targets = set()
        plex_path = pathlib.Path(self.get_mapping_path(data['path']))
        targets.add(str(plex_path) if data['is_folder'] else str(plex_path.parent))
        if data.get('removed_path'):
            removed_plex_path = pathlib.Path(self.get_mapping_path(data['removed_path']))
            targets.add(str(removed_plex_path) if data['is_folder'] else str(removed_plex_path.parent))
        for p_ in targets:
            self.plex.scan(p_, is_directory=True)


class MultiPlexRcloneDispatcher(BufferedDispatcher):

    def __init__(self, interval: int = 30, rclones: list = [], plexes: list = []) -> None:
        super(MultiPlexRcloneDispatcher, self).__init__(interval=interval)
        self.rclones = [RcloneDispatcher(**rclone) for rclone in rclones]
        self.plexes = [PlexDispatcher(**plex) for plex in plexes]

    async def buffered_dispatch(self, item: tuple[str, dict]) -> None:
        '''override'''
        logger.debug(item)
        parent = pathlib.Path(item[0])
        deletes = item[1].pop('delete', set())
        for dispatcher in self.rclones:
            for is_folder, name in deletes:
                dispatcher.rclone.forget(str(parent / name), is_folder)
            dispatcher.rclone.refresh(dispatcher.get_mapping_path(str(parent)))
        has_file = False
        folders = []
        for action_value in item[1].values():
            for type_, name in action_value:
                if type_ == 'file':
                    has_file = True
                else:
                    folders.append((str(parent / name)))
        for dispatcher in self.plexes:
            for target in [str(parent)] if has_file else folders:
                dispatcher.plex.scan(dispatcher.get_mapping_path(target))


class PlexRcloneDispatcher(MultiPlexRcloneDispatcher):
    '''DEPRECATED'''

    def __init__(self, url: str = None, mappings: list = None, plex_url: str = None, plex_token: str = None, interval: int = 30, plex_mappings: list = None) -> None:
        rclones = [{
            'url': url,
            'mappings': mappings
        }]
        plexes = [{
            'url': plex_url,
            'token': plex_token,
            'mappings': plex_mappings
        }]
        super(PlexRcloneDispatcher, self).__init__(interval, rclones, plexes)


class CommandDispatcher(Dispatcher):

    def __init__(self, command: str, wait_for_process: bool = False, drop_during_process = False, timeout: int = 300, mappings: list = None) -> None:
        super(CommandDispatcher, self).__init__(mappings=mappings)
        self.command = command
        self.wait_for_process = wait_for_process
        self.drop_during_process = drop_during_process
        self.timeout = timeout
        self.process_watchers = set()

    async def dispatch(self, data: dict) -> None:
        '''override'''
        if self.drop_during_process and bool(self.process_watchers):
            logger.warning(f'Already running: {self.process_watchers}')
            return

        cmd_parts = shlex.split(self.command)
        cmd_parts.append(data['action'])
        cmd_parts.append('directory' if data['is_folder'] else 'file')
        cmd_parts.append(self.get_mapping_path(data['path']))
        if data.get('removed_path'):
            cmd_parts.append(self.get_mapping_path(data['removed_path']))
        logger.info(f'Command: {cmd_parts}')

        process = subprocess.Popen(cmd_parts)
        if self.wait_for_process:
            try:
                process.wait(timeout=self.timeout)
            except:
                logger.error(traceback.format_exc())
                logger.error(data['path'])
        else:
            task = asyncio.create_task(watch_process(process, self.stop_event, timeout=self.timeout))
            task.set_name(data['path'])
            self.process_watchers.add(task)
            task.add_done_callback(self.process_watchers.discard)
