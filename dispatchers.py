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


class DummyDispatcher(Dispatcher):

    async def dispatch(self, data: dict) -> None:
        '''override'''
        logger.info(f'DummyDispatcher: {data}')


class KavitaDispatcher(Dispatcher):

    def __init__(self, url: str = None, apikey: str = None, mappings: list = None) -> None:
        super(KavitaDispatcher, self).__init__(mappings=mappings)
        self.kavita = Kavita(url, apikey)

    async def dispatch(self, data: dict) -> None:
        '''override'''
        parents = set()
        target_path = pathlib.Path(self.get_mapping_path(data['path']))
        target_path = str(target_path) if data.get('is_folder') else str(target_path.parent)
        parents.add(target_path)
        if data.get('removed_path'):
            removed_path = pathlib.Path(self.get_mapping_path(data['removed_path']))
            removed_path = str(removed_path) if data.get('is_folder') else str(removed_path.parent)
            parents.add(removed_path)
        for p_ in parents:
            result = self.kavita.api_library_scan_folder(p_)
            logger.info(f'Kavita: scan_target="{p_}" status_code={result.get("status_code", 0)}')
            if result.get('status_code', 0) == 401:
                self.kavita.set_token()
                result = self.kavita.api_library_scan_folder(p_)
                logger.info(f'Kavita: scan_target="{p_}" status_code={result.get("status_code", 0)}')


class FlaskfarmDispatcher(Dispatcher):

    def __init__(self, url: str = None, apikey: str = None, mappings: list = None) -> None:
        super(FlaskfarmDispatcher, self).__init__(mappings=mappings)
        self.flaskfarm = Flaskfarm(url, apikey)


class GDSToolDispatcher(FlaskfarmDispatcher):

    async def dispatch(self, data: dict) -> None:
        '''override'''
        match (data.get('action'), data.get('is_folder')):
            case 'create' | 'move' | 'move' | 'rename', _:
                scan_mode = 'ADD'
            case 'delete', True:
                scan_mode = 'REMOVE_FOLDER'
            case 'delete', False:
                scan_mode = 'REMOVE_FILE'
            case 'edit', _:
                scan_mode = 'REFRESH'
            case _, _:
                scan_mode = None
        if not scan_mode:
            logger.warning(f'No applicable action: {data["action"]}')
            return
        gds_path = self.get_mapping_path(data['path'])
        logger.info(f'gds_tool: mode={scan_mode} target="{gds_path}"')
        self.flaskfarm.api_gds_tool_fp_broadcast(gds_path, scan_mode)
        if data.get('removed_path'):
            removed_scan_mode = 'REMOVE_FOLDER' if data.get('is_folder') else 'REMOVE_FILE'
            removed_gds_path = self.get_mapping_path(data['removed_path'])
            self.flaskfarm.api_gds_tool_fp_broadcast(removed_gds_path, removed_scan_mode)


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
            self.rclone.api_vfs_forget(data['path'], data['is_folder'])
            return
        remote_path = self.get_mapping_path(data['path'])
        self.rclone.refresh(remote_path)
        if data.get('removed_path'):
            self.rclone.api_vfs_forget(data['removed_path'], data['is_folder'])


class PlexDispatcher(Dispatcher):

    def __init__(self, url: str = None, token: str = None, mappings: list = None) -> None:
        super(PlexDispatcher, self).__init__(mappings=mappings)
        self.plex = Plex(url, token)

    async def dispatch(self, data: dict) -> None:
        '''override'''
        parents = set()
        target_path = pathlib.Path(self.get_mapping_path(data['path']))
        target_path = str(target_path) if data['is_folder'] else str(target_path.parent)
        parents.add(target_path)
        if data.get('removed_path'):
            removed_path = pathlib.Path(self.get_mapping_path(data['removed_path']))
            removed_path = str(removed_path) if data['is_folder'] else str(removed_path.parent)
            parents.add(removed_path)
        for p_ in parents:
            self.plex.scan(p_, is_directory=True)


class BufferedDispatcher(Dispatcher):

    def __init__(self, interval: int = 30) -> None:
        super(BufferedDispatcher, self).__init__()
        self.interval = interval
        self.folder_buffer = FolderBuffer()

    async def dispatch(self, data: dict) -> None:
        '''override'''
        self.folder_buffer.put(data['path'], data['action'], data['is_folder'])
        if data.get('removed_path'):
            self.folder_buffer.put(data['removed_path'], 'delete', data['is_folder'])

    def buffered_dispatch(self, item: tuple[str, dict]) -> None:
        logger.debug(item)

    async def on_start(self) -> None:
        '''override'''
        while not self.stop_event.is_set():
            while len(self.folder_buffer) > 0:
                item: tuple[str, dict] = self.folder_buffer.pop()
                try:
                    self.buffered_dispatch(item)
                except:
                    logger.error(traceback.format_exc())
            for _ in range(self.interval):
                await asyncio.sleep(1)
                if self.stop_event.is_set(): break


class PlexRcloneDispatcher(BufferedDispatcher):

    def __init__(self, url: str = None, mappings: list = None, plex_url: str = None, plex_token: str = None, interval: int = 30, plex_mappings: list = None) -> None:
        super(PlexRcloneDispatcher, self).__init__(interval=interval)
        self.rclone = Rclone(url)
        self.mappings = parse_mappings(mappings) if mappings else None
        self.plex = Plex(plex_url, plex_token)
        self.plex_mappings = parse_mappings(plex_mappings) if plex_mappings else None

    def buffered_dispatch(self, item: tuple[str, dict]) -> None:
        '''override'''
        logger.debug(item)
        action, _, parent = item[0].partition('|')
        match action:
            case 'delete':
                result = self.rclone.api_vfs_forget(parent, True).get('json', {})
                logger.info(f'Rclone: {result}')
            case _:
                remote_path = self.get_mapping_path(parent)
                self.rclone.refresh(remote_path)
        plex_path = map_path(parent, self.plex_mappings) if self.plex_mappings else parent
        self.plex.scan(plex_path)


class MultiPlexRcloneDispatcher(BufferedDispatcher):

    def __init__(self, interval: int = 30, rclones: list = [], plexes: list = []) -> None:
        super(MultiPlexRcloneDispatcher, self).__init__(interval=interval)
        self.rclones = [RcloneDispatcher(**rclone) for rclone in rclones]
        self.plexes = [PlexDispatcher(**plex) for plex in plexes]

    def buffered_dispatch(self, item: tuple[str, dict]) -> None:
        '''override'''
        logger.debug(item)
        action, _, parent = item[0].partition('|')
        for rclone in self.rclones:
            match action:
                case 'delete':
                    result = rclone.rclone.api_vfs_forget(parent, True).get('json', {})
                    logger.info(f'Rclone: {result}')
                case _:
                    remote_path = rclone.get_mapping_path(parent)
                    rclone.rclone.refresh(remote_path)
        for plex in self.plexes:
            plex_path = plex.get_mapping_path(parent)
            plex.plex.scan(plex_path)


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
