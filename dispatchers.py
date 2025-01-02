import logging
import pathlib
import threading
import asyncio

from apis import Rclone, Plex, Kavita, Discord, Flaskfarm
from helpers import FolderBuffer, parse_mappings, map_path

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

    def dispatch(self, data: dict) -> None:
        raise Exception('이 메소드를 구현하세요.')

    def get_mapping_path(self, target_path: str) -> str:
        return map_path(target_path, self.mappings) if self.mappings else target_path


class DummyDispatcher(Dispatcher):

    def dispatch(self, data: dict) -> None:
        '''override'''
        logger.info(f'DummyDispatcher: {data}')


class KavitaDispatcher(Dispatcher):

    def __init__(self, url: str = None, apikey: str = None, mappings: list = None) -> None:
        super(KavitaDispatcher, self).__init__(mappings=mappings)
        self.kavita = Kavita(url, apikey)

    def dispatch(self, data: dict) -> None:
        '''override'''
        kavita_path = self.get_mapping_path(data['path'])
        if not data.get('is_folder'):
            kavita_path = pathlib.Path(kavita_path).parent.as_posix()
        logger.debug(f'Kavita: scan_target="{kavita_path}"')
        result = self.kavita.api_library_scan_folder(kavita_path)


class FlaskfarmDispatcher(Dispatcher):

    def __init__(self, url: str = None, apikey: str = None, mappings: list = None) -> None:
        super(FlaskfarmDispatcher, self).__init__(mappings=mappings)
        self.flaskfarm = Flaskfarm(url, apikey)


class GDSToolDispatcher(FlaskfarmDispatcher):

    def dispatch(self, data: dict) -> None:
        '''override'''
        match (data.get('action'), data.get('is_folder')):
            case 'create' | 'move', _:
                scan_mode = 'ADD'
            case 'delete', True:
                scan_mode = 'REMOVE_FOLDER'
            case 'delete', False:
                scan_mode = 'REMOVE_FILE'
            case 'edit', _:
                scan_mode = 'REFRESH'
        gds_path = self.get_mapping_path(data['path'])
        self.flaskfarm.api_gds_tool_fp_broadcast(gds_path, scan_mode)


class PlexmateDispatcher(FlaskfarmDispatcher):

    def dispatch(self, data: dict) -> None:
        '''override'''
        target_path = self.get_mapping_path(data['path'])
        if data['action'] == 'delete':
            mode = 'REMOVE_FOLDER' if data['is_folder'] else 'REMOVE_FILE'
        else:
            mode = 'ADD'
        logger.info(f'Plexmate: {self.flaskfarm.api_plex_mate_scan_do_scan(target_path, mode=mode)}')
        if data.get('removed_path'):
            mode = 'REMOVE_FOLDER' if data['is_folder'] else 'REMOVE_FILE'
            removed_path = self.get_mapping_path(data['removed_path'])
            logger.info(f"plex_mate: {self.flaskfarm.api_plex_mate_scan_do_scan(removed_path, mode=mode)}")


class DiscordDispatcher(Dispatcher):

    def __init__(
            self,
            url: str = 'https://discord.com/api',
            webhook_id: str = None,
            webhook_token: str = None,
            colors: dict = None,
            mappings: list = None
        ) -> None:
        super(DiscordDispatcher, self).__init__(mappings=mappings)
        self.colors = colors or {'default': '0', 'move': '3447003', 'create': '5763719', 'delete': '15548997', 'edit': '16776960'}
        self.discord = Discord(url, webhook_id, webhook_token)

    def dispatch(self, data: dict) -> None:
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
        elif data.get('action_detail'):
            embed['fields'].append({'name': 'Details', 'value': data["action_detail"]})
        embed['fields'].append({'name': 'ID', 'value': data['target'][1]})
        embed['fields'].append({'name': 'MIME', 'value': data['target'][2]})
        embed['fields'].append({'name': 'Link', 'value': data['url']})
        embed['fields'].append({'name': 'Occurred at', 'value': data['timestamp']})
        result = self.discord.api_webhook(embeds=[embed])
        if result.get('status_code', 0) == 204:
            result.pop('exception', None)
        logger.debug(f"Discord: target=\"{data['target'][0]}\" result={result}")


class RcloneDispatcher(Dispatcher):

    def __init__(self, url: str = None, mappings: list = None) -> None:
        super(RcloneDispatcher, self).__init__(mappings=mappings)
        self.rclone = Rclone(url)

    def dispatch(self, data: dict) -> None:
        '''override'''
        if data.get('action', '') == 'delete':
            self.rclone.api_vfs_forget(data['path'], data['is_folder'])
            return
        remote_path = self.get_mapping_path(data['path'])
        self.rclone.refresh(remote_path, is_directory=data['is_folder'])
        if data.get('removed_path'):
            self.rclone.api_vfs_forget(data['removed_path'], data['is_folder'])


class PlexDispatcher(Dispatcher):

    def __init__(self, url: str = None, token: str = None, mappings: list = None) -> None:
        super(PlexDispatcher, self).__init__(mappings=mappings)
        self.plex = Plex(url, token)

    def dispatch(self, data: dict) -> None:
        '''override'''
        plex_path = self.get_mapping_path(data['path'])
        self.plex.scan(plex_path, is_directory=data['is_folder'])
        if data.get('removed_path'):
            plex_path = self.get_mapping_path(data['removed_path'])
            self.plex.scan(plex_path, is_directory=data.get('is_folder'))


class RclonePlexDispatcher(RcloneDispatcher):

    def __init__(self, url: str = None, mappings: list = None, plex_url: str = None, plex_token: str = None, interval: int = 30, plex_mappings: list = None) -> None:
        super(RclonePlexDispatcher, self).__init__(url=url, mappings=mappings)
        self.plex = Plex(plex_url, plex_token)
        self.plex_mappings = parse_mappings(plex_mappings) if plex_mappings else None
        self.interval = interval
        self.folder_buffer = FolderBuffer()

    def dispatch(self, data: dict) -> None:
        '''override'''
        self.folder_buffer.put(data['path'], data['action'], data['is_folder'])
        if data.get('removed_path'):
            self.folder_buffer.put(data['removed_path'], 'delete', data['is_folder'])

    async def on_start(self) -> None:
        '''override'''
        logger.debug(f'RclonePlexDispatcher starts...')
        while not self.stop_event.is_set():
            while len(self.folder_buffer) > 0:
                item: tuple[str, dict] = self.folder_buffer.pop()
                logger.debug(item)
                action, _, parent = item[0].partition('|')
                match action:
                    case 'delete':
                        result = self.rclone.api_vfs_forget(parent, True)
                        logger.debug(f'Rclone: {result}')
                    case _:
                        remote_path = self.get_mapping_path(parent)
                        self.rclone.refresh(remote_path)
                plex_path = map_path(parent, self.plex_mappings) if self.plex_mappings else parent
                self.plex.scan(plex_path)
            for _ in range(self.interval):
                await asyncio.sleep(1)
                if self.stop_event.is_set(): break
        logger.debug(f'RclonePlexDispatcher ends...')
