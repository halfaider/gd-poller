import queue
import pathlib
import logging
import traceback
import datetime
import asyncio
import re
import time
from abc import ABC, abstractmethod
from typing import Any, Iterable

import dispatchers
from apis import GoogleDrive
from helpers import await_sync, PrioritizedItem, check_tasks

LOCAL_TIMEZONE = datetime.datetime.now(datetime.timezone(datetime.timedelta(0))).astimezone().tzinfo
logger = logging.getLogger(__name__)


class GoogleDrivePoller(ABC):

    def __init__(self, drive: GoogleDrive, targets: Iterable[str], dispatcher_list: Iterable[dispatchers.Dispatcher] = None, name: str = None,
                 polling_interval: int = 60, page_size: int = 50, actions: Iterable = None, task_check_interval: int = -1,
                 patterns: list = None, ignore_patterns: list = None, ignore_folder: bool = True, dispatch_interval: int = 1):
        self.drive = drive
        self.targets = targets
        self.dispatcher_list = dispatcher_list
        self.name = name
        self.polling_interval = polling_interval
        self.page_size = page_size
        self.actions = actions
        self.patterns = patterns
        self.ignore_patterns = ignore_patterns
        self.ignore_folder = ignore_folder
        self.dispatch_interval = dispatch_interval
        self.task_check_interval = task_check_interval

        self._stop_event = asyncio.Event()
        self._dispatch_queue = None
        self._tasks = None

    @property
    def drive(self) -> GoogleDrive:
        return self._drive

    @drive.setter
    def drive(self, drive: GoogleDrive) -> None:
        self._drive = drive

    @property
    def targets(self) -> list:
        return self._targets

    @targets.setter
    def targets(self, targets: list) -> None:
        try:
            if len(targets) < 1:
                raise Exception(f'The targets is empty: {targets=}')
            self._targets = targets
        except:
            raise Exception(f'The targets is not a list: {targets=}')

    @property
    def dispatcher_list(self) -> Iterable[dispatchers.Dispatcher]:
        return self._dispatcher_list

    @dispatcher_list.setter
    def dispatcher_list(self, dispatcher_list: Iterable[dispatchers.Dispatcher]) -> None:
        self._dispatcher_list = dispatcher_list if dispatcher_list else (dispatchers.DummyDispatcher(),)

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, name: str) -> None:
        self._name = name if name else self.__class__.__name__

    @property
    def polling_interval(self) -> int:
        return self._polling_interval

    @polling_interval.setter
    def polling_interval(self, polling_interval: int) -> None:
        self._polling_interval = int(polling_interval) if polling_interval else 60

    @property
    def page_size(self) -> int:
        return self._page_size

    @page_size.setter
    def page_size(self, page_size: int) -> None:
        self._page_size = int(page_size) if page_size else 50

    @property
    def actions(self) -> tuple:
        return self._actions

    @actions.setter
    def actions(self, actions: Iterable) -> None:
        self._actions = tuple(actions) if actions else (
            'create',
            'edit',
            'move',
            'rename',
            'delete',
            'restore',
            'permissionChange',
            'comment',
            'dlpChange',
            'reference',
            'settingsChange',
            'appliedLabelChange'
        )

    @property
    def patterns(self) -> Iterable[re.Pattern]:
        return self._patterns

    @patterns.setter
    def patterns(self, patterns: Iterable[str]) -> None:
        self._patterns = tuple(re.compile(pattern, re.I) for pattern in patterns) if patterns else (re.compile('.*', re.I),)

    @property
    def ignore_patterns(self) -> list:
        return self._ignore_patterns

    @ignore_patterns.setter
    def ignore_patterns(self, ignore_patterns: list) -> None:
        self._ignore_patterns = tuple(re.compile(pattern, re.I) for pattern in ignore_patterns) if ignore_patterns else ()

    @property
    def dispatch_interval(self) -> int:
        return self._dispatch_interval

    @dispatch_interval.setter
    def dispatch_interval(self, dispatch_interval: int) -> None:
        try:
            self._dispatch_interval = int(dispatch_interval)
        except:
            logger.error(traceback.format_exc())
            self._dispatch_interval = 1

    @property
    def ignore_folder(self) -> bool:
        return self._ignore_folder

    @ignore_folder.setter
    def ignore_folder(self, ignore_folder: bool) -> bool:
        self._ignore_folder = ignore_folder if type(ignore_folder) is bool else True

    @property
    def stop_event(self) -> asyncio.Event:
        return self._stop_event

    @property
    def dispatch_queue(self) -> queue.PriorityQueue:
        return self._dispatch_queue

    @property
    def tasks(self) -> list:
        return self._tasks

    @tasks.setter
    def tasks(self, tasks: list) -> None:
        self._tasks = tasks if tasks is not None else []

    @property
    def task_check_interval(self) -> int:
        return self._task_check_interval

    @task_check_interval.setter
    def task_check_interval(self, task_check_interval: int) -> None:
        self._task_check_interval = int(task_check_interval)

    async def start(self) -> None:
        self._dispatch_queue = queue.PriorityQueue()
        self.tasks = []
        if self.stop_event.is_set():
            self.stop_event.clear()
        self.tasks.append(asyncio.create_task(self.dispatch(), name=f'dispatching-{self.name}'))
        for target in self.targets:
            id_, _, root = target.partition('#')
            self.tasks.append(asyncio.create_task(self.poll(target), name=f'polling-{root if root else id_}'))
        for dispatcher in self.dispatcher_list:
            self.tasks.append(asyncio.create_task(dispatcher.start(), name=f'{self.name}-{dispatcher.__class__.__name__}'))
        try:
            gathers = [*self.tasks]
            if self.task_check_interval > 0:
                check_task = asyncio.create_task(check_tasks(self.tasks, self.task_check_interval), name='check_tasks')
                gathers.append(check_task)
            await asyncio.gather(*gathers)
        except asyncio.CancelledError:
            logger.warning(f'Tasks are cancelled: {self.name}')

    async def stop(self) -> None:
        self.stop_event.set()
        for dispatcher in self.dispatcher_list:
            await dispatcher.stop()
        for task in self.tasks:
            logger.debug(task)
            if not task.done():
                task.print_stack()
                try:
                    task.cancel()
                except:
                    logger.error(traceback.format_exc())
        self._dispatch_queue = None
        self._tasks = None

    def check_patterns(self, path: str, patterns: Iterable[re.Pattern]) -> bool:
        for pattern in patterns:
            if not pattern:
                continue
            try:
                if pattern.search(path):
                    return True
            except:
                continue
        return False

    @abstractmethod
    async def poll(self, target: Any) -> None:
        pass

    @abstractmethod
    async def dispatch(self) -> None:
        pass


class ChangePoller(GoogleDrivePoller):
    pass


class ActivityPoller(GoogleDrivePoller):

    def __init__(self, *args, **kwds):
        super(ActivityPoller, self).__init__(*args, **kwds)
        # 구글 응답에 맞춰서 UTC
        self.last_activity_timestamp = datetime.datetime.now(datetime.timezone.utc)
        # Time is the problem....
        self.last_no_activity_timestamp = time.time()

    async def dispatch(self) -> None:
        logger.info(f'Dispatching task starts: {self.name}')
        while not self.stop_event.is_set():
            await self._dispatch()
            # 큐에서 각 아이템을 꺼낸 후 sleep
            for _ in range(max(self.dispatch_interval * 10, 10)):
                await asyncio.sleep(0.1)
                if self.stop_event.is_set(): break
        logger.info(f'Dispatching task ends: {self.name}')

    async def _dispatch(self) -> None:
        data = None
        try:
            '''
            data = {
                'ancestor': str,
                'timestamp': datetime.datetime,
                'action': str,
                'action_detail': str | tuple | list | None,
                'target': tuple[str, str, str],
            }
            '''
            data: dict = self.dispatch_queue.get_nowait().item
            # action 필터링
            if data['action'] not in self.actions:
                logger.debug(f'Skipped: target={data["target"]} reason={data["action"]}')
                return
            # 폴더 타입 확인
            if data['target'][2] in (
                    'application/vnd.google-apps.folder',
                    'application/vnd.google-apps.shortcut'
                ):
                data['is_folder'] = True
            else:
                data['is_folder'] = False
            # 폴더 무시 판단
            if self.ignore_folder and data['is_folder']:
                logger.debug(f'Skipped: target={data["target"]} reason=folder')
                return
            # 대상이 영구히 삭제돼서 조회 불가능 할 경우
            if data['action'] == 'delete' and data['action_detail'] != 'TRASH':
                logger.debug(f'Skipped: target={data["target"]} reason="deleted permanently"')
                return
            # 대상 경로
            target_id = data['target'][1].partition('/')[-1]
            data['path'], parent, web_view = self.drive.get_full_path(target_id, data.get('ancestor'))
            if not parent[0]:
                logger.warning(f"Could not figure out its path: id={target_id} ancestor={data.get('ancestor')} parent={parent[0]}")
                data['path'] = f"/unknown/{data['target'][0]}"
            # url 링크
            if web_view:
                data['link'] = web_view.strip()
            else:
                url_folder_id = target_id if data['is_folder'] else parent[1]
                data['link'] = f'https://drive.google.com/drive/folders/{url_folder_id}'
            # move, rename일 경우 소스 경로
            data['removed_path'] = None
            if data['action'] == 'move' and data['action_detail']:
                logger.debug(f'Moved from: {data["action_detail"]}')
                try:
                    removed_parent_id = data['action_detail'][1].partition('/')[-1]
                    removed_path, _, _ = self.drive.get_full_path(removed_parent_id, data.get('ancestor'))
                    data['removed_path'] = str(pathlib.Path(removed_path, data['target'][0]))
                except:
                    logger.error(traceback.format_exc())
            elif data['action'] == 'rename' and data['action_detail']:
                logger.debug(f'Renamed from: {data["action_detail"]}')
                data['removed_path'] = str(pathlib.Path(data['path']).with_name(data['action_detail']))
            # 기타 정보
            data['timestamp'] = data['timestamp'].astimezone(LOCAL_TIMEZONE).strftime('%Y-%m-%dT%H:%M:%S%z')
            data['poller'] = self.name
            # 패턴 체크
            if not self.check_patterns(data['path'], self.patterns):
                logger.debug(f'Skipped: target="{data["path"]}" reason="Not match with patterns"')
                data['path'] = None
            elif self.check_patterns(data['path'], self.ignore_patterns):
                logger.debug(f'Skipped: target="{data["path"]}" reason="Match with ignore patterns"')
                data['path'] = None
            # removed_path 패턴 체크
            if data['removed_path'] and not self.check_patterns(data['removed_path'], self.patterns):
                logger.debug(f'Skipped: removed_path="{data["removed_path"]}" reason="Not match with patterns"')
                data['removed_path'] = None
            elif data['removed_path'] and self.check_patterns(data['removed_path'], self.ignore_patterns):
                logger.debug(f'Skipped: removed_path="{data["removed_path"]}" reason="Match with ignore patterns"')
                data['removed_path'] = None
            # move된 경로를 접근할 수 없을 경우
            match bool(data['path']), bool(data['removed_path']):
                case False, True:
                    data['path'] = data['removed_path']
                    data['removed_path'] = None
                    data['action'] = 'delete'
                    data['link'] = f'https://drive.google.com/drive/folders/{data["action_detail"][1].partition("/")[-1]}'
                    data['action_detail'] = f'Moved but can not access: {data["target"][1]}'
                case False, False:
                    return
            for dispatcher in self.dispatcher_list:
                # activity 발생 순서대로, dispatcher 배치 순서대로
                await dispatcher.dispatch(data)
        except queue.Empty:
            pass
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error(f'{data=}')
        finally:
            if data:
                self.dispatch_queue.task_done()

    async def poll(self, ancestor: str) -> None:
        logger.info(f'Polling task starts: {ancestor}')
        activity_api = self.drive.api_activity.activity()
        while not self.stop_event.is_set():
            await self._poll(ancestor, activity_api)
            for _ in range(self.polling_interval):
                await asyncio.sleep(1)
                if self.stop_event.is_set(): break
        logger.info(f'Polling task ends: {ancestor}')

    async def _poll(self, ancestor: str, activity_api: callable) -> None:
        ancestor_id, _, root = ancestor.partition('#')
        next_page_token = None
        while not self.stop_event.is_set():
            try:
                query = activity_api.query(body={
                    'pageSize': self.page_size,
                    'ancestorName': f'items/{ancestor_id}',
                    'pageToken': next_page_token,
                    'filter': f'time > "{self.last_activity_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ")}"',
                })
                try:
                    results = await await_sync(query.execute)
                except Exception as e:
                    self.drive.handle_error(e)
                    logger.error(f'Polling failed: {ancestor=}')
                    break
                next_page_token = results.get('nextPageToken')
                activities = results.get('activities', [])
                if not activities:
                    current_timestamp = time.time()
                    if self.task_check_interval > 0 and current_timestamp - self.last_no_activity_timestamp > self.task_check_interval:
                        logger.debug(F'No activity in "{root or ancestor}" since {self.last_activity_timestamp.astimezone(LOCAL_TIMEZONE).strftime("%Y-%m-%dT%H:%M:%S.%f%z")}')
                        self.last_no_activity_timestamp = current_timestamp
                    break
                last_timestamp = self.last_activity_timestamp
                logger.debug(f'Last activity timestamp: {self.last_activity_timestamp}')
                for activity in activities:
                    data = self.get_activity(activity)
                    data['ancestor'] = ancestor
                    logger.debug(f"{data['action']}, {data['target']} at {data['timestamp']}")
                    self.dispatch_queue.put(PrioritizedItem(data['timestamp'].timestamp(), data))
                    if data['timestamp'] > self.last_activity_timestamp:
                        if data['timestamp'] > datetime.datetime.now(datetime.timezone.utc):
                            logger.warning(f'Skipped: timestamp={data["timestamp"]} reason="future"')
                        else:
                            self.last_activity_timestamp = data['timestamp']
                if last_timestamp == self.last_activity_timestamp:
                    logger.warning('Last activity timestamp is not updated.')
                    #self.last_activity_timestamp += datetime.timedelta(seconds=1)
                if not next_page_token:
                    break
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error(f'{ancestor=}')

    def get_activity(self, activity: dict) -> dict:
            #logger.debug(f'{activity["primaryActionDetail"]=}')
            #logger.debug(f'{activity["actions"]=}')
            #logger.debug(f'{activity["targets"]=}')
            time_info = self.get_time_info(activity)
            timestmap_format = '%Y-%m-%dT%H:%M:%S.%f%z' if '.' in time_info else '%Y-%m-%dT%H:%M:%S%z'
            data = {
                'timestamp': datetime.datetime.strptime(time_info, timestmap_format),
                'target': next(map(self.get_target_info, activity['targets']), None),
            }
            data['action'], data['action_detail'] = self.get_action_info(activity['primaryActionDetail'])
            return data

    def get_move_from(self, action_detail: dict) -> str:
        removed_parents = action_detail['move'].get('removedParents', [{}])
        return self.get_target_info(removed_parents[0])

    def get_one_of(self, obj: dict) -> str:
        # Returns the name of a set property in an object, or else "unknown".
        for key in obj:
            return key
        return 'unknown'

    def get_time_info(self, activity: dict) -> str:
        # Returns a time associated with an activity.
        if 'timestamp' in activity:
            return activity['timestamp']
        if 'timeRange' in activity:
            return activity['timeRange']['endTime']
        return 'unknown'

    def get_action_info(self, actionDetail: dict) -> tuple:
        # Returns the type of action.
        for key in actionDetail:
            match key:
                case 'create':
                    action_detail: str = self.get_one_of(actionDetail[key])
                case 'move' if actionDetail[key].get('removedParents'):
                    action_detail: tuple = self.get_target_info(actionDetail[key]["removedParents"][0])
                case 'rename' if actionDetail[key].get('oldTitle'):
                    action_detail: str = actionDetail[key]['oldTitle']
                case 'delete' | 'restore' | 'dlpChange' | 'reference':
                    action_detail: str = actionDetail[key]['type']
                case 'permissionChange':
                    action_detail: list = actionDetail[key]['addedPermissions']
                case 'comment':
                    actionDetail[key].pop('mentionedUsers')
                    action_detail = actionDetail[key][self.get_one_of(actionDetail[key])]['subtype']
                case 'settingsChange':
                    action_detail = actionDetail[key]['restrictionChanges'][0]['newRestriction']
                case _:
                    action_detail = None
            return key, action_detail
        return 'unknown', None

    def get_target_info(self, target: dict) -> tuple:
        # Returns the type of a target and an associated title.
        if 'driveItem' in target:
            title = target['driveItem'].get('title') or 'unknown'
            name = target['driveItem'].get('name')
            mimeType = target['driveItem'].get('mimeType')
            return title, name, mimeType
        if 'drive' in target:
            title = target['drive'].get('title') or 'unknown'
            name = target['drive'].get('name')
            mimeType = target['drive'].get('mimeType')
            return title, name, mimeType
        if 'fileComment' in target:
            parent = target['fileComment'].get('parent') or {}
            title = parent.get('title') or 'unknown'
            name = parent.get('name')
            mimeType = parent.get('mimeType')
            return title, name, mimeType
        return self.get_one_of(target), None, None
