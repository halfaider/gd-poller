import queue
import pathlib
import logging
import traceback
import datetime
import asyncio
from typing import Any, Iterable

import dispatchers
from gd_api import GoogleDrive
from helpers import await_sync, PrioritizedItem

LOCAL_TIMEZONE = datetime.datetime.now(datetime.timezone(datetime.timedelta(0))).astimezone().tzinfo
logger = logging.getLogger(__name__)


class GoogleDrivePoller:

    def __init__(self, drive: GoogleDrive, targets: list[str], dispatcher_list: list[dispatchers.Dispatcher] = None, name: str = None,
                 polling_interval: int = 60, page_size: int = 50, actions: Iterable = None,
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
    def dispatcher_list(self) -> list[dispatchers.Dispatcher]:
        return self._dispatcher_list

    @dispatcher_list.setter
    def dispatcher_list(self, dispatcher_list: list[dispatchers.Dispatcher]) -> None:
        self._dispatcher_list = dispatcher_list if dispatcher_list else [dispatchers.DummyDispatcher()]

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
    def patterns(self) -> list:
        return self._patterns

    @patterns.setter
    def patterns(self, patterns: list) -> None:
        self._patterns = list(patterns) if patterns else ['*']

    @property
    def ignore_patterns(self) -> list:
        return self._ignore_patterns

    @ignore_patterns.setter
    def ignore_patterns(self, ignore_patterns: list) -> None:
        self._ignore_patterns = list(ignore_patterns) if ignore_patterns else []

    @property
    def dispatch_interval(self) -> int:
        return self._dispatch_interval

    @dispatch_interval.setter
    def dispatch_interval(self, dispatch_interval: int) -> None:
        self._dispatch_interval = int(dispatch_interval) if dispatch_interval else 1

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

    async def start(self) -> None:
        self._dispatch_queue = queue.PriorityQueue()
        self._tasks = []
        if self.stop_event.is_set():
            self.stop_event.clear()
        self.tasks.append(asyncio.create_task(self.dispatch(), name=self.name))
        for target in self.targets:
            self.tasks.append(asyncio.create_task(self.poll(target), name=target))
        try:
            await asyncio.gather(*self.tasks)
        except asyncio.CancelledError:
            logger.warning(f'Tasks are cancelled: {self.name}')

    async def stop(self) -> None:
        self.stop_event.set()
        for task in self.tasks:
            logger.debug(task)
            if not task.done():
                task.print_stack()
                task.cancel()
        self._dispatch_queue = None
        self._tasks = None

    def check_patterns(self, path: str, patterns: list) -> bool:
        test = pathlib.Path(path)
        for pattern in patterns:
            if test.match(pattern):
                return True
        return False

    async def poll(self, target: Any) -> None:
        raise Exception('이 메소드를 구현하세요.')

    async def dispatch(self) -> None:
        raise Exception('이 메소드를 구현하세요.')


class ChangePoller(GoogleDrivePoller):
    pass


class ActivityPoller(GoogleDrivePoller):

    async def dispatch(self) -> None:
        logger.info(f'Dispatching task starts: {self.name}')
        while not self.stop_event.is_set():
            while not self.dispatch_queue.empty():
                data = None
                try:
                    data = self.dispatch_queue.get().item
                    # action 필터링
                    if data['action'] not in self.actions:
                        logger.debug(f'Skip: target={data["target"]} reason={data["action"]}')
                        continue
                    # 폴더 타입 확인
                    if data['target'][2] in [
                            'application/vnd.google-apps.folder',
                            'application/vnd.google-apps.shortcut'
                            ]:
                        data['is_folder'] = True
                    else:
                        data['is_folder'] = False
                    # 폴더 무시 판단
                    if self.ignore_folder and data['is_folder']:
                        logger.debug(f'Skip: target={data["target"]} reason=folder')
                        continue
                    # 대상이 영구히 삭제돼서 조회 불가능 할 경우
                    if data['action'] == 'delete' and data['action_detail'] != 'TRASH':
                        logger.debug(f'Skip: target={data["target"]} reason="deleted permanently"')
                        continue
                    # 대상 경로
                    target_id = data['target'][1].partition('/')[-1]
                    data['path'], parent = self.drive.get_full_path(target_id, data.get('ancestor'))
                    if not parent[0]:
                        logger.warning(f"Could not figure out its path: id={target_id} ancestor={data.get('ancestor')} parent={parent[0]}")
                        data['path'] = f"/unknown/{data['target'][0]}"
                    # url 링크
                    if data['is_folder']:
                        url_folder_id = target_id
                    else:
                        url_folder_id = parent[1]
                    data['url'] = f'https://drive.google.com/drive/folders/{url_folder_id}'
                    # 패턴 체크
                    if not self.check_patterns(data['path'], self.patterns):
                        logger.debug(f'Skip: target={data["target"]} reason="Not match with patterns"')
                        continue
                    if self.check_patterns(data['path'], self.ignore_patterns):
                        logger.debug(f'Skip: target={data["target"]} reason="Match with ignore patterns"')
                        continue
                    # move일 경우 소스 경로
                    data['removed_path'] = None
                    if data['action'] == 'move' and data['action_detail']:
                        logger.debug(f'Moved from: {data["action_detail"]}')
                        try:
                            removed_parent_id = data['action_detail'][1].partition('/')[-1]
                            data['removed_path'], _ = self.drive.get_full_path(removed_parent_id, data.get('ancestor'))
                        except Exception as e:
                            logger.error(traceback.format_exc())
                    # 기타 정보
                    data['timestamp'] = data['timestamp'].astimezone(LOCAL_TIMEZONE).strftime('%Y-%m-%dT%H:%M:%S%z')
                    data['poller'] = self.name
                    for dispatcher in self.dispatcher_list:
                        await dispatcher.dispatch(data)
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error(f'{data=}')
                finally:
                    if data:
                        self.dispatch_queue.task_done()
                # 큐에서 각 아이템을 꺼낸 후 sleep
                for _ in range(self.dispatch_interval):
                    await asyncio.sleep(1)
            # 큐에서 아이템을 모두 꺼낸 후 sleep
            await asyncio.sleep(1)
        logger.info(f'Dispatching task ends: {self.name}')

    async def poll(self, ancestor: str) -> None:
        ancestor_id, _, _ = ancestor.partition('#')
        next_page_token = None
        # 구글 응답에 맞춰서 UTC
        last_activity_timestamp = datetime.datetime.now().astimezone(datetime.timezone.utc)
        logger.info(f'Polling task starts: {ancestor}')
        while not self.stop_event.is_set():
            while not self.stop_event.is_set():
                try:
                    query = self.drive.api_activity.activity().query(body={
                        'pageSize': self.page_size,
                        'ancestorName': f'items/{ancestor_id}',
                        'pageToken': next_page_token,
                        'filter': f'time > "{last_activity_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ")}"',
                    })
                    try:
                        results = await await_sync(query.execute)
                    except Exception as e:
                        logger.error(traceback.format_exc())
                        logger.error(f'{ancestor=}')
                        break
                    if results.get('nextPageToken'):
                        next_page_token = results.get('nextPageToken')
                    activities = results.get('activities', [])
                    if not activities:
                        #logger.debug(F'No activity in {ancestor} since {last_activity_timestamp}')
                        break
                    for activity in activities:
                        data = {'ancestor': ancestor}
                        #logger.debug(f'{activity["primaryActionDetail"]=}')
                        #logger.debug(f'{activity["actions"]=}')
                        #logger.debug(f'{activity["targets"]=}')
                        timestamp = self.getTimeInfo(activity)
                        timestmap_format = '%Y-%m-%dT%H:%M:%S.%f%z' if '.' in timestamp else '%Y-%m-%dT%H:%M:%S%z'
                        data['timestamp'] = datetime.datetime.strptime(timestamp, timestmap_format)
                        if data['timestamp'] > last_activity_timestamp:
                            last_activity_timestamp = data['timestamp']
                        data['action'], data['action_detail'] = self.getActionInfo(activity['primaryActionDetail'])
                        data['target'] = next(map(self.getTargetInfo, activity['targets']))
                        logger.debug(f"{data['action']}, {data['target']}")
                        self.dispatch_queue.put(PrioritizedItem(data['timestamp'].timestamp(), data))
                    if not next_page_token:
                        break
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error(f'{ancestor=}')
                    break
            for _ in range(self.polling_interval):
                await asyncio.sleep(1)
                if self.stop_event.is_set(): break
        logger.info(f'Polling task ends: {ancestor}')

    def get_move_from(self, action_detail: dict) -> str:
        removed_parents = action_detail['move'].get('removedParents', [{}])
        return self.getTargetInfo(removed_parents[0])

    def getOneOf(self, obj: dict) -> str:
        # Returns the name of a set property in an object, or else "unknown".
        for key in obj:
            return key
        return 'unknown'

    def getTimeInfo(self, activity: dict) -> str:
        # Returns a time associated with an activity.
        if 'timestamp' in activity:
            return activity['timestamp']
        if 'timeRange' in activity:
            return activity['timeRange']['endTime']
        return 'unknown'

    def getActionInfo(self, actionDetail: dict) -> tuple:
        # Returns the type of action.
        for key in actionDetail:
            match key:
                case 'create':
                    action_detail = self.getOneOf(actionDetail[key])
                case 'move' if actionDetail[key].get('removedParents'):
                    action_detail: tuple = self.getTargetInfo(actionDetail[key]["removedParents"][0])
                case 'rename' if actionDetail[key].get('oldTitle'):
                    action_detail = actionDetail[key]['oldTitle']
                case 'delete' | 'restore' | 'dlpChange' | 'reference':
                    action_detail = actionDetail[key]['type']
                case 'permissionChange':
                    action_detail = actionDetail[key]['addedPermissions']
                case 'comment':
                    actionDetail[key].pop('mentionedUsers')
                    action_detail = actionDetail[key][self.getOneOf(actionDetail[key])]['subtype']
                case 'settingsChange':
                    action_detail = actionDetail[key]['restrictionChanges'][0]['newRestriction']
                case _:
                    action_detail = None
            return key, action_detail
        return 'unknown', None

    def getTargetInfo(self, target: dict) -> tuple:
        # Returns the type of a target and an associated title.
        if 'driveItem' in target:
            title = target['driveItem'].get('title', 'unknown')
            name = target['driveItem'].get('name')
            mimeType = target['driveItem'].get('mimeType')
            return title, name, mimeType
        if 'drive' in target:
            title = target['drive'].get('title', 'unknown')
            name = target['drive'].get('name')
            mimeType = target['drive'].get('mimeType')
            return title, name, mimeType
        if 'fileComment' in target:
            parent = target['fileComment'].get('parent', {})
            title = parent.get('title', 'unknown')
            name = parent.get('name')
            mimeType = parent.get('mimeType')
            return title, name, mimeType
        return self.getOneOf(target), None, None
