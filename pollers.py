import queue
import pathlib
import logging
import traceback
import time
import datetime
from threading import Thread, Event
from typing import Any

from gd_api import GoogleDrive
from dispatchers import Dispatcher


LOCAL_TIMEZONE = datetime.datetime.now(datetime.timezone(datetime.timedelta(0))).astimezone().tzinfo
logger = logging.getLogger(__name__)


class GoogleDrivePoller:

    def __init__(self, drive: GoogleDrive, dispatchers: list[Dispatcher] = None, targets: list = None, name: str = None,
                 polling_interval: int = 60, page_size: int = 100, actions: tuple = None,
                 patterns: list = None, ignore_patterns: list = None, ignore_directories: bool = True):
        self._drive = drive
        self._targets = targets
        self._name = name
        self._polling_interval = polling_interval
        self._page_size = page_size
        self._event = Event()
        self._dispatch_queue = queue.Queue()
        self._tasks = []
        self._dispatchers = dispatchers
        self._ignore_directories = bool(ignore_directories)
        self._patterns = patterns if patterns else ['*']
        self._ignore_patterns = ignore_patterns if ignore_patterns else []
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
    def drive(self) -> GoogleDrive:
        return self._drive

    @property
    def dispatchers(self) -> list[Dispatcher]:
        return self._dispatchers

    @property
    def targets(self) -> list:
        return self._targets

    @property
    def name(self) -> str:
        return self._name

    @property
    def polling_interval(self) -> int:
        return self._polling_interval

    @property
    def page_size(self) -> int:
        return self._page_size

    @property
    def event(self) -> Event:
        return self._event

    @property
    def dispatch_queue(self) -> queue.Queue:
        return self._dispatch_queue

    @property
    def actions(self) -> tuple:
        return self._actions

    @property
    def ignore_directories(self) -> bool:
        return self._ignore_directories

    @property
    def patterns(self) -> list:
        return self._patterns

    @property
    def ignore_patterns(self) -> list:
        return self._ignore_patterns

    @property
    def tasks(self) -> list[Thread]:
        return self._tasks

    def start(self) -> None:
        if self.event.is_set():
            self.event.clear()
        dispatching_task = Thread(target=self.dispatch)
        dispatching_task.start()
        self.tasks.append(dispatching_task)
        for target in self.targets:
            polling_task = Thread(target=self.poll, args=(target,))
            polling_task.start()
            self.tasks.append(polling_task)

    def join(self) -> None:
        for task in self.tasks:
            task.join()

    def stop(self) -> None:
        self.event.set()

    def poll(self, target: Any) -> None:
        raise Exception('이 메소드를 구현하세요.')

    def dispatch(self) -> None:
        raise Exception('이 메소드를 구현하세요.')


class ChangePoller(GoogleDrivePoller):
    pass


class ActivityPoller(GoogleDrivePoller):

    def check_patterns(self, path: str, patterns: list) -> bool:
        test = pathlib.Path(path)
        for pattern in patterns:
            if test.match(pattern):
                return True
        return False

    def dispatch(self) -> None:
        logger.info(f'Dispatching task starts: {self.name}')
        while not self.event.is_set():
            while not self.dispatch_queue.empty():
                try:
                    data = self.dispatch_queue.get()
                    self.dispatch_queue.task_done()
                    if data['action'] not in self.actions:
                        logger.debug(f'Not included in actions: {data["action"]}')
                        continue
                    if data['target'][2] in [
                            'application/vnd.google-apps.folder',
                            'application/vnd.google-apps.shortcut'
                            ]:
                        data['is_folder'] = True
                    else:
                        data['is_folder'] = False
                    if self.ignore_directories and data['is_folder']:
                        logger.debug(f'Ignore this "directory": {data["path"]}')
                        continue
                    traget_id = data['target'][1].partition('/')[-1]
                    data['path'] = self.drive.get_full_path(traget_id, data.get('ancestor'))
                    if not self.check_patterns(data['path'], self.patterns):
                        logger.debug(f'Not match with patterns: {data["path"]}')
                        continue
                    if self.check_patterns(data['path'], self.ignore_patterns):
                        logger.debug(f'Match with ignore patterns: {data["path"]}')
                        continue
                    if data.get('move_from'):
                        logger.debug(f'Moved from: {data["move_from"]}')
                        if data['move_from'][1]:
                            src_id = data['move_from'][1].partition('/')[-1]
                            # 다른 ancestor에서 옮겨진 거라면 정확한 경로를 알 수 없음
                            data['src_path'] = self.drive.get_full_path(src_id, data.get('ancestor'))
                        else:
                            # action 데이터에 removedParents가 없으면 출처를 알 수 없음
                            data['src_path'] = data['move_from'][0]
                    data['timestamp'] = data['timestamp'].astimezone(LOCAL_TIMEZONE).strftime('%Y-%m-%dT%H:%M:%S%z')
                    for dispatcher in self.dispatchers:
                        dispatcher.dispatch(data)
                except Exception as e:
                    logger.error(traceback.format_exc())
            time.sleep(1)
        logger.info(f'Dispatching task ends: {self.name}')

    def poll(self, ancestor: str) -> None:
        ancestor_id, _, _ = ancestor.partition('/')
        next_page_token = None
        # 구글 응답에 맞춰서 UTC
        last_activity_timestamp = datetime.datetime.now().astimezone(datetime.timezone.utc)
        logger.info(f'Polling task starts: {ancestor}')
        while not self.event.is_set():
            while not self.event.is_set():
                try:
                    query = self.drive.api_activity.activity().query(body={
                        'pageSize': self.page_size,
                        'ancestorName': f'items/{ancestor_id}',
                        'pageToken': next_page_token,
                        'filter': f'time > "{last_activity_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ")}"',
                    })
                    try:
                        results = query.execute()
                    except Exception as e:
                        logger.error(traceback.format_exc())
                        break
                    if results.get('nextPageToken'):
                        next_page_token = results.get('nextPageToken')
                    activities = results.get('activities', [])
                    if not activities:
                        #logger.debug(F'No activity in {ancestor} since {last_activity_timestamp}')
                        break
                    for activity in activities:
                        data = {}
                        logger.debug(f'{activity["primaryActionDetail"]=}')
                        logger.debug(f'{activity["actions"]=}')
                        logger.debug(f'{activity["targets"]=}')
                        timestamp = self.getTimeInfo(activity)
                        timestmap_format = '%Y-%m-%dT%H:%M:%S.%f%z' if '.' in timestamp else '%Y-%m-%dT%H:%M:%S%z'
                        timestamp_utc = datetime.datetime.strptime(timestamp, timestmap_format)
                        if timestamp_utc > last_activity_timestamp:
                            last_activity_timestamp = timestamp_utc
                        action = self.getActionInfo(activity['primaryActionDetail'])
                        if action == 'move':
                            data['move_from'] = self.get_move_from(activity['primaryActionDetail'])
                        #targets = [self.getTargetInfo(target) for target in activity['targets']]
                        target = next(map(self.getTargetInfo, activity['targets']))
                        logger.debug(f'{action}, {target}')
                        data['timestamp'] = timestamp_utc
                        data['action'] = action
                        data['target'] = target
                        data['ancestor'] = ancestor
                        self.dispatch_queue.put_nowait(data)
                    if not next_page_token:
                        break
                except Exception as e:
                    logger.error(traceback.format_exc())
                    break
            for _ in range(self.polling_interval):
                time.sleep(1)
                if self.event.is_set(): break
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

    def getActionInfo(self, actionDetail: dict) -> str:
        # Returns the type of action.
        return self.getOneOf(actionDetail)

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
