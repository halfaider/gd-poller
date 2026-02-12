import re
import time
import queue
import pathlib
import logging
import asyncio
import datetime
from abc import ABC, abstractmethod
from typing import Any, Iterable

from . import dispatchers
from .apis import GoogleDrive
from .models import ActivityData
from .helpers.helpers import await_sync, check_tasks

logger = logging.getLogger(__name__)

LOCAL_TIMEZONE = (
    datetime.datetime.now(datetime.timezone(datetime.timedelta(0))).astimezone().tzinfo
)


class GoogleDrivePoller(ABC):

    def __init__(
        self,
        drive: GoogleDrive,
        targets: Iterable[str],
        dispatcher_list: Iterable[dispatchers.Dispatcher] = None,
        name: str = None,
        polling_interval: int = 60,
        page_size: int = 50,
        actions: Iterable = None,
        task_check_interval: int = -1,
        patterns: list = None,
        ignore_patterns: list = None,
        ignore_folder: bool = True,
        dispatch_interval: int = 1,
        polling_delay: int = 60,
    ):
        self.drive = drive
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
        self.polling_delay = polling_delay

        # polling_delay 초기화 후
        self.targets = targets

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
    def targets(self) -> dict:
        return self._targets

    @targets.setter
    def targets(self, targets: list) -> None:
        try:
            if len(targets) < 1:
                raise Exception(f"The targets is empty: {targets=}")
            """
            {
                '1DqPY6JooQ-_C1lNKxmptbVsKP1OWMIUv': {
                    'root': 'ROOT/GDRIVE/VIDEO/영화/제목',
                    'timestamps': [datetime.datetime(2025, 5, 30, 6, 30, 55, 536629, tzinfo=datetime.timezone.utc), 1748586702.0969362],
                }
            }
            """
            self._targets = {}
            last_activity = datetime.datetime.now(LOCAL_TIMEZONE) - datetime.timedelta(
                seconds=self.polling_delay
            )
            last_no_activity = time.time()
            # [마지막 액티비티의 시간, 마지막 task 확인 시간]
            for target in targets:
                ancestor_id, _, root = target.partition("#")
                self._targets[ancestor_id] = {
                    "root": root,
                    "timestamps": [last_activity, last_no_activity],
                }
        except:
            raise Exception(f"The targets is not a list: {targets=}")

    @property
    def dispatcher_list(self) -> Iterable[dispatchers.Dispatcher]:
        return self._dispatcher_list

    @dispatcher_list.setter
    def dispatcher_list(
        self, dispatcher_list: Iterable[dispatchers.Dispatcher]
    ) -> None:
        self._dispatcher_list = (
            dispatcher_list if dispatcher_list else (dispatchers.DummyDispatcher(),)
        )

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
    def polling_interval(self, polling_interval: int = 60) -> None:
        self._polling_interval = int(polling_interval)

    @property
    def polling_delay(self) -> int:
        return self._polling_delay

    @polling_delay.setter
    def polling_delay(self, polling_delay: int = 60) -> None:
        self._polling_delay = int(polling_delay)

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
        self._actions = (
            tuple(actions)
            if actions
            else (
                "create",
                "edit",
                "move",
                "rename",
                "delete",
                "restore",
                "permissionChange",
                "comment",
                "dlpChange",
                "reference",
                "settingsChange",
                "appliedLabelChange",
            )
        )

    @property
    def patterns(self) -> Iterable[re.Pattern]:
        return self._patterns

    @patterns.setter
    def patterns(self, patterns: Iterable[str]) -> None:
        self._patterns = (
            tuple(re.compile(pattern, re.I) for pattern in patterns)
            if patterns
            else (re.compile(".*", re.I),)
        )

    @property
    def ignore_patterns(self) -> list:
        return self._ignore_patterns

    @ignore_patterns.setter
    def ignore_patterns(self, ignore_patterns: list) -> None:
        self._ignore_patterns = (
            tuple(re.compile(pattern, re.I) for pattern in ignore_patterns)
            if ignore_patterns
            else ()
        )

    @property
    def dispatch_interval(self) -> int:
        return self._dispatch_interval

    @dispatch_interval.setter
    def dispatch_interval(self, dispatch_interval: int) -> None:
        try:
            self._dispatch_interval = int(dispatch_interval)
        except Exception as e:
            logger.exception(e)
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
        self.tasks.append(
            asyncio.create_task(self.dispatch(), name=f"dispatching-{self.name}")
        )
        for target in self.targets:
            name = root if (root := self.targets[target].get("root")) else target
            self.tasks.append(
                asyncio.create_task(self.poll(target), name=f"polling-{name}")
            )
        for dispatcher in self.dispatcher_list:
            self.tasks.append(
                asyncio.create_task(
                    dispatcher.start(),
                    name=f"{self.name}-{dispatcher.__class__.__name__}",
                )
            )
        try:
            gathers = [*self.tasks]
            if self.task_check_interval > 0:
                check_task = asyncio.create_task(
                    check_tasks(self.tasks, self.task_check_interval),
                    name="check_tasks",
                )
                gathers.append(check_task)
            await asyncio.gather(*gathers)
        except asyncio.CancelledError:
            logger.warning(f"Tasks are cancelled: {self.name}")

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
                except Exception as e:
                    logger.exception(e)
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

    def __init__(self, *args: Any, **kwds: Any):
        super().__init__(*args, **kwds)
        self.resource = self.drive.api_activity.activity()
        self.semaphore = asyncio.Semaphore(5)

    async def dispatch(self) -> None:
        logger.info(f"Dispatching task starts: {self.name}")
        while not self.stop_event.is_set():
            await self._dispatch()
            # 큐에서 각 아이템을 꺼낸 후 sleep
            for _ in range(max(self.dispatch_interval * 10, 10)):
                await asyncio.sleep(0.1)
                if self.stop_event.is_set():
                    break
        logger.info(f"Dispatching task ends: {self.name}")

    async def _dispatch(self) -> None:
        data: ActivityData = None
        try:
            data: ActivityData = self.dispatch_queue.get_nowait()
            # action 필터링
            if data.action not in self.actions:
                logger.debug(f"Skipped: target={data.target} reason={data.action}")
                return
            # 폴더 타입 확인
            if data.target[2] in (
                "application/vnd.google-apps.folder",
                "application/vnd.google-apps.shortcut",
            ):
                data.is_folder = True
            # 폴더 무시 판단
            if self.ignore_folder and data.is_folder:
                logger.debug(f"Skipped: target={data.target} reason=folder")
                return
            # 대상이 영구히 삭제돼서 조회 불가능 할 경우
            if data.action == "delete" and data.action_detail != "TRASH":
                logger.debug(
                    f'Skipped: target={data.target} reason="deleted permanently"'
                )
                return
            # 대상 경로
            target_id = data.target[1].partition("/")[-1]
            async with self.semaphore:
                path_info = await asyncio.to_thread(
                    self.drive.get_full_path, target_id, data.ancestor, data.root
                )
            data.path = None
            parent = (None, None)
            web_view = None
            if path_info:
                data.path, parent, web_view, size = path_info
                data.size = int(size)
                if not parent[0]:
                    logger.warning(
                        f"Could not figure out its path: id={target_id} ancestor={data.ancestor} root={data.root} parent={parent[0]}"
                    )
                    data.path = f"/unknown/{data.target[0]}"
            data.parent = parent
            # url 링크
            if web_view:
                data.link = web_view.strip()
            else:
                url_folder_id = target_id if data.is_folder else parent[1]
                data.link = f"https://drive.google.com/drive/folders/{url_folder_id}"
            # move, rename일 경우 소스 경로
            data.removed_path = None
            if data.action == "move" and data.action_detail:
                logger.debug(f"Moved from: {data.action_detail}")
                try:
                    removed_parent_id = data.action_detail[1].partition("/")[-1]
                    # 다른 ancestor에서 이동된 경우 경로 매핑이 어려움
                    async with self.semaphore:
                        removed_path_info = await asyncio.to_thread(
                            self.drive.get_full_path,
                            removed_parent_id,
                            data.ancestor,
                            data.root,
                        )
                    if removed_path_info:
                        removed_path, _, _, _ = removed_path_info
                        data.removed_path = str(
                            pathlib.Path(removed_path, data.target[0])
                        )
                except Exception as e:
                    logger.exception(e)
            elif data.action == "rename" and data.action_detail:
                logger.debug(f"Renamed from: {data.action_detail}")
                if data.path:
                    data.removed_path = str(
                        pathlib.Path(data.path).with_name(data.action_detail)
                    )
            # 기타 정보
            data.timestamp_text = data.timestamp.astimezone(LOCAL_TIMEZONE).strftime(
                "%Y-%m-%dT%H:%M:%S%z"
            )
            data.poller = self.name
            # 패턴 체크
            for attr, name in [("path", "target"), ("removed_path", "removed_path")]:
                path_value = getattr(data, attr)
                if not path_value:
                    continue
                msg = f'Skipped: {name}="{path_value}" reason='
                if not self.check_patterns(path_value, self.patterns):
                    logger.debug(msg + '"Not match with patterns"')
                    setattr(data, attr, None)
                elif self.check_patterns(path_value, self.ignore_patterns):
                    logger.debug(msg + '"Match with ignore patterns"')
                    setattr(data, attr, None)
            # move된 경로를 접근할 수 없을 경우
            match bool(data.path), bool(data.removed_path):
                case False, True:
                    data.path = data.removed_path
                    data.removed_path = None
                    data.action = "delete"
                    if data.action_detail:
                        data.link = f'https://drive.google.com/drive/folders/{data.action_detail[1].partition("/")[-1]}'
                        data.action_detail = (
                            f"Moved but can not access: {data.target[1]}"
                        )
                case False, False:
                    logger.info(
                        f'Skipped: target={data.target} reason="No applicable path"'
                    )
                    return
            for dispatcher in self.dispatcher_list:
                # activity 발생 순서대로, dispatcher 배치 순서대로
                await dispatcher.dispatch(data)
        except queue.Empty:
            pass
        except:
            logger.exception(f"{data=}")
        finally:
            if data:
                self.dispatch_queue.task_done()

    async def poll(self, ancestor: str) -> None:
        logger.info(f"Polling task starts: {ancestor}")
        while not self.stop_event.is_set():
            await self._poll(ancestor)
            for _ in range(self.polling_interval):
                await asyncio.sleep(1)
                if self.stop_event.is_set():
                    break
        logger.info(f"Polling task ends: {ancestor}")

    async def _poll(self, ancestor: str) -> None:
        root = self.targets[ancestor].get("root")
        ancestor_name = root or ancestor
        next_page_token = None
        timestamps = self.targets[ancestor]["timestamps"]
        while not self.stop_event.is_set():
            try:
                start_time = timestamps[0]
                end_time = datetime.datetime.now(LOCAL_TIMEZONE) - datetime.timedelta(
                    seconds=self.polling_delay
                )
                query = self.resource.query(
                    body={
                        "pageSize": self.page_size,
                        "ancestorName": f"items/{ancestor}",
                        "pageToken": next_page_token,
                        "filter": f"time > {int(start_time.timestamp() * 1000)} AND time <= {int(end_time.timestamp() * 1000)}",
                    }
                )
                try:
                    results = await await_sync(query.execute)
                    # logger.debug(f'Polling: {str(start_time)} ~ {str(end_time)} ({ancestor_name}) {results=}')
                except Exception as e:
                    self.drive.handle_error(e)
                    logger.error(f"Polling failed: {ancestor=}")
                    break
                next_page_token = results.get("nextPageToken")
                activities = results.get("activities")
                if not activities:
                    current_timestamp = time.time()
                    if (
                        self.task_check_interval > 0
                        and current_timestamp - timestamps[1] > self.task_check_interval
                    ):
                        logger.debug(
                            f'No activity in "{ancestor_name}" since {start_time.astimezone(LOCAL_TIMEZONE)}'
                        )
                        timestamps[1] = current_timestamp
                    break
                # logger.debug(f'Polling: {str(start_time)} ~ {str(end_time)} ({ancestor_name}) {results=}')
                # activity가 1 개라도 있으면 start_time를 갱신
                timestamps[0] = end_time
                for activity in activities:
                    data = self.get_activity(activity)
                    data.ancestor = ancestor
                    data.root = root
                    data.priority = data.timestamp.timestamp()
                    logger.info(
                        f"{data.action}, {data.target} at {data.timestamp.astimezone(LOCAL_TIMEZONE)} ({ancestor_name})"
                    )
                    self.dispatch_queue.put(data)
                if not next_page_token:
                    break
            except:
                logger.exception(f"{ancestor=}")

    def get_activity(self, activity: dict) -> ActivityData:
        # logger.debug(f'{activity["primaryActionDetail"]=}')
        # logger.debug(f'{activity["actions"]=}')
        # logger.debug(f'{activity["targets"]=}')
        time_info = self.get_time_info(activity)
        timestmap_format = (
            "%Y-%m-%dT%H:%M:%S.%f%z" if "." in time_info else "%Y-%m-%dT%H:%M:%S%z"
        )
        action, action_detail = self.get_action_info(activity["primaryActionDetail"])
        return ActivityData(
            activity=activity,
            timestamp=datetime.datetime.strptime(time_info, timestmap_format),
            target=next(map(self.get_target_info, activity["targets"]), None),
            action=action,
            action_detail=action_detail,
        )

    def get_move_from(self, action_detail: dict) -> str:
        removed_parents = action_detail["move"].get("removedParents", [{}])
        return self.get_target_info(removed_parents[0])

    def get_one_of(self, obj: dict) -> str:
        # Returns the name of a set property in an object, or else "unknown".
        if len(obj) > 1:
            logger.error(f"MULTIPLE VALUES: {obj}")
        for key in obj:
            return key
        return "unknown"

    def get_time_info(self, activity: dict) -> str:
        # Returns a time associated with an activity.
        if "timestamp" in activity:
            return activity["timestamp"]
        if "timeRange" in activity:
            return activity["timeRange"]["endTime"]
        return "unknown"

    def get_action_info(self, actionDetail: dict) -> tuple:
        # Returns the type of action.
        for key in actionDetail:
            match key:
                case "create":
                    action_detail: str = self.get_one_of(actionDetail[key])
                case "move" if actionDetail[key].get("removedParents"):
                    removed_parents = actionDetail[key]["removedParents"]
                    if removed_parents:
                        action_detail: tuple = self.get_target_info(removed_parents[0])
                    else:
                        action_detail = None
                case "rename" if actionDetail[key].get("oldTitle"):
                    action_detail: str = actionDetail[key]["oldTitle"]
                case "delete" | "restore" | "dlpChange" | "reference":
                    action_detail: str = actionDetail[key]["type"]
                case "permissionChange":
                    action_detail: list = actionDetail[key]["addedPermissions"]
                case "comment":
                    actionDetail[key].pop("mentionedUsers")
                    action_detail = actionDetail[key][
                        self.get_one_of(actionDetail[key])
                    ]["subtype"]
                case "settingsChange":
                    action_detail = actionDetail[key]["restrictionChanges"][0][
                        "newRestriction"
                    ]
                case _:
                    action_detail = None
            return key, action_detail
        return "unknown", None

    def get_target_info(self, target: dict) -> tuple:
        # Returns the type of a target and an associated title.
        if "driveItem" in target:
            title = target["driveItem"].get("title") or "unknown"
            name = target["driveItem"].get("name")
            mimeType = target["driveItem"].get("mimeType")
            return title, name, mimeType
        if "drive" in target:
            title = target["drive"].get("title") or "unknown"
            name = target["drive"].get("name")
            mimeType = target["drive"].get("mimeType")
            return title, name, mimeType
        if "fileComment" in target:
            parent = target["fileComment"].get("parent") or {}
            title = parent.get("title") or "unknown"
            name = parent.get("name")
            mimeType = parent.get("mimeType")
            return title, name, mimeType
        return self.get_one_of(target), None, None
