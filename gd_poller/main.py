import logging
import asyncio
import datetime

import pydantic

from . import dispatchers
from .apis import GoogleDrive
from .pollers import ActivityPoller
from .helpers.helpers import check_tasks
from .helpers.loggers import set_logger
from .models import AppSettings

logger = logging.getLogger(__name__)

LOCAL_TIMEZONE = (
    datetime.datetime.now(datetime.timezone(datetime.timedelta(0))).astimezone().tzinfo
)


async def async_main(settings_file: str | None = None) -> None:
    pollers = []
    tasks = []
    try:
        try:
            settings = AppSettings(user_yaml_file=settings_file) # type: ignore
        except pydantic.ValidationError as e:
            logger.error(e)
            return
        set_logger(
            level=settings.logging.level,
            format=settings.logging.format,
            datefmt=settings.logging.date_format,
            redacted_patterns=settings.logging.redacted_patterns,
            redacted_substitute=settings.logging.redacted_substitute,
        )
        if not settings.pollers:
            raise ValueError("pollers 설정이 없습니다.")
        drive = GoogleDrive(
            settings.google_drive.token.model_dump(),
            settings.google_drive.scopes,
            cache_enable=settings.google_drive.cache_enable,
            cache_maxsize=settings.google_drive.cache_maxsize,
            cache_ttl=settings.google_drive.cache_ttl,
        )
        for poller in settings.pollers:
            dispatcher_list = []
            for dispatcher in poller.dispatchers:
                try:
                    # yaml의 앵커는 동일한 객체를 참조
                    class_ = getattr(dispatchers, dispatcher.class_)
                    dispatcher_list.append(
                        class_(**dispatcher.model_dump(exclude={"class_"}))
                    )
                except Exception as e:
                    logger.exception(dispatcher)
                    raise e
            activity_poller = ActivityPoller(
                drive,
                poller.targets,
                dispatcher_list=dispatcher_list,
                name=poller.name,
                polling_interval=poller.polling_interval,
                page_size=poller.page_size,
                actions=poller.actions,
                task_check_interval=poller.task_check_interval,
                patterns=poller.patterns,
                ignore_patterns=poller.ignore_patterns,
                ignore_folder=poller.ignore_folder,
                dispatch_interval=poller.dispatch_interval,
                polling_delay=poller.polling_delay,
            )
            pollers.append(activity_poller)
        for poller in pollers:
            tasks.append(asyncio.create_task(poller.start(), name=poller.name))
        task_check_interval = settings.task_check_interval or -1
        if task_check_interval > 0:
            tasks.append(
                asyncio.create_task(
                    check_tasks(tasks, task_check_interval),
                    name="check_tasks",
                )
            )
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        logger.warning(f"Tasks are cancelled...")
    except Exception as e:
        logger.exception(e)
    finally:
        logger.info("Stopping pollers....")
        running_tasks = tuple(task for task in tasks if not task.done())
        if running_tasks:
            for task in running_tasks:
                task.cancel()
            await asyncio.gather(*running_tasks, return_exceptions=True)
        stop_tasks = []
        for poller in pollers:
            stop_tasks.append(asyncio.create_task(poller.stop(), name=poller.name))
        if stop_tasks:
            await asyncio.gather(*stop_tasks, return_exceptions=True)


def main(settings_file: str | None = None) -> None:
    try:
        asyncio.run(async_main(settings_file))
    except KeyboardInterrupt:
        logger.debug("KeyboardInterrupt....")
