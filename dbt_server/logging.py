import json
import logging
import uuid
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from dbt.events.functions import EVENT_MANAGER
from dbt.events.eventmgr import LoggerConfig, LineFormat, EventLevel
from dbt.events.base_types import BaseEvent
from pythonjsonlogger import jsonlogger
from dbt_server.services import filesystem_service

from dbt.events import AdapterLogger
from dbt.events.types import (
    AdapterEventDebug,
    AdapterEventInfo,
    AdapterEventWarning,
    AdapterEventError,
)

from dbt_server.models import TaskState

DBT_SERVER_EVENT_LOGGER = AdapterLogger("Server")
DBT_SERVER_EVENT_TYPES = [
    AdapterEventDebug,
    AdapterEventInfo,
    AdapterEventWarning,
    AdapterEventError
]

ACCOUNT_ID = os.environ.get("ACCOUNT_ID")
ENVIRONMENT_ID = os.environ.get("ENVIRONMENT_ID")
WORKSPACE_ID = os.environ.get("WORKSPACE_ID")

dbt_event_to_python_root_log = {
    EventLevel.DEBUG: logging.root.debug,
    EventLevel.TEST: logging.root.debug,
    EventLevel.INFO: logging.root.info,
    EventLevel.WARN: logging.root.warn,
    EventLevel.ERROR: logging.root.error,
}

class CustomJsonFormatter(jsonlogger.JsonFormatter):
    def add_fields(self, log_record, record, message_dict):
        super(CustomJsonFormatter, self).add_fields(log_record, record, message_dict)
        if not log_record.get("timestamp"):
            created = datetime.utcnow()
            if record.created:
                created = datetime.utcfromtimestamp(record.created)
            now = created.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            log_record["timestamp"] = now
        if ACCOUNT_ID and "accountID" not in log_record:
            log_record["accountID"] = ACCOUNT_ID
        if ENVIRONMENT_ID and "environmentID" not in log_record:
            log_record["environmentID"] = ENVIRONMENT_ID
        if WORKSPACE_ID and "workspaceID" not in log_record:
            log_record["workspaceID"] = WORKSPACE_ID


# setup json logging for stdout and datadog
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
stdout = logging.StreamHandler()
if os.environ.get("APPLICATION_ENVIRONMENT") in ("dev", None):
    formatter = logging.Formatter(
        "%(asctime)s - [%(process)d] %(name)s - %(levelname)s - %(message)s"
    )
else:
    formatter = CustomJsonFormatter(
        "%(timestamp)f %(filename)s %(funcName)s %(levelname)s "
        "%(lineno)d %(message)s %(module)s %(pathname)s %(process)d "
        "%(processName)s %(thread)s %(threadName)s %(name)s "
        "[dd.service=%(dd.service)s dd.env=%(dd.env)s dd.version=%(dd.version)s "
        "dd.trace_id=%(dd.trace_id)s dd.span_id=%(dd.span_id)s]"
    )
stdout.setFormatter(formatter)
logger.addHandler(stdout)


# make sure uvicorn is deferring to the root
# logger to format logs
logger_instance = logging.root.manager.loggerDict.get("uvicorn")
if logger_instance:
    logger.propagate = True
    logger_instance.handlers = []
logger_instance = logging.root.manager.loggerDict.get("uvicorn.error")
if logger_instance:
    logger.propagate = True
    logger_instance.handlers = []


def configure_uvicorn_access_log():
    """Configure uvicorn access log.

    This is in a dedicated function because it
    needes to be configured via the application
    startup event, otherwise uvicorn will overrride
    our desired configuration.
    """
    ual = logging.getLogger("uvicorn.access")
    ual.propagate = True
    ual.handlers = []


# Push event messages to root python logger for formatting
def log_event_to_console(event: BaseEvent):
    logging_method = dbt_event_to_python_root_log[event.log_level()]
    if type(event) not in DBT_SERVER_EVENT_TYPES and logging_method == logging.root.debug:
        # Only log debug level for dbt-server logs
        return
    logging_method(event.info.msg)


EVENT_MANAGER.callbacks.append(log_event_to_console)


# TODO: This should be some type of event. We may also choose to send events for all task state updates.
@dataclass
class ServerLog:
    state: TaskState
    error: Optional[str]

    def to_json(self):
        return json.dumps(self.__dict__)


# TODO: Make this a contextmanager
class LogManager(object):
    def __init__(self, log_path):
        self.name = str(uuid.uuid4())
        self.log_path = log_path
        filesystem_service.ensure_dir_exists(self.log_path)
        logger_config = LoggerConfig(
            name=self.name,
            line_format=LineFormat.Json,
            level=EventLevel.INFO,
            use_colors=True,
            output_file_name=log_path,
            # TODO: Add scrubber for secrets
        )
        EVENT_MANAGER.add_logger(logger_config)

    def cleanup(self):
        # TODO: verify that threading doesn't result in wonky list
        EVENT_MANAGER.loggers = [log for log in EVENT_MANAGER.loggers if log.name != self.name]

