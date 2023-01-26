import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

try:
    from dbt.events.functions import STDOUT_LOG, FILE_LOG
except (ModuleNotFoundError, ImportError):
    STDOUT_LOG = None
    FILE_LOG = None

from pythonjsonlogger import jsonlogger
from dbt_server.models import TaskState


ACCOUNT_ID = os.environ.get("ACCOUNT_ID")
ENVIRONMENT_ID = os.environ.get("ENVIRONMENT_ID")
WORKSPACE_ID = os.environ.get("WORKSPACE_ID")


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


# setup json logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
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
dbt_server_logger = logging.getLogger("dbt-server")
dbt_server_logger.setLevel(logging.DEBUG)
GLOBAL_LOGGER = dbt_server_logger

# remove handlers from these loggers, so
# that they propagate up to the root logger
# for json formatting
if STDOUT_LOG and FILE_LOG:
    STDOUT_LOG.handlers = []
    FILE_LOG.handlers = []

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


@dataclass
class ServerLog:
    state: TaskState
    error: Optional[str]

    def to_json(self):
        return json.dumps(self.__dict__)
