import json
import logging
from typing import Optional
import logbook
import logbook.queues

import dbt.logger as dbt_logger

import io
from dataclasses import dataclass

from .services import filesystem_service
from .models import TaskState

GLOBAL_LOGGER = logging.getLogger(__name__)
GLOBAL_LOGGER.setLevel(logging.DEBUG)
stdout = logging.StreamHandler()
stdout.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
stdout.setFormatter(formatter)
GLOBAL_LOGGER.addHandler(stdout)
logger = GLOBAL_LOGGER


json_formatter = dbt_logger.JsonFormatter(format_string=dbt_logger.STDOUT_LOG_FORMAT)


@dataclass
class ServerLog:
    state: TaskState
    error: Optional[str]

    def to_json(self):
        return json.dumps(self.__dict__)


class LogManager(object):
    def __init__(self, log_path):
        self.log_path = log_path

        filesystem_service.ensure_dir_exists(self.log_path)

        logs_redirect_handler = logbook.FileHandler(
            filename=self.log_path,
            level=logbook.DEBUG,
            bubble=True,
            # TODO : Do we want to filter these?
            filter=self._dbt_logs_only_filter,
        )

        # Big hack?
        logs_redirect_handler.formatter = json_formatter

        self.handlers = [
            logs_redirect_handler,
        ]

        dbt_logger.log_manager.set_path(None)

    def _dbt_logs_only_filter(self, record, handler):
        """
        DUPLICATE OF LogbookStepLogsStreamWriter._dbt_logs_only_filter
        """
        return record.channel.split(".")[0] == "dbt"

    def setup_handlers(self):
        logger.info("Setting up log handlers...")

        dbt_logger.log_manager.objects = [
            handler
            for handler in dbt_logger.log_manager.objects
            if type(handler) is not logbook.NullHandler
        ]

        handlers = [logbook.NullHandler()] + self.handlers

        self.log_context = logbook.NestedSetup(handlers)
        self.log_context.push_application()

        logger.info("Done setting up log handlers.")

    def cleanup(self):
        self.log_context.pop_application()


class CapturingLogManager(LogManager):
    def __init__(self, log_path):
        super().__init__(log_path)

        self._stream = io.StringIO()
        capture_handler = logbook.StreamHandler(
            stream=self._stream,
            level=logbook.DEBUG,
            bubble=True,
            filter=self._dbt_logs_only_filter,
        )

        capture_handler.formatter = json_formatter

        self.handlers += [capture_handler]

    def getLogs(self):
        # Be a good citizen with the seek pos
        pos = self._stream.tell()
        self._stream.seek(0)
        res = self._stream.read().split("\n")
        self._stream.seek(pos)
        return res
