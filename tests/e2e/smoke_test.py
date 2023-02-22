# Script that sends http request to local dbt-server for testing purpose.
# - It doesn't setup the dbt-server and requires user to start dbt-server
#   locally, the profile should match testing project located
#   in tests/e2e/fixtures/testing/jaffle_shop(i.e. profile name is jaffle_shop).
# - It will copy the jaffle_shop project into a temporary directory and test.
# - Requests are sent one by one, it won't touch concurrency issue.
# Notice:
# - Until bug fixed, you need to run dbt deps in tests/e2e/fixtures/jaffle_shop
#   first, otherwise the initial parse will fail.
# What can be done in future?
# - More commands for other use cases.
# - Switch parse endpoint to async endpoint.
# - Test other endpoints, e.g sync endpoint.
#
# Example run:
# # Under root directory.
# python3 -m tests.e2e.smoke_test --local_task_db_path=./working-dir/sql_app.db
#
from absl import app
from absl import flags
import asyncio
from dbt_server.models import Task
from dbt_server.models import TaskState
import logging
from tempfile import TemporaryDirectory
from os import path
from sqlalchemy import create_engine
from shutil import copytree
from sqlalchemy import select
from requests import post
from time import time
from typing import Any, NewType, List

flags.DEFINE_integer(
    "command_exec_timeout_seconds",
    60,
    "How many seconds shall we wait after command is issued. Test will fail if"
    "timeout happens."
)

flags.DEFINE_integer(
    "dbt_local_server_port",
    8580,
    "Dbt local server port for testing."
)

flags.DEFINE_string(
    "local_task_db_path",
    None,
    "A local path point to dbt-server sqlite db file, task status will be "
    "checked."
)

flags.DEFINE_enum(
    "testcase",
    "IDE",
    ["IDE"],
    "A enum to indicate testcases that will run."
)

flags.mark_flag_as_required("local_task_db_path")

TESTING_FIXTURE = "./tests/e2e/fixtures"
JAFFLE_SHOP_DIR = "jaffle_shop"
# State directory has manifest.json file to be used as --state.
STATE_DIR = "init_target"
LOCAL_URL = "http://127.0.0.1"
# Test case files.
IDE_COMMAND_SUCCESS_FILE = "./tests/e2e/testcases/ide_commands.txt"
IDE_COMMAND_FAILURE_FILE = "./tests/e2e/testcases/ide_commands_failure.txt"
# How long we will get task status from task db periodically.
ASYNC_POLLING_SECONDS = 1

ASYNC_DBT_URL = "async/dbt"


class DbtServerSmokeTest:
    """ DbtServerSmokeTest sets up a test dbt proejct(jaffle shop) to temp
    directory and provides testing helper functions to caller.

    This class is thread-safe.
    """

    def __init__(self, temp_dir: str, dbt_local_server_port: int,
                 local_task_db_path: str,
                 command_exec_timeout_seconds: int):
        """ Initializes smoke test environment.

        Args:
            temp_dir: a temp directory string that jaffle shop will be placed.
                Notice caller needs to clean up the directory.
            dbt_local_server_port: local dbt-server port.
            local_task_db_path: path to local task sqlite db file.
            command_exec_timeout_seconds: seconds that each test command is
                allowed to run.
        """
        self.temp_dir = temp_dir
        self.command_exec_timeout_seconds = command_exec_timeout_seconds
        self.dbt_local_server_port = dbt_local_server_port
        self.task_db_engine = create_engine(
            f"sqlite:///{local_task_db_path}",
            connect_args={"check_same_thread": False},
        )
        # Copy jaffle shop to temp directory.
        copytree(path.join(TESTING_FIXTURE, JAFFLE_SHOP_DIR),
                 path.join(self.temp_dir, JAFFLE_SHOP_DIR), dirs_exist_ok=True)

    def get_jaffle_shop_dir(self) -> str:
        """ Returns directory of testing jaffle shop, it's under temp directory.
        """
        return path.join(self.temp_dir, JAFFLE_SHOP_DIR)

    def post_request(self, url_path: str, body_obj: dict) -> dict:
        """ Sends a http post request to local dbt server with given `url_path`
        and json `body_obj`, the content type will always be json. Returns
        http response json body.

        Args:
            url_path: string of local dbt server url, DON'T add prefix /.
                Correct example: async/get.
            body_obj: dict of json body.

        Raises:
            Exception: if response status code is not 200.
        """
        url = f"{LOCAL_URL}:{self.dbt_local_server_port}/{url_path}"
        resp = post(url, json=body_obj)
        if resp.status_code != 200:
            logging.error(f"Request to {url_path} failed {resp.status_code}.")
            logging.error(f"body = {str(body_obj)}")
            raise Exception(f"Request failure with code = {resp.status_code}")
        return resp.json()

    def parse(self) -> None:
        """ Sends a http post request to make dbt server parse jaffle shop which
        is located in temp directory.

        Raises:
            Exception: if response status code is not 200.
        """
        self.post_request("parse", {
            "project_path": self.get_jaffle_shop_dir()
        })

    async def wait_async_exec(self, task_id: str) -> TaskState:
        """
        Waits task with `task_id` to be finished. Returns task final state.
        Raises Exception if task is not finished after given timeout config.

        Args:
            task_id: string of task id, we will lookup task status in task db
                according to task_id.
        """
        start_timestamp_seconds = time()
        while (time()
               < start_timestamp_seconds + self.command_exec_timeout_seconds):
            stmt = select(Task).where(Task.task_id == task_id)
            with self.task_db_engine.connect() as conn:
                tasks = list(conn.execute(stmt))
                if len(tasks) == 1 and tasks[0].state in [
                    TaskState.FINISHED, TaskState.ERROR
                ]:
                    return tasks[0].state
            await asyncio.sleep(ASYNC_POLLING_SECONDS)
        raise Exception(
            f"Task {task_id} is not finished after {self.command_exec_timeout_seconds}s")


# A common placeholder reference dict, all commands in test files will be
# replaced according to this.
COMMON_PLACEHOLDER_REFERENCE = {
    "%SELECTOR": "test_selector",
    "%VARIABLE": "test_var: 1",
    "%MODEL": "customers",
    "%MACRO": "test_macro",
    "%MACRO_ARGS": "int_value: 1"
}


def _parse_command_string(override_placeholder_reference: dict,
                          raw_command: str) -> List[str]:
    """ Parses input `raw_command` based on `override_placeholder_reference`,
    return list of string commands.

    Args:
        override_placeholder_reference: similar to COMMON_PLACEHOLDER_REFERENCE
            but it will override it. It's useful if there is any placeholder
            value is generated during execution.
        raw_command: string of input test raw command, may have placeholder.
    """
    commands = raw_command.split()
    for index in range(len(commands)):
        for placeholder, value in (
            COMMON_PLACEHOLDER_REFERENCE | override_placeholder_reference
        ).items():
            if commands[index] != placeholder:
                continue
            commands[index] = value
    return commands


async def _run_async_testcase(smoke_test: DbtServerSmokeTest,
                              command_list: List[str],
                              expected_db_task_status: TaskState) -> None:
    """ Sends post request to async endpoints with `command_list` as request
    body. If task status is not `expected_db_task_status` or local log file
    doesn't exist but status is finished, raises Exceptions.
    """
    logging.info(
        f"Start async test case {str(command_list)}, expect {expected_db_task_status}")
    resp = smoke_test.post_request(ASYNC_DBT_URL, {
        "command": command_list
    })
    task_id = resp["task_id"]
    task_status = await smoke_test.wait_async_exec(task_id)
    if task_status != expected_db_task_status:
        raise Exception(
            f"Error task_id={task_id}, status != {expected_db_task_status}")
    # Ensure log file is created when task is finished but won't check
    # details.
    if (task_status == TaskState.FINISHED
        and not path.isfile(path.join(smoke_test.get_jaffle_shop_dir(),
                                      resp["log_path"]))):
        raise Exception(f"Can't find log file for task_id={task_id}")


def _read_testcase_file(path: str) -> List[str]:
    """ Reads testcase file and filter out line starts with # which is comment.
    """
    with open(path, "r") as f:
        return list(filter(lambda line: not line.strip().startswith("#"),
                           f.readlines()))


async def _run_test_ide(smoke_test: DbtServerSmokeTest) -> None:
    """ Runs IDE test. """
    # First, trigger parse command to setup project path.
    # TODO(dichen): we may consider to switch to async endpoint in future.
    smoke_test.parse()
    # Second, test successful commands.
    override_placeholder_reference = {
        "%STATE_DIR": f"{smoke_test.get_jaffle_shop_dir()}/{STATE_DIR}",
    }
    success_commands = _read_testcase_file(IDE_COMMAND_SUCCESS_FILE)
    for command in success_commands:
        command_list = _parse_command_string(
            override_placeholder_reference, command)
        await _run_async_testcase(smoke_test, command_list, TaskState.FINISHED)
    # Third, test failure commands.
    failure_command = _read_testcase_file(IDE_COMMAND_FAILURE_FILE)
    for command in failure_command:
        command_list = _parse_command_string(
            override_placeholder_reference, command)
        await _run_async_testcase(smoke_test, command_list, TaskState.ERROR)


async def entry_point(temp_dir: str) -> None:
    command_exec_timeout_seconds = flags.FLAGS.command_exec_timeout_seconds
    dbt_local_server_port = flags.FLAGS.dbt_local_server_port
    local_task_db_path = flags.FLAGS.local_task_db_path
    testcase = flags.FLAGS.testcase

    smoke_test = DbtServerSmokeTest(
        temp_dir, dbt_local_server_port, local_task_db_path,
        command_exec_timeout_seconds
    )

    if testcase == "IDE":
        await _run_test_ide(smoke_test)
    else:
        raise Exception("Unsupported testcase type.")

    logging.info("All testcases are passed.")


def main(argv):
    del argv  # Unused.
    loop = asyncio.get_event_loop()
    with TemporaryDirectory() as temp_dir:
        loop.run_until_complete(entry_point(temp_dir))


if __name__ == "__main__":
    app.run(main)
