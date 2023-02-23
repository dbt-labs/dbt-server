# Script that sends http request to local dbt-server for testing purpose.
# - It doesn't setup the dbt-server and requires user to start dbt-server
#   locally, the profile should match testing project located
#   in tests/e2e/fixtures/testing/jaffle_shop(i.e. profile name is jaffle_shop).
# - It will copy the jaffle_shop project into a temporary directory and test.
# - Requests are sent one by one, it won't touch concurrency issue.
# Notice:
# - Until bug fixed, you need to run dbt deps in tests/e2e/fixtures/jaffle_shop
#   first, otherwise the initial parse will fail.
# TODO(dichen): switch to async end point for parse.
# What can be done in future?
# - More commands for other use cases.
# - Switch parse endpoint to async endpoint.
# - Test other endpoints, e.g sync endpoint.
#
# Example run:
# # Under root directory.
# python -m unittest discover -s tests/e2e -p "smoke*.py"

import asyncio
from dbt_server.models import Task
from dbt_server.models import TaskState
import logging
from os import path
import pytest
from requests import post
from shutil import copytree
from shutil import rmtree
from sqlalchemy import create_engine
from sqlalchemy import select
from time import time
from typing import List
from unittest import IsolatedAsyncioTestCase

TESTING_FIXTURE = "tests/e2e/fixtures"
JAFFLE_SHOP_DIR = "jaffle_shop"
# State directory has manifest.json file to be used as --state.
JAFFLE_SHOP_STATE_DIR = "init_target"

LOCAL_URL = "http://127.0.0.1"
# How long we will get task status from task db periodically.
ASYNC_POLLING_SECONDS = 1
ASYNC_DBT_URL = "async/dbt"
TEST_LOCAL_DBT_SERVER_PORT = 8580
TEST_LOCAL_TASK_DB_PATH = "working-dir/sql_app.db"
TEMP_DIR = "/tmp/dbt_project"


class DbtServerSmokeTest:
    """ DbtServerSmokeTest provides testing helpers interacting and verifying 
    dbt-server for caller.

    This class is thread-safe.
    """

    def __init__(self, dbt_project_dir: str, dbt_local_server_port: int,
                 local_task_db_path: str):
        """ Initializes smoke test environment.

        Args:
            dbt_project_dir: directory to dbt project.
            dbt_local_server_port: local dbt-server port.
            local_task_db_path: path to local task sqlite db file.
        """
        self.dbt_project_dir = dbt_project_dir
        self.dbt_local_server_port = dbt_local_server_port
        self.task_db_engine = create_engine(
            f"sqlite:///{local_task_db_path}",
            connect_args={"check_same_thread": False},
        )

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
        """ Sends a http post request to make dbt server parse dbt project.

        Raises:
            Exception: if response status code is not 200.
        """
        self.post_request("parse", {
            "project_path": self.dbt_project_dir
        })

    async def wait_async_exec(
            self, task_id: str,
            command_exec_timeout_seconds: int = 60) -> TaskState:
        """ Waits task with `task_id` to be finished. Returns task final state.
        Raises Exception if task is not finished after given timeout config.

        Args:
            task_id: string of task id, we will lookup task status in task db
                according to task_id.
            command_exec_timeout_seconds: timeout seconds of each command.
        """
        start_timestamp_seconds = time()
        while (time()
               < start_timestamp_seconds + command_exec_timeout_seconds):
            stmt = select(Task).where(Task.task_id == task_id)
            with self.task_db_engine.connect() as conn:
                tasks = list(conn.execute(stmt))
                if len(tasks) == 1 and tasks[0].state in [
                    TaskState.FINISHED, TaskState.ERROR
                ]:
                    return tasks[0].state
            await asyncio.sleep(ASYNC_POLLING_SECONDS)
        raise Exception(
            f"Task {task_id} is not finished after {command_exec_timeout_seconds}s")

    async def run_async_testcase(self, command_list: List[str],
                                 expected_db_task_status: TaskState,
                                 command_exec_timeout_seconds: int = 60) -> None:
        """ Sends post request to async endpoints with `command_list` as request
        body. If execution is timeout after `command_exec_timeout_seconds` 
        seconds or task status is not `expected_db_task_status` or local log 
        file doesn't exist but status is finished, raises Exceptions.
        Args:
            command_list: list string of commands, e.g. [build, -h].
            expected_db_task_status: task state we expect.
            command_exec_timeout_seconds: timeout seconds of each command.
        """
        logging.info(
            f"Start async test case {str(command_list)}, expect {expected_db_task_status}")
        resp = self.post_request(ASYNC_DBT_URL, {
            "command": command_list
        })
        task_id = resp["task_id"]
        task_status = await self.wait_async_exec(task_id,
                                                 command_exec_timeout_seconds)
        if task_status != expected_db_task_status:
            raise Exception(
                f'Error task_id={task_id}, status != {expected_db_task_status}, error = {resp["error"]}')
        # Ensure log file is created when task is finished but won't check
        # details.
        if (task_status == TaskState.FINISHED
            and not path.isfile(path.join(self.dbt_project_dir,
                                          resp["log_path"]))):
            raise Exception(f"Can't find log file for task_id={task_id}")


def copy_jaffle_shop_fixture(dir):
    """ Copy jaffle shop test fixture to `dir`.
    """
    copytree(path.join(TESTING_FIXTURE, JAFFLE_SHOP_DIR),
             dir, dirs_exist_ok=True)


def read_testcase_file(path: str) -> List[str]:
    """ Reads testcase file and filter out empty line or starts with # which 
    is comment.
    """
    with open(path, "r") as f:
        return list(filter(
            lambda line: (not line.startswith("#")) and line,
            [line.strip() for line in f.readlines()])
        )


def parse_command_string(placeholder_reference: dict,
                         raw_command: str) -> List[str]:
    """ Parses input `raw_command` based on `placeholder_reference`,
    return list of string commands.

    Args:
        placeholder_reference: key is placeholder name, value is replaced value,
            all commands will be replaced with value according to this.
        raw_command: string of input test raw command, may have placeholder.

    Example:
        parse_command_string({"%key": "value"}, "command %key) will return
        ["command", "value"].
    """
    commands = raw_command.split()
    for index in range(len(commands)):
        for placeholder, value in placeholder_reference.items():
            if commands[index] != placeholder:
                continue
            commands[index] = value
    return commands


class TestJaffleShopBase(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.temp_dir = TEMP_DIR
        copy_jaffle_shop_fixture(self.temp_dir)
        self.smoke_test = DbtServerSmokeTest(self.temp_dir,
                                             TEST_LOCAL_DBT_SERVER_PORT,
                                             TEST_LOCAL_TASK_DB_PATH
                                             )
        # Trigger parse command to setup project path.
        self.smoke_test.parse()

    def tearDown(self) -> None:
        rmtree(self.temp_dir)

    def get_jaffle_shop_override_placeholder_reference(self):
        """ Returns common placeholder refenrece for jaffle shop testing 
        project. 
        """
        return {
            "%STATE_DIR": f"{self.temp_dir}/{JAFFLE_SHOP_STATE_DIR}",
            "%SELECTOR": "test_selector",
            "%VARIABLE": "test_var: 1",
            "%MODEL": "customers",
            "%MACRO": "test_macro",
            "%MACRO_ARGS": "int_value: 1"
        }


@pytest.mark.skip("Test is slow. If you want to manually run, comment pytest "
                  "mark.")
# TODO(dichen): Consider add pytest mark to testcases so we can group them.
class TestIde(TestJaffleShopBase):
    IDE_COMMAND_SUCCESS_FILE = "tests/e2e/testcases/ide_commands.txt"
    IDE_COMMAND_FAILURE_FILE = "tests/e2e/testcases/ide_commands_failure.txt"

    async def test_success(self) -> None:
        success_commands = read_testcase_file(self.IDE_COMMAND_SUCCESS_FILE)
        for command in success_commands:
            command_list = parse_command_string(
                self.get_jaffle_shop_override_placeholder_reference(), command)
            await self.smoke_test.run_async_testcase(command_list,
                                                     TaskState.FINISHED)

    async def test_error(self) -> None:
        failure_command = read_testcase_file(self.IDE_COMMAND_FAILURE_FILE)
        for command in failure_command:
            command_list = parse_command_string(
                self.get_jaffle_shop_override_placeholder_reference(), command)
            await self.smoke_test.run_async_testcase(command_list,
                                                     TaskState.ERROR)


class TestSimple(TestJaffleShopBase):
    SIMPLE_COMMAND_SUCCESS_FILE = "tests/e2e/testcases/simple_commands.txt"
    SIMPLE_COMMAND_FAILURE_FILE = "tests/e2e/testcases/simple_commands_failure.txt"

    async def test_success(self) -> None:
        success_commands = read_testcase_file(self.SIMPLE_COMMAND_SUCCESS_FILE)
        for command in success_commands:
            command_list = parse_command_string(
                self.get_jaffle_shop_override_placeholder_reference(), command)
            await self.smoke_test.run_async_testcase(command_list,
                                                     TaskState.FINISHED)

    async def test_error(self) -> None:
        failure_command = read_testcase_file(self.SIMPLE_COMMAND_FAILURE_FILE)
        for command in failure_command:
            command_list = parse_command_string(
                self.get_jaffle_shop_override_placeholder_reference(), command)
            await self.smoke_test.run_async_testcase(command_list,
                                                     TaskState.ERROR)
