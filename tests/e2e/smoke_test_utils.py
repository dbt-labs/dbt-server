from dbt_server.models import Task
from dbt_server.models import TaskState
from sqlalchemy import create_engine
from sqlalchemy import select
import logging
from requests import post
from shutil import copytree
from os import path
from tests.e2e.helpers import set_dbt_profiles_dir_env
from time import time
from time import sleep
from typing import List

LOCAL_URL = "http://127.0.0.1"
# How long we will get task status from task db periodically.
POLLING_SECONDS = 1

TESTING_FIXTURE = "tests/e2e/fixtures"
JAFFLE_SHOP_DIR = "jaffle_shop"

ASYNC_DBT_URL = "async/dbt"


class DbtServerSmokeTest:
    """DbtServerSmokeTest provides testing helpers interacting and verifying
    dbt-server for caller.

    This class is thread-safe."""

    def __init__(
        self, dbt_project_dir: str, dbt_local_server_port: int, local_task_db_path: str
    ):
        """Initializes smoke test environment.

        Args:
            dbt_project_dir: directory to dbt project.
            dbt_local_server_port: local dbt-server port.
            local_task_db_path: path to local task sqlite db file."""
        self.dbt_project_dir = dbt_project_dir
        self.dbt_local_server_port = dbt_local_server_port
        self.task_db_engine = create_engine(
            f"sqlite:///{local_task_db_path}",
            connect_args={"check_same_thread": False},
        )

    def post_request(self, url_path: str, body_obj: dict) -> dict:
        """Sends a http post request to local dbt server with given `url_path`
        and json `body_obj`, the content type will always be json. Returns
        http response json body.

        Args:
            url_path: string of local dbt server url, DON'T add prefix /.
                Correct example: async/get.
            body_obj: dict of json body.

        Raises:
            Exception: if response status code is not 200."""
        url = f"{LOCAL_URL}:{self.dbt_local_server_port}/{url_path}"
        resp = post(url, json=body_obj)
        if resp.status_code != 200:
            logging.error(f"Request to {url_path} failed {resp.status_code}.")
            logging.error(f"body = {str(body_obj)}")
            raise Exception(f"Request failure with code = {resp.status_code}")
        return resp.json()

    def parse(self, profile_dir: str = None) -> None:
        """Sends a http post request to make dbt server parse dbt project.

        Args:
            profile_dir: string to directory of profiles.yml, if it's set env
                var will be set and dbt-server will try to get profiles from
                that.

        Raises:
            Exception: if response status code is not 200."""
        if profile_dir:
            set_dbt_profiles_dir_env(profile_dir)
        self.post_request("parse", {"project_path": self.dbt_project_dir})

    def wait_async_exec(
        self, task_id: str, command_exec_timeout_seconds: int = 60
    ) -> TaskState:
        """Waits task with `task_id` to be finished. Returns task final state.
        Raises Exception if task is not finished after given timeout config.

        Args:
            task_id: string of task id, we will lookup task status in task db
                according to task_id.
            command_exec_timeout_seconds: timeout seconds of each command."""
        start_timestamp_seconds = time()
        while time() < start_timestamp_seconds + command_exec_timeout_seconds:
            stmt = select(Task).where(Task.task_id == task_id)
            with self.task_db_engine.connect() as conn:
                tasks = list(conn.execute(stmt))
                if len(tasks) == 1 and tasks[0].state in [
                    TaskState.FINISHED,
                    TaskState.ERROR,
                ]:
                    return tasks[0].state
            sleep(POLLING_SECONDS)
        raise Exception(
            f"Task {task_id} is not finished after {command_exec_timeout_seconds}s"
        )

    def run_async_testcase(
        self,
        command_list: List[str],
        expected_db_task_status: TaskState,
        command_exec_timeout_seconds: int = 60,
    ) -> None:
        """Sends post request to async endpoints with `command_list` as request
        body. If execution is timeout after `command_exec_timeout_seconds`
        seconds or task status is not `expected_db_task_status` or local log
        file doesn't exist but status is finished, raises Exceptions.
        Args:
            command_list: list string of commands, e.g. [build, -h].
            expected_db_task_status: task state we expect.
            command_exec_timeout_seconds: timeout seconds of each command."""
        logging.info(
            f"Start async test case {str(command_list)}, expect {expected_db_task_status}"
        )
        resp = self.post_request(ASYNC_DBT_URL, {"command": command_list})
        task_id = resp["task_id"]
        task_status = self.wait_async_exec(task_id, command_exec_timeout_seconds)
        if task_status != expected_db_task_status:
            raise Exception(
                f'Error task_id={task_id}, status != {expected_db_task_status}, error = {resp["error"]}'
            )
        # Ensure log file is created when task is finished but won't check
        # details.
        if task_status == TaskState.FINISHED and not path.isfile(
            path.join(self.dbt_project_dir, resp["log_path"])
        ):
            raise Exception(f"Can't find log file for task_id={task_id}")


def copy_jaffle_shop_fixture(dir: str):
    """Copy jaffle shop test fixture to `dir`."""
    copytree(path.join(TESTING_FIXTURE, JAFFLE_SHOP_DIR), dir, dirs_exist_ok=True)


def read_testcase_file(path: str) -> List[str]:
    """Reads testcase file and filter out empty line or starts with # which
    is comment."""
    with open(path, "r") as f:
        return list(
            filter(
                lambda line: (not line.startswith("#")) and line,
                [line.strip() for line in f.readlines()],
            )
        )


def parse_placeholder_string(placeholder_reference: dict, input_string: str) -> str:
    """Parses input `input_string` based on `placeholder_reference`,
    return parsed string.

    Args:
        placeholder_reference: key is placeholder name, value is replaced value,
            input string will be replaced with value according to this.
        input_string: string that may have placeholder.

    Example:
        parse_placeholder_string({"%key": "value"}, "command %key") will return
        "command value"."""
    for placeholder, value in placeholder_reference.items():
        input_string = input_string.replace(placeholder, value)
    return input_string


def replace_placeholders(
    placeholder_reference: dict, input_string_list: List[str]
) -> None:
    """Similar to parse_placeholder_string, replaces input `input_string_list`
    based on `placeholder_reference`.

    Example:
        parse_placeholder_string({"%key": "value"}, ["command", "%key"]) will
        make input_string_list = ["command", "value"]."""

    for index in range(len(input_string_list)):
        for placeholder, value in placeholder_reference.items():
            input_string_list[index] = input_string_list[index].replace(
                placeholder, value
            )
