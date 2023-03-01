# Script that sends http request to local dbt-server for testing purpose.
# - It will setup the dbt-server and dbt project, user needs to install correct
#   dbt-core package, adaptor packages and environment(e.g. local postgres db).
# - Environment variables of data warehouse connection are required to run
#   tests, if not specified the test will be skipped. See test comment below to
#   check required env vars.
# - Requests are sent one by one, it won't touch concurrency issue.
# TODO(dichen): switch to async end point for parse.
# TODO(dichen): until bug fixed, we must run dbt deps before sending init parse
# command, hence for short term, we checked in dbt_packages fixtures but we
# should remove them later.
# What can be done in future?
# - More commands for other use cases.
# - Switch parse endpoint to async endpoint.
# - Test other endpoints, e.g sync endpoint.
#
# Example run:
# # Under root directory.
# pytest -v tests/e2e/smoke_test.py

from dbt_server.models import TaskState
import logging
import os
from os import path
import pytest
from shutil import rmtree
from tempfile import TemporaryDirectory
from tests.e2e.smoke_test_utils import copy_jaffle_shop_fixture
from tests.e2e.smoke_test_utils import DbtServerSmokeTest
from tests.e2e.smoke_test_utils import TESTING_FIXTURE
from tests.e2e.smoke_test_utils import parse_placeholder_string
from tests.e2e.smoke_test_utils import replace_placeholders
from tests.e2e.smoke_test_utils import read_testcase_file
from time import time
from time import sleep
from threading import Thread
from unittest import TestCase
import uvicorn

#
# Required env vars for different adaptors. If env vars are missing for specific
# adaptor, tests will be skipped.
#

POSTGRES_PROFILE_PLACEHOLDER_REFERENCE = {
    "%USER": os.getenv("SMOKE_TEST_POSTGRES_USER", ""),
    "%PASSWORD": os.getenv("SMOKE_TEST_POSTGRES_PASSWORD", ""),
    "%PORT": os.getenv("SMOKE_TEST_POSTGRES_PORT", "5432"),
}

SNOWFLAKE_PROFILE_PLACEHOLDER_REFERENCE = {
    "%ACCOUNT": os.getenv("SMOKE_TEST_SNOWFLAKE_ACCOUNT", ""),
    "%DATABASE": os.getenv("SMOKE_TEST_SNOWFLAKE_DATABASE", ""),
    "%PASSWORD": os.getenv("SMOKE_TEST_SNOWFLAKE_PASSWORD", ""),
    "%ROLE": os.getenv("SMOKE_TEST_SNOWFLAKE_ROLE", ""),
    "%SCHEMA": os.getenv("SMOKE_TEST_SNOWFLAKE_SCHEMA", ""),
    "%USER": os.getenv("SMOKE_TEST_SNOWFLAKE_USER", ""),
    "%WAREHOUSE": os.getenv("SMOKE_TEST_SNOWFLAKE_WAREHOUSE", ""),
}


# State directory has manifest.json file to be used as --state.
JAFFLE_SHOP_STATE_DIR = "init_target"
PROFILE_YML = "profiles.yml"
POSTGRES_PROFILES_TEMPLATE_PATH = "profiles/postgres/profiles_template.yml"
SNOWFLAKE_PROFILES_TEMPLATE_PATH = "profiles/snowflake/profiles_template.yml"

TEST_LOCAL_DBT_SERVER_PORT = 8580
TEST_LOCAL_TASK_DB_PATH = "working-dir/sql_app.db"
TEST_LOCAL_STATE_ID_FILE = "working-dir/latest-state-id.txt"
TEST_LOCAL_PROJECT_PATH_FILE = "working-dir/latest-project-path.txt"

DBT_SERVER_WAIT_SECONDS = int(os.getenv("SMOKE_TEST_DBT_SERVER_START_UP_SECONDS", 5))


dbt_server_start_timestamp_seconds = None


def _start_dbt_server():
    """Starts dbt server locally."""
    # If state file or project path file exists, dbt-server will try to
    # initialize local manifest cache, it may cause dbt-server crash.
    # We are not able to config those file pathes hence just delete them
    # now.
    # TODO(dichen): consider introduce flag to control those file pathes.
    if os.path.isfile(TEST_LOCAL_STATE_ID_FILE):
        os.remove(TEST_LOCAL_STATE_ID_FILE)
    if os.path.isfile(TEST_LOCAL_PROJECT_PATH_FILE):
        os.remove(TEST_LOCAL_PROJECT_PATH_FILE)
    # Store start timestamps to make sure server is ready before sending out
    # any commands.
    global dbt_server_start_timestamp_seconds
    dbt_server_start_timestamp_seconds = time()
    logging.info(f"Start dbt-server locally, port = {TEST_LOCAL_DBT_SERVER_PORT}")
    uvicorn.run(
        "dbt_server.server:app", port=TEST_LOCAL_DBT_SERVER_PORT, loop="asyncio"
    )


def start_dbt_server() -> None:
    Thread(target=_start_dbt_server, daemon=True).start()


start_dbt_server()


class TestJaffleShopBase(TestCase):
    def wait_dbt_server(self):
        """Waits until dbt server is ready. It will block current working
        threads."""
        logging.info("Wait dbt-server setup.")
        while True:
            now = time()
            global dbt_server_start_timestamp_seconds
            if (
                dbt_server_start_timestamp_seconds
                and now > dbt_server_start_timestamp_seconds + DBT_SERVER_WAIT_SECONDS
            ):
                logging.info("Dbt-server is ready.")
                return
            sleep(DBT_SERVER_WAIT_SECONDS)

    def materialize_profiles_yml(self) -> None:
        """Materializes profiles.yml in testing folder.
        Derived class should override this function."""
        raise NotImplementedError("Not implemented.")

    def setUp(self) -> None:
        self.temp_dir = TemporaryDirectory().name
        copy_jaffle_shop_fixture(self.temp_dir)
        self.materialize_profiles_yml()
        self.smoke_test = DbtServerSmokeTest(
            self.temp_dir, TEST_LOCAL_DBT_SERVER_PORT, TEST_LOCAL_TASK_DB_PATH
        )
        self.wait_dbt_server()

    def write_profile(self, profile_content: str):
        """Writes `profile_content` to profile path."""
        with open(path.join(self.temp_dir, PROFILE_YML), "w") as output_file:
            output_file.write(profile_content)

    def tearDown(self) -> None:
        rmtree(self.temp_dir)

    def get_jaffle_shop_override_placeholder_reference(self):
        """Returns common placeholder refenrece for jaffle shop testing project."""
        return {
            "%STATE_DIR": f"{self.temp_dir}/{JAFFLE_SHOP_STATE_DIR}",
            "%SELECTOR": "test_selector",
            "%VARIABLE": "test_var: 1",
            "%MODEL": "customers",
            "%MACRO_NAME": "test_macro",
            "%MACRO_ARGS": "int_value: 1",
        }


@pytest.mark.skipif(
    "" in POSTGRES_PROFILE_PLACEHOLDER_REFERENCE.values(),
    reason=f"""Smoke test for postgres adaptor requires env vars set for placeholders = {
                        str(list(POSTGRES_PROFILE_PLACEHOLDER_REFERENCE.keys()))}""",
)
class TestJaffleShopPostgresBase(TestJaffleShopBase):
    def materialize_profiles_yml(self) -> None:
        with open(
            path.join(TESTING_FIXTURE, POSTGRES_PROFILES_TEMPLATE_PATH), "r"
        ) as template_file:
            self.write_profile(
                parse_placeholder_string(
                    POSTGRES_PROFILE_PLACEHOLDER_REFERENCE, template_file.read()
                )
            )

    def setUp(self) -> None:
        super().setUp()
        # Trigger parse command to setup project path.
        self.smoke_test.parse(self.temp_dir)

    def tearDown(self) -> None:
        super().tearDown()


@pytest.mark.skipif(
    "" in SNOWFLAKE_PROFILE_PLACEHOLDER_REFERENCE.values(),
    reason=f"""Smoke test for snowflake adaptor requires env vars set for placeholders = {
                        str(list(SNOWFLAKE_PROFILE_PLACEHOLDER_REFERENCE.keys()))}""",
)
class TestJaffleShopSnowflakeBase(TestJaffleShopBase):
    def materialize_profiles_yml(self) -> None:
        with open(
            path.join(TESTING_FIXTURE, SNOWFLAKE_PROFILES_TEMPLATE_PATH), "r"
        ) as template_file:
            self.write_profile(
                parse_placeholder_string(
                    SNOWFLAKE_PROFILE_PLACEHOLDER_REFERENCE, template_file.read()
                )
            )

    def setUp(self) -> None:
        super().setUp()
        # Trigger parse command to setup project path.
        self.smoke_test.parse(self.temp_dir)

    def tearDown(self) -> None:
        super().tearDown()


@pytest.mark.skip("Test is slow. If you want to manually run, comment pytest " "mark.")
# TODO(dichen): Consider add pytest mark to testcases so we can group them
# together.
class TestIde(TestJaffleShopBase):
    IDE_COMMAND_SUCCESS_FILE = "tests/e2e/testcases/ide_commands.txt"
    IDE_COMMAND_FAILURE_FILE = "tests/e2e/testcases/ide_commands_failure.txt"

    def _test_success(self) -> None:
        success_commands = read_testcase_file(self.IDE_COMMAND_SUCCESS_FILE)
        for command in success_commands:
            command_list = command.split()
            replace_placeholders(
                self.get_jaffle_shop_override_placeholder_reference(), command_list
            )
            self.smoke_test.run_async_testcase(command_list, TaskState.FINISHED)

    def _test_error(self) -> None:
        failure_command = read_testcase_file(self.IDE_COMMAND_FAILURE_FILE)
        for command in failure_command:
            command_list = command.split()
            replace_placeholders(
                self.get_jaffle_shop_override_placeholder_reference(), command_list
            )
            self.smoke_test.run_async_testcase(command_list, TaskState.ERROR)


class TestIdePostgres(TestIde, TestJaffleShopPostgresBase):
    def test_success(self) -> None:
        self._test_success()

    def test_error(self) -> None:
        self._test_error()


class TestIdeSnowflake(TestIde, TestJaffleShopSnowflakeBase):
    def test_success(self) -> None:
        self._test_success()

    def test_error(self) -> None:
        self._test_error()


class TestSimple(TestJaffleShopBase):
    SIMPLE_COMMAND_SUCCESS_FILE = "tests/e2e/testcases/simple_commands.txt"
    SIMPLE_COMMAND_FAILURE_FILE = "tests/e2e/testcases/simple_commands_failure.txt"

    def _test_success(self) -> None:
        success_commands = read_testcase_file(self.SIMPLE_COMMAND_SUCCESS_FILE)
        for command in success_commands:
            command_list = command.split()
            replace_placeholders(
                self.get_jaffle_shop_override_placeholder_reference(), command_list
            )
            self.smoke_test.run_async_testcase(command_list, TaskState.FINISHED)

    def _test_error(self) -> None:
        failure_command = read_testcase_file(self.SIMPLE_COMMAND_FAILURE_FILE)
        for command in failure_command:
            command_list = command.split()
            replace_placeholders(
                self.get_jaffle_shop_override_placeholder_reference(), command_list
            )
            self.smoke_test.run_async_testcase(command_list, TaskState.ERROR)


class TestSimplePostgres(TestSimple, TestJaffleShopPostgresBase):
    def test_success(self) -> None:
        self._test_success()

    def test_error(self) -> None:
        self._test_error()


class TestSimpleSnowflake(TestSimple, TestJaffleShopSnowflakeBase):
    def test_success(self) -> None:
        self._test_success()

    def test_error(self) -> None:
        self._test_error()
