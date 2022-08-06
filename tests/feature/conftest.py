import os

import pytest
import yaml
from dbt_server.server import app
from fastapi.testclient import TestClient
from fs import open_fs


client = TestClient(app)


@pytest.fixture
def test_client() -> TestClient:
    # Setup
    delete_working_dir = _create_working_dir()
    delete_profile_directory = _setup_dbt_profile()
    restore_environment_variables = _setup_environment_variables()

    yield TestClient(app)

    # Teardown
    delete_working_dir()
    _reset_database()
    delete_profile_directory()
    restore_environment_variables()


def _reset_database() -> None:
    """
    Reset the dbt-server application database

    dbt-server creates and migrates a sqlite database if needed when booted. So for now, we can take a naive approach of simply deleting the database file.
    """
    with open_fs(".") as root:
        if root.exists("sql_app.db"):
            root.remove("sql_app.db")


def _create_working_dir():
    """
    Create the working directory dbt-server.
    Return a function to delete that directory
    """
    with open_fs(".") as root:
        if not root.exists("working-dir"):
            root.makedir("working-dir")

    def _delete_working_dir():
        with open_fs(".") as root:
            if root.exists("working-dir"):
                root.removetree(
                    "working-dir",
                )

    return _delete_working_dir


def _setup_dbt_profile():
    dbt_profile_dict = {
        "test": {
            "target": "dev",
            "outputs": {
                "dev": {
                    "type": "duckdb",
                    "path": "./test.duckdb",
                    "schema": "test_schema",
                }
            },
        }
    }
    dbt_profile_yaml = yaml.dump(dbt_profile_dict)

    with open_fs(".") as root:
        if not root.exists("test-tmp"):
            root.makedir("test-tmp")
        if not root.exists("test-tmp/dbt_profiles.yml"):
            root.touch("test-tmp/dbt_profiles.yml")
        with root.open("test-tmp/dbt_profiles.yml", mode="w") as dbt_profile:

            dbt_profile.write(dbt_profile_yaml)

    def _delete_profile_directory():
        with open_fs(".") as root:
            if root.exists("test-tmp"):
                root.removetree("test-tmp")

    return _delete_profile_directory


def _setup_environment_variables():
    original_dbt_profiles_dir = os.getenv("DBT_PROFILES_DIR")
    original_dbt_profile_name = os.getenv("DBT_PROFILE_NAME")

    os.environ["DBT_PROFILES_DIR"] = "test-tmp"
    os.environ["DBT_PROFILE_NAME"] = "test"

    def _restore_environment_variables():
        if original_dbt_profiles_dir is not None:
            os.environ["DBT_PROFILES_DIR"] = original_dbt_profiles_dir
        if original_dbt_profile_name is not None:
            os.environ["DBT_PROFILE_NAME"] = original_dbt_profile_name

    return _restore_environment_variables
