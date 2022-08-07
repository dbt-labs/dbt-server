import os
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Generator

import yaml
from clients.filesystem import FilesystemClient
from dbt_server.server import app
from fastapi.testclient import TestClient
from tests.context import temp_dir


@dataclass
class FeatureTestContext:
    client: TestClient


@contextmanager
def test_context() -> Generator[FeatureTestContext, None, None]:
    # Setup
    delete_working_dir = _create_working_dir()

    with temp_dir() as dbt_profiles_dir:
        _setup_dbt_profile(dbt_profiles_dir, "test")
        restore_environment_variables = _setup_environment_variables(
            dbt_profiles_dir, "test"
        )

        context = FeatureTestContext(client=TestClient(app))
        yield context

        restore_environment_variables()

    # Teardown
    delete_working_dir()
    _reset_database()


def _reset_database() -> None:
    """
    Reset the dbt-server application database

    dbt-server creates and migrates a sqlite database if needed when booted. So for now, we can take a naive approach of simply deleting the database file.
    """
    root = FilesystemClient(".")
    root.delete("sql_app.db")


def _create_working_dir():
    """
    Create the working directory dbt-server.
    Return a function to delete that directory
    """
    root = FilesystemClient(".")
    # Just make an empty file so that working-dir is created along with it
    root.put("working-dir/empty.txt", "")

    def _delete_working_dir():
        root.delete("working-dir", force=True)

    return _delete_working_dir


def _setup_dbt_profile(dir: str, profile_name: str) -> None:
    """
    Sets up a dbt profile in a temporary directory.
    Returns the path to that directory.
    """
    dbt_profile_dict = {
        profile_name: {
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

    dbt_directory_client = FilesystemClient(dir)
    dbt_directory_client.put("profiles.yml", dbt_profile_yaml)


def _setup_environment_variables(dbt_profile_dir: str, profile_name: str):
    original_dbt_profiles_dir = os.getenv("DBT_PROFILES_DIR")
    original_dbt_profile_name = os.getenv("DBT_PROFILE_NAME")

    os.environ["DBT_PROFILES_DIR"] = dbt_profile_dir
    os.environ["DBT_PROFILE_NAME"] = profile_name

    def _restore_environment_variables():
        if original_dbt_profiles_dir is not None:
            os.environ["DBT_PROFILES_DIR"] = original_dbt_profiles_dir
        if original_dbt_profile_name is not None:
            os.environ["DBT_PROFILE_NAME"] = original_dbt_profile_name

    return _restore_environment_variables
