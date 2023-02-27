import os
from importlib import util
from unittest import TestCase

DBT_POSTGRES_PACKAGE_NAME = "dbt.adapters.postgres"
DBT_SNOWFLAKE_PACKAGE_NAME = "dbt.adapters.snowflake"


def set_dbt_working_dir_env(working_dir: str):
    os.environ["__DBT_WORKING_DIR"] = working_dir


def set_dbt_profiles_dir_env(profiles_dir: str):
    os.environ["DBT_PROFILES_DIR"] = profiles_dir


class DbtCoreTestBase(TestCase):
    """A base class to setup local dbt core environments and delete after
    tests.
    Example:
        from tests.e2e.fixtures import Profiles
        class DerivedTest(DbtCoreTestBase):
            def setUp(self) -> None:
                self.set_envs("working_dir", Profiles.Postgres)

            def tearDown(self)-> None:
                # If you don't explicitly define tearDown in derived class,
                # DbtCoreTestBase.tearDown() will executed automatically.
                super().tearDown()
    """

    def set_envs(self, working_dir, profiles_dir) -> None:
        set_dbt_working_dir_env(working_dir)
        set_dbt_profiles_dir_env(profiles_dir)

    def tearDown(self) -> None:
        del os.environ["__DBT_WORKING_DIR"]
        del os.environ["DBT_PROFILES_DIR"]


def _is_packge_installed(package_name: str):
    """Returns if `package_name` is installed in python env."""
    return util.find_spec(package_name) is not None


def miss_postgres_adaptor_package():
    """Returns true if postgres adaptor isn't installed in python env."""
    return not _is_packge_installed(DBT_POSTGRES_PACKAGE_NAME)


def miss_snowflake_adaptor_package():
    """Returns true if snowflake adaptor isn't installed in python env."""
    return not _is_packge_installed(DBT_SNOWFLAKE_PACKAGE_NAME)
