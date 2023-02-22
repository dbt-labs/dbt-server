import os
from unittest import TestCase

class DbtCoreTestBase(TestCase):
    """ A base class to setup local dbt core environments and delete after 
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
        os.environ["__DBT_WORKING_DIR"] = working_dir
        os.environ["DBT_PROFILES_DIR"] = profiles_dir

    def tearDown(self) -> None:
        del os.environ["__DBT_WORKING_DIR"]
        del os.environ["DBT_PROFILES_DIR"]
