import shutil
import tempfile
from unittest import TestCase

from dbt_server.state import StateController, LAST_PARSED
from dbt_server.views import DBTCommandArgs
from .helpers import profiles_dir
from .fixtures import Profiles
import os


class StateControllerTestCase(TestCase):
    """
    Full functionality test class using a real dbt project manifest
    """

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        os.environ["__DBT_WORKING_DIR"] = self.temp_dir.name

        self.state_id = "test123"
        self.state_dir = f"{self.temp_dir.name}/state-{self.state_id}"
        shutil.copytree("tests/e2e/fixtures/test-project", self.state_dir)

    def tearDown(self):
        del os.environ["__DBT_WORKING_DIR"]
        self.temp_dir.cleanup()
        LAST_PARSED.reset()

    def test_load_state(self):
        # CURRENTLY USING SNOWFLAKE DUE TO DBT VERSION MISMATCH WITH POSTGRES
        with profiles_dir(Profiles.Snowflake):
            args = DBTCommandArgs(command=["run"], state_id=self.state_id)
            result = StateController.load_state(args)

        assert result.state_id == self.state_id
        assert result.config.profile_name == "user"
        assert result.config.target_name == "default"
        assert result.config.user_config is not None
        assert result.config.credentials is not None

        # Should not cache on load
        assert LAST_PARSED.lookup(self.state_id) is None
