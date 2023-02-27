import shutil
import tempfile

from dbt_server.state import StateController, LAST_PARSED
from dbt_server.views import DbtCommandArgs
from tests.e2e.helpers import DbtCoreTestBase
from tests.e2e.fixtures import Profiles
import pytest
from tests.e2e.helpers import miss_snowflake_adaptor_package


@pytest.mark.skipif(
    miss_snowflake_adaptor_package(),
    reason="This test requires dbt-snowflake installed.",
)
class StateControllerTestCase(DbtCoreTestBase):
    """
    Full functionality test class using a real dbt project manifest
    """

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.set_envs(self.temp_dir.name, Profiles.Snowflake)

        self.state_id = "test123"
        self.state_dir = f"{self.temp_dir.name}/state-{self.state_id}"
        shutil.copytree("tests/e2e/fixtures/test-project", self.state_dir)

    def tearDown(self):
        super().tearDown()
        self.temp_dir.cleanup()
        LAST_PARSED.reset()

    def test_load_state(self):
        # CURRENTLY USING SNOWFLAKE DUE TO DBT VERSION MISMATCH WITH POSTGRES
        args = DbtCommandArgs(command=["run"], state_id=self.state_id)
        result = StateController.load_state(args)

        assert result.state_id == self.state_id
        assert result.config.profile_name == "user"
        assert result.config.target_name == "default"
        assert result.config.user_config is not None
        assert result.config.credentials is not None

        # Should not cache on load
        assert LAST_PARSED.lookup(self.state_id) is None
