from unittest import TestCase

from dbt_server.state import StateController, LAST_PARSED
from dbt_server.views import dbtCommandArgs
from .helpers import profiles_dir
from .fixtures import Profiles
import os


class StateControllerTestCase(TestCase):
  """
    Full functionality test class using a real dbt project manifest
  """
  def setUp(self):
    current_dir = os.getcwd()
    os.environ["__DBT_WORKING_DIR"] = os.path.join(current_dir, "tests/e2e/fixtures/test-working-dir")

  def tearDown(self):
    del os.environ["__DBT_WORKING_DIR"]
  
  def test_load_state(self):
    # CURRENTLY USING SNOWFLAKE DUE TO DBT VERSION MISMATCH WITH POSTGRES
    with profiles_dir(Profiles.Snowflake):
      args = dbtCommandArgs(command=['run'], state_id="test123")
      result = StateController.load_state(args)

    assert result.state_id == "test123"
    assert result.config.profile_name == "user"
    assert result.config.target_name == "default"
    assert result.config.user_config is not None
    assert result.config.credentials is not None

    # Should not cache on load
    assert LAST_PARSED.lookup("test123") is None

