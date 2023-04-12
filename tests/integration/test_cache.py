import os

from fastapi.testclient import TestClient
from unittest.mock import patch

from dbt_server.server import app, startup_cache_initialize
from dbt_server.state import LAST_PARSED
from dbt_server.exceptions import StateNotFoundException
from dbt_server.services.filesystem_service import DEFAULT_WORKING_DIR
from tests.e2e.helpers import DbtCoreTestBase

TEST_LATEST_STATE_ID = "abc123"


class FakeManifest:
    pass


fake_manifest = FakeManifest()
client = TestClient(app)


class StartupCacheTest(DbtCoreTestBase):
    def setUp(self):
        self.set_envs(DEFAULT_WORKING_DIR, "")
        LAST_PARSED.reset()

    def tearDown(self):
        super().tearDown()
        LAST_PARSED.reset()

    @patch(
        "dbt_server.services.filesystem_service.get_latest_state_id",
        return_value=TEST_LATEST_STATE_ID,
    )
    @patch(
        "dbt_server.services.filesystem_service.get_latest_project_path",
        return_value=None,
    )
    @patch("dbt_server.services.filesystem_service.get_size", return_value=1024)
    @patch(
        "dbt_server.services.dbt_service.deserialize_manifest",
        return_value=fake_manifest,
    )
    def test_startup_cache_succeeds(
        self,
        mock_dbt,
        mock_fs_get_size,
        mock_fs_get_latest_project_path,
        mock_fs_get_latest_state_id,
    ):
        # Make sure it's not errantly cached
        assert LAST_PARSED.manifest is None

        startup_cache_initialize()

        # Make sure manifest is now cached
        expected_path = os.path.abspath(
            f"./working-dir/state-{TEST_LATEST_STATE_ID}/manifest.msgpack"
        )
        mock_fs_get_latest_state_id.assert_called_once_with(None)
        mock_fs_get_size.assert_called_once_with(expected_path)
        mock_dbt.assert_called_once_with(expected_path)
        assert LAST_PARSED.manifest is fake_manifest
        assert LAST_PARSED.state_id == TEST_LATEST_STATE_ID
        assert LAST_PARSED.manifest_size == 1024

    @patch(
        "dbt_server.services.filesystem_service.get_latest_state_id", return_value=None
    )
    def test_startup_cache_fails_no_state(self, mock_fs):
        # Make sure it's not errantly cached
        assert LAST_PARSED.manifest is None

        startup_cache_initialize()

        # Make sure manifest is still not cached
        mock_fs.assert_called_once_with(None)
        assert LAST_PARSED.manifest is None
        assert LAST_PARSED.state_id is None

    @patch(
        "dbt_server.services.filesystem_service.get_latest_state_id",
        return_value=TEST_LATEST_STATE_ID,
    )
    @patch(
        "dbt_server.services.filesystem_service.get_latest_project_path",
        return_value=None,
    )
    @patch(
        "dbt_server.services.dbt_service.deserialize_manifest",
        side_effect=TypeError("bad"),
    )
    def test_startup_cache_fails_bad_manifest(
        self, mock_dbt, mock_get_latest_project_path, mock_fs
    ):
        # Make sure it's not errantly cached
        assert LAST_PARSED.manifest is None

        startup_cache_initialize()

        # Make sure manifest is still not cached
        mock_fs.assert_called_once_with(None)
        mock_dbt.assert_called_once_with(
            os.path.abspath(
                f"./working-dir/state-{TEST_LATEST_STATE_ID}/manifest.msgpack"
            )
        )
        assert LAST_PARSED.manifest is None
        assert LAST_PARSED.state_id is None

    @patch(
        "dbt_server.services.filesystem_service.get_latest_state_id",
        return_value=TEST_LATEST_STATE_ID,
    )
    @patch(
        "dbt_server.services.filesystem_service.get_latest_project_path",
        return_value=None,
    )
    @patch(
        "dbt_server.services.filesystem_service.read_serialized_manifest",
        side_effect=StateNotFoundException(),
    )
    def test_startup_cache_fails_specified_state_is_missing(
        self, mock_dbt, mock_get_latest_project_path, mock_fs
    ):
        # Make sure it's not errantly cached
        assert LAST_PARSED.manifest is None

        startup_cache_initialize()

        # Make sure manifest is still not cached
        mock_fs.assert_called_once_with(None)
        mock_dbt.assert_called_once_with(
            os.path.abspath(
                f"./working-dir/state-{TEST_LATEST_STATE_ID}/manifest.msgpack"
            )
        )
        assert LAST_PARSED.manifest is None
        assert LAST_PARSED.state_id is None
