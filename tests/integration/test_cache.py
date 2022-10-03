from fastapi.testclient import TestClient
from unittest.mock import patch
import unittest

from dbt_server.server import app, startup_cache_initialize
from dbt_server.state import LAST_PARSED
from dbt_server.exceptions import StateNotFoundException


class FakeManifest:
    pass


fake_manifest = FakeManifest()
client = TestClient(app)


class StartupCacheTest(unittest.TestCase):
    def setUp(self):
        LAST_PARSED.reset()

    def tearDown(self):
        LAST_PARSED.reset()

    @patch(
        "dbt_server.services.filesystem_service.get_latest_state_id",
        return_value="abc123",
    )
    @patch(
        "dbt_server.services.dbt_service.deserialize_manifest",
        return_value=fake_manifest,
    )
    def test_startup_cache_succeeds(self, mock_dbt, mock_fs):
        # Make sure it's not errantly cached
        assert LAST_PARSED.manifest is None

        startup_cache_initialize()

        # Make sure manifest is now cached
        mock_fs.assert_called_once_with(None)
        mock_dbt.assert_called_once_with("./working-dir/state-abc123/manifest.msgpack")
        assert LAST_PARSED.manifest is fake_manifest
        assert LAST_PARSED.state_id == "abc123"

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
        return_value="abc123",
    )
    @patch(
        "dbt_server.services.dbt_service.deserialize_manifest",
        side_effect=TypeError("bad"),
    )
    def test_startup_cache_fails_bad_manifest(self, mock_dbt, mock_fs):
        # Make sure it's not errantly cached
        assert LAST_PARSED.manifest is None

        startup_cache_initialize()

        # Make sure manifest is still not cached
        mock_fs.assert_called_once_with(None)
        mock_dbt.assert_called_once_with("./working-dir/state-abc123/manifest.msgpack")
        assert LAST_PARSED.manifest is None
        assert LAST_PARSED.state_id is None

    @patch(
        "dbt_server.services.filesystem_service.get_latest_state_id",
        return_value="abc123",
    )
    @patch(
        "dbt_server.services.filesystem_service.read_serialized_manifest",
        side_effect=StateNotFoundException(),
    )
    def test_startup_cache_fails_specified_state_is_missing(self, mock_dbt, mock_fs):
        # Make sure it's not errantly cached
        assert LAST_PARSED.manifest is None

        startup_cache_initialize()

        # Make sure manifest is still not cached
        mock_fs.assert_called_once_with(None)
        mock_dbt.assert_called_once_with("./working-dir/state-abc123/manifest.msgpack")
        assert LAST_PARSED.manifest is None
        assert LAST_PARSED.state_id is None
