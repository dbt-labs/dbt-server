from dbt_server.exceptions import StateNotFoundException
from dbt_server.state import LAST_PARSED
from dbt_server.state import CachedManifest
from dbt_server.state import StateController
from dbt_server.views import ParseArgs
from unittest import TestCase
from unittest import mock

TEST_STATE_ID = "test_state"
TEST_PROJECT_PATH = "test_project_path"
TEST_ROOT_PATH = "test_root_path"
TEST_MANIFEST_FILE = "test_root_path/manifest.msgpack"
TEST_PARTIAL_PARSE_FILE = "partial.msgpack"
TEST_MANIFEST = "test_manifest"
TEST_MANIFEST_SIZE = 10
TEST_CONFIG = "test_config"
TEST_SQL_PARSER = "test_sql_parse"
TEST_ARGS = 1
TEST_VALUE = 10
TEST_QUERY = "test_query"
TEST_TASK_ID = "test_task_id"
TEST_DB = "db"
TEST_CALLBACK_URL = "test_callback_url"
TEST_COMMAND = ["testa", "testb"]


@mock.patch(
    "dbt_server.services.dbt_service.get_sql_parser", return_value=TEST_SQL_PARSER
)
def _set_test_cache(state_id, _):
    """Helper sets global singleton manifest cache."""
    LAST_PARSED.set_last_parsed_manifest(
        state_id,
        TEST_PROJECT_PATH,
        TEST_ROOT_PATH,
        TEST_MANIFEST,
        TEST_MANIFEST_SIZE,
        TEST_CONFIG,
    )


def _get_test_cache(state_id: str, project_path: str = TEST_PROJECT_PATH):
    """Helper returns a CachedManifest object with given `state_id` and
    `project_path`.
    """
    return CachedManifest(
        state_id,
        project_path,
        TEST_ROOT_PATH,
        TEST_MANIFEST,
        TEST_MANIFEST_SIZE,
        TEST_CONFIG,
        TEST_SQL_PARSER,
    )


class TestCachedManifest(TestCase):
    def tearDown(self) -> None:
        LAST_PARSED.reset()

    @mock.patch(
        "dbt_server.services.dbt_service.get_sql_parser", return_value=TEST_SQL_PARSER
    )
    def test_set_last_parsed_manifest(self, mock_get_sql_parser):
        LAST_PARSED.set_last_parsed_manifest(
            TEST_STATE_ID,
            TEST_PROJECT_PATH,
            TEST_ROOT_PATH,
            TEST_MANIFEST,
            TEST_MANIFEST_SIZE,
            TEST_CONFIG,
        )
        mock_get_sql_parser.assert_called_once_with(TEST_CONFIG, TEST_MANIFEST)
        self.assertEqual(LAST_PARSED, _get_test_cache(TEST_STATE_ID))

    def test_lookup_none_state_empty_cache(self):
        self.assertIsNone(LAST_PARSED.lookup(None))

    def test_lookup_none_state_matched(self):
        _set_test_cache(None)
        self.assertEqual(LAST_PARSED.lookup(None), _get_test_cache(None))

    def test_lookup_state_empty_cache(self):
        self.assertIsNone(LAST_PARSED.lookup(TEST_STATE_ID))

    def test_lookup_state_cache_mismatched(self):
        _set_test_cache("unknown_test_state")
        self.assertIsNone(LAST_PARSED.lookup(TEST_STATE_ID))

    def test_lookup_state_cache_matched(self):
        _set_test_cache(TEST_STATE_ID)
        self.assertEqual(
            LAST_PARSED.lookup(TEST_STATE_ID), _get_test_cache(TEST_STATE_ID)
        )


class TestStateController(TestCase):
    def setUp(self) -> None:
        LAST_PARSED.reset()

    def _assert_state_controller_equals(self, a, b):
        self.assertEqual(a.state_id, b.state_id)
        self.assertEqual(a.project_path, b.project_path)
        self.assertEqual(a.root_path, b.root_path)
        self.assertEqual(a.manifest, b.manifest)
        self.assertEqual(a.config, b.config)
        self.assertEqual(a.parser, b.parser)
        self.assertEqual(a.manifest_size, b.manifest_size)
        self.assertEqual(a.is_manifest_cached, b.is_manifest_cached)

    def test_from_cached(self):
        self._assert_state_controller_equals(
            StateController.from_cached(_get_test_cache(TEST_STATE_ID)),
            StateController(
                TEST_STATE_ID,
                TEST_PROJECT_PATH,
                TEST_ROOT_PATH,
                TEST_MANIFEST,
                TEST_CONFIG,
                TEST_SQL_PARSER,
                TEST_MANIFEST_SIZE,
                True,
            ),
        )

    @mock.patch(
        "dbt_server.services.filesystem_service.get_root_path",
        return_value=TEST_ROOT_PATH,
    )
    @mock.patch(
        "dbt_server.services.dbt_service.parse_to_manifest", return_value=TEST_MANIFEST
    )
    @mock.patch(
        "dbt_server.services.dbt_service.create_dbt_config", return_value=TEST_CONFIG
    )
    @mock.patch(
        "dbt_server.services.dbt_service.get_sql_parser", return_value=TEST_SQL_PARSER
    )
    def test_parse_from_source(
        self,
        mock_get_sql_parser,
        mock_create_dbt_config,
        mock_parse_to_manifest,
        mock_get_root_path,
    ):
        parse_args = ParseArgs(state_id=TEST_STATE_ID, project_path=TEST_PROJECT_PATH)
        self._assert_state_controller_equals(
            StateController.parse_from_source(parse_args),
            StateController(
                TEST_STATE_ID,
                TEST_PROJECT_PATH,
                TEST_ROOT_PATH,
                TEST_MANIFEST,
                TEST_CONFIG,
                TEST_SQL_PARSER,
                0,  # manifest_size
                False,
            ),
        )
        mock_get_root_path.assert_called_once_with(TEST_STATE_ID, TEST_PROJECT_PATH)
        mock_create_dbt_config.assert_called_once_with(TEST_ROOT_PATH, parse_args)
        mock_get_sql_parser.assert_called_once_with(TEST_CONFIG, TEST_MANIFEST)
        mock_parse_to_manifest.assert_called_once_with(TEST_ROOT_PATH, parse_args)

    def test_load_state_non_state_cached(self):
        args = ParseArgs(state_id=None)
        _set_test_cache(None)
        self._assert_state_controller_equals(
            StateController.load_state(args),
            StateController(
                None,
                TEST_PROJECT_PATH,
                TEST_ROOT_PATH,
                TEST_MANIFEST,
                TEST_CONFIG,
                TEST_SQL_PARSER,
                TEST_MANIFEST_SIZE,
                True,
            ),
        )

    def test_load_state_state_cached(self):
        args = ParseArgs(state_id=TEST_STATE_ID)
        _set_test_cache(TEST_STATE_ID)
        self._assert_state_controller_equals(
            StateController.load_state(args),
            StateController(
                TEST_STATE_ID,
                TEST_PROJECT_PATH,
                TEST_ROOT_PATH,
                TEST_MANIFEST,
                TEST_CONFIG,
                TEST_SQL_PARSER,
                TEST_MANIFEST_SIZE,
                True,
            ),
        )

    @mock.patch(
        "dbt_server.services.filesystem_service.get_latest_state_id", return_value=None
    )
    @mock.patch(
        "dbt_server.services.filesystem_service.get_latest_project_path",
        return_value=None,
    )
    def test_load_state_no_cache_state_id_missing(
        self, mock_get_latest_project_path, mock_get_latest_state_id
    ):
        args = ParseArgs(state_id=None)
        with self.assertRaises(StateNotFoundException) as _:
            StateController.load_state(args)
            mock_get_latest_state_id.assert_called_once_with(None)
            mock_get_latest_project_path.assert_called_once_with()

    @mock.patch(
        "dbt_server.services.filesystem_service.get_latest_state_id", return_value=None
    )
    @mock.patch(
        "dbt_server.services.filesystem_service.get_latest_project_path",
        return_value=TEST_PROJECT_PATH,
    )
    @mock.patch(
        "dbt_server.services.filesystem_service.get_root_path",
        return_value=TEST_ROOT_PATH,
    )
    @mock.patch(
        "dbt_server.services.dbt_service.deserialize_manifest",
        return_value=TEST_MANIFEST,
    )
    @mock.patch(
        "dbt_server.services.filesystem_service.get_size",
        return_value=TEST_MANIFEST_SIZE,
    )
    @mock.patch(
        "dbt_server.services.dbt_service.create_dbt_config", return_value=TEST_CONFIG
    )
    @mock.patch(
        "dbt_server.services.dbt_service.get_sql_parser", return_value=TEST_SQL_PARSER
    )
    def test_load_state_no_cache_has_project_path(
        self,
        mock_get_sql_parser,
        mock_create_dbt_config,
        mock_get_size,
        mock_deserialize_manifest,
        mock_get_root_path,
        mock_get_latest_project_path,
        mock_get_latest_state_id,
    ):
        args = ParseArgs(state_id=None)

        self._assert_state_controller_equals(
            StateController.load_state(args),
            StateController(
                None,
                TEST_PROJECT_PATH,
                TEST_ROOT_PATH,
                TEST_MANIFEST,
                TEST_CONFIG,
                TEST_SQL_PARSER,
                TEST_MANIFEST_SIZE,
                False,
            ),
        )
        mock_get_root_path.assert_called_once_with(None, TEST_PROJECT_PATH)
        mock_get_latest_state_id.assert_called_once_with(None)
        mock_get_latest_project_path.assert_called_once_with()
        mock_deserialize_manifest.assert_called_once_with(TEST_MANIFEST_FILE)
        mock_get_size.assert_called_once_with(TEST_MANIFEST_FILE)
        mock_get_sql_parser.assert_called_once_with(TEST_CONFIG, TEST_MANIFEST)
        mock_create_dbt_config.assert_called_once_with(TEST_ROOT_PATH, args)

    @mock.patch(
        "dbt_server.services.filesystem_service.get_latest_state_id",
        return_value=TEST_STATE_ID,
    )
    @mock.patch(
        "dbt_server.services.filesystem_service.get_latest_project_path",
        return_value=None,
    )
    @mock.patch(
        "dbt_server.services.filesystem_service.get_root_path",
        return_value=TEST_ROOT_PATH,
    )
    @mock.patch(
        "dbt_server.services.dbt_service.deserialize_manifest",
        return_value=TEST_MANIFEST,
    )
    @mock.patch(
        "dbt_server.services.filesystem_service.get_size",
        return_value=TEST_MANIFEST_SIZE,
    )
    @mock.patch(
        "dbt_server.services.dbt_service.create_dbt_config", return_value=TEST_CONFIG
    )
    @mock.patch(
        "dbt_server.services.dbt_service.get_sql_parser", return_value=TEST_SQL_PARSER
    )
    def test_load_state_no_cache_has_state_id(
        self,
        mock_get_sql_parser,
        mock_create_dbt_config,
        mock_get_size,
        mock_deserialize_manifest,
        mock_get_root_path,
        mock_get_latest_project_path,
        mock_get_latest_state_id,
    ):
        args = ParseArgs(state_id=TEST_STATE_ID)

        self._assert_state_controller_equals(
            StateController.load_state(args),
            StateController(
                TEST_STATE_ID,
                None,
                TEST_ROOT_PATH,
                TEST_MANIFEST,
                TEST_CONFIG,
                TEST_SQL_PARSER,
                TEST_MANIFEST_SIZE,
                False,
            ),
        )
        mock_get_root_path.assert_called_once_with(TEST_STATE_ID, None)
        mock_get_latest_state_id.assert_called_once_with(TEST_STATE_ID)
        mock_get_latest_project_path.assert_called_once_with()
        mock_deserialize_manifest.assert_called_once_with(TEST_MANIFEST_FILE)
        mock_get_size.assert_called_once_with(TEST_MANIFEST_FILE)
        mock_get_sql_parser.assert_called_once_with(TEST_CONFIG, TEST_MANIFEST)
        mock_create_dbt_config.assert_called_once_with(TEST_ROOT_PATH, args)

    @mock.patch(
        "dbt_server.services.dbt_service.serialize_manifest", return_value=TEST_MANIFEST
    )
    @mock.patch(
        "dbt_server.services.filesystem_service.get_size",
        return_value=TEST_MANIFEST_SIZE,
    )
    def test_serialize_manifest(self, mock_get_size, mock_serialize_manifest):

        serialized_path = f"{TEST_ROOT_PATH}/manifest.msgpack"
        state_controller = StateController(
            None, None, TEST_ROOT_PATH, TEST_MANIFEST, None, None, None, None
        )
        state_controller.serialize_manifest()
        self.assertEqual(state_controller.manifest_size, TEST_MANIFEST_SIZE)
        mock_serialize_manifest.assert_called_once_with(TEST_MANIFEST, serialized_path)
        mock_get_size.assert_called_once_with(serialized_path)

    @mock.patch("dbt_server.services.filesystem_service.update_state_id")
    @mock.patch("dbt_server.services.filesystem_service.update_project_path")
    @mock.patch(
        "dbt_server.services.dbt_service.get_sql_parser", return_value=TEST_SQL_PARSER
    )
    def test_update_cache_state_id(
        self, mock_get_sql_parser, mock_update_project_path, mock_update_state_id
    ):
        state_controller = StateController(
            TEST_STATE_ID,
            None,
            TEST_ROOT_PATH,
            TEST_MANIFEST,
            TEST_CONFIG,
            TEST_SQL_PARSER,
            TEST_MANIFEST_SIZE,
            False,
        )
        state_controller.update_cache()
        mock_update_project_path.assert_not_called()
        mock_update_state_id.assert_called_once_with(TEST_STATE_ID)
        self.assertEqual(LAST_PARSED, _get_test_cache(TEST_STATE_ID, None))
        mock_get_sql_parser.assert_called_once_with(TEST_CONFIG, TEST_MANIFEST)

    @mock.patch("dbt_server.services.filesystem_service.update_state_id")
    @mock.patch("dbt_server.services.filesystem_service.update_project_path")
    @mock.patch(
        "dbt_server.services.dbt_service.get_sql_parser", return_value=TEST_SQL_PARSER
    )
    def test_update_cache_project_path(
        self, mock_get_sql_parser, mock_update_project_path, mock_update_state_id
    ):
        state_controller = StateController(
            None,
            TEST_PROJECT_PATH,
            TEST_ROOT_PATH,
            TEST_MANIFEST,
            TEST_CONFIG,
            TEST_SQL_PARSER,
            TEST_MANIFEST_SIZE,
            False,
        )
        state_controller.update_cache()
        mock_update_state_id.assert_not_called()
        mock_update_project_path.assert_called_once_with(TEST_PROJECT_PATH)
        self.assertEqual(LAST_PARSED, _get_test_cache(None, TEST_PROJECT_PATH))
        mock_get_sql_parser.assert_called_once_with(TEST_CONFIG, TEST_MANIFEST)

    @mock.patch("dbt_server.services.dbt_service.compile_sql", return_value=TEST_VALUE)
    def test_compile_query(self, mock_compile_sql):
        state_controller = StateController(
            None,
            None,
            TEST_ROOT_PATH,
            TEST_MANIFEST,
            TEST_CONFIG,
            TEST_SQL_PARSER,
            None,
            None,
        )
        self.assertEqual(state_controller.compile_query(TEST_QUERY), TEST_VALUE)
        mock_compile_sql.assert_called_once_with(
            TEST_MANIFEST, TEST_CONFIG, TEST_SQL_PARSER, TEST_QUERY
        )

    @mock.patch("dbt_server.services.dbt_service.execute_sql", return_value=TEST_VALUE)
    def test_execute_query(self, mock_execute_sql):
        state_controller = StateController(
            None, None, TEST_ROOT_PATH, TEST_MANIFEST, None, None, None, None
        )
        self.assertEqual(state_controller.execute_query(TEST_QUERY), TEST_VALUE)
        mock_execute_sql.assert_called_once_with(
            TEST_MANIFEST, TEST_ROOT_PATH, TEST_QUERY
        )

    @mock.patch("dbt_server.services.dbt_service.execute_async_command")
    def test_execute_async_command(self, mock_execute_async_command):
        state_controller = StateController(
            TEST_STATE_ID, None, TEST_ROOT_PATH, TEST_MANIFEST, None, None, None, None
        )
        state_controller.execute_async_command(
            TEST_TASK_ID, TEST_COMMAND, TEST_DB, TEST_CALLBACK_URL
        )
        mock_execute_async_command.assert_called_once_with(
            TEST_COMMAND,
            TEST_TASK_ID,
            TEST_ROOT_PATH,
            TEST_MANIFEST,
            TEST_DB,
            TEST_STATE_ID,
            TEST_CALLBACK_URL,
        )

    @mock.patch(
        "dbt_server.services.dbt_service.execute_sync_command", return_value=TEST_VALUE
    )
    def test_execute_sync_command(self, mock_execute_sync_command):
        state_controller = StateController(
            None, None, TEST_ROOT_PATH, TEST_MANIFEST, None, None, None, None
        )
        self.assertEqual(
            state_controller.execute_sync_command(TEST_COMMAND), TEST_VALUE
        )
        mock_execute_sync_command.assert_called_once_with(
            TEST_COMMAND, TEST_ROOT_PATH, TEST_MANIFEST
        )
