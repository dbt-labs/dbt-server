from dbt_server.exceptions import StateNotFoundException
from dbt_server.services.filesystem_service import get_working_dir
from dbt_server.services.filesystem_service import get_target_path
from dbt_server.services.filesystem_service import get_root_path
from dbt_server.services.filesystem_service import get_task_artifacts_path
from dbt_server.services.filesystem_service import get_log_path
from dbt_server.services.filesystem_service import get_partial_parse_path
from dbt_server.services.filesystem_service import get_db_path
from dbt_server.services.filesystem_service import get_latest_state_file_path
from dbt_server.services.filesystem_service import get_latest_project_path_file_path
from dbt_server.services.filesystem_service import get_path
from dbt_server.services.filesystem_service import get_size
from dbt_server.services.filesystem_service import write_file
from dbt_server.services.filesystem_service import copy_file
from dbt_server.services.filesystem_service import read_serialized_manifest
from dbt_server.services.filesystem_service import write_unparsed_manifest_to_disk
from dbt_server.services.filesystem_service import get_latest_state_id
from dbt_server.services.filesystem_service import get_latest_project_path
from dbt_server.services.filesystem_service import update_state_id
from dbt_server.services.filesystem_service import update_project_path
from os import makedirs
from os import environ
from os import path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest import mock

TEST_STATE_ID = "test_state"
TEST_PREVIOUS_STATE_ID = "test_previous_state"
TEST_PATH = "/test_path"
TEST_PROJECT_PATH = "/tmp"
TEST_TASK_ID = "task_id"
TEST_STRING = "test_string"
TEST_MANIFEST = "test_manifest"


class TestFile:
    pass


class TestCachedManifest(TestCase):
    def tearDown(self) -> None:
        if "__DBT_WORKING_DIR" in environ:
            del environ["__DBT_WORKING_DIR"]
        if "DBT_TARGET_PATH" in environ:
            del environ["DBT_TARGET_PATH"]

    def test_get_working_dir(self):
        # Env
        environ["__DBT_WORKING_DIR"] = TEST_PATH
        self.assertEqual(get_working_dir(), TEST_PATH)
        del environ["__DBT_WORKING_DIR"]
        # Default
        self.assertEqual(get_working_dir(), "./working-dir")

    def test_get_target_path(self):
        # Env
        environ["DBT_TARGET_PATH"] = TEST_PATH
        self.assertEqual(get_target_path(), TEST_PATH)
        del environ["DBT_TARGET_PATH"]
        # Default
        self.assertEqual(get_target_path(), "./target")

    def test_get_root_path(self):
        # None
        self.assertIsNone(get_root_path(None, None))
        # State id
        self.assertEqual(
            get_root_path(TEST_STATE_ID, None), "./working-dir/state-test_state"
        )
        # Project path
        self.assertEqual(get_root_path(None, TEST_PROJECT_PATH), "/tmp")

    def test_get_task_artifacts_path(self):
        # Non-state
        self.assertEqual(
            get_task_artifacts_path(TEST_TASK_ID, None), "./working-dir/task_id"
        )
        # State id
        self.assertEqual(
            get_task_artifacts_path(TEST_TASK_ID, TEST_STATE_ID),
            "./working-dir/state-test_state/task_id",
        )

    def test_get_log_path(self):
        # Non-state
        self.assertEqual(
            get_log_path(TEST_TASK_ID, None), "./working-dir/task_id/dbt.log"
        )
        # State id
        self.assertEqual(
            get_log_path(TEST_TASK_ID, TEST_STATE_ID),
            "./working-dir/state-test_state/task_id/dbt.log",
        )

    def test_get_partial_parse_path(self):
        self.assertEqual(get_partial_parse_path(), "./target/partial_parse.msgpack")

    def test_get_db_path(self):
        # New directory.
        with TemporaryDirectory() as temp_dir:
            test_working_dir = path.join(temp_dir, "test_path")
            environ["__DBT_WORKING_DIR"] = test_working_dir
            self.assertEqual(get_db_path(), f"{test_working_dir}/sql_app.db")
            self.assertTrue(path.isdir(test_working_dir))
            del environ["__DBT_WORKING_DIR"]

        # Existing directory.
        with TemporaryDirectory() as temp_dir:
            environ["__DBT_WORKING_DIR"] = temp_dir
            self.assertEqual(get_db_path(), f"{temp_dir}/sql_app.db")
            del environ["__DBT_WORKING_DIR"]

    def test_get_latest_state_file_path(self):
        self.assertEqual(
            get_latest_state_file_path(), "./working-dir/latest-state-id.txt"
        )

    def test_get_latest_project_path_file_path(self):
        self.assertEqual(
            get_latest_project_path_file_path(), "./working-dir/latest-project-path.txt"
        )

    def test_get_path(self):
        self.assertEqual(get_path("a", "b", "c"), "a/b/c")

    def test_get_size(self):
        with TemporaryDirectory() as temp_dir:
            test_file_path = path.join(temp_dir, "a.txt")
            with open(test_file_path, "w") as test_file:
                test_file.write(TEST_STRING)
            self.assertEqual(get_size(test_file_path), len(TEST_STRING))

    def test_write_file(self):
        # New directory.
        with TemporaryDirectory() as temp_dir:
            test_write_path = path.join(temp_dir, "test_path", "a.txt")
            write_file(test_write_path, TEST_STRING)
            with open(test_write_path, "r") as input_file:
                self.assertEqual(input_file.read(), TEST_STRING)

        # Existing directory.
        with TemporaryDirectory() as temp_dir:
            test_write_path = path.join(temp_dir, "a.txt")
            write_file(test_write_path, TEST_STRING)
            with open(test_write_path, "r") as input_file:
                self.assertEqual(input_file.read(), TEST_STRING)

    def test_copy_file(self):
        with TemporaryDirectory() as temp_dir:
            source_path = path.join(temp_dir, "a.txt")
            with open(source_path, "w") as source_file:
                source_file.write(TEST_STRING)
            # New directory.
            dest_path = path.join(temp_dir, "a", "a.txt")
            copy_file(source_path, dest_path)
            with open(dest_path, "r") as input_file:
                self.assertEqual(input_file.read(), TEST_STRING)
            # Existing directory.
            dest_path = path.join(temp_dir, "b.txt")
            copy_file(source_path, dest_path)
            with open(dest_path, "r") as input_file:
                self.assertEqual(input_file.read(), TEST_STRING)

    def test_read_serialized_manifest(self):
        with TemporaryDirectory() as temp_dir:
            # Found
            file_path = path.join(temp_dir, "a.txt")
            with open(file_path, "w") as input_file:
                input_file.write(TEST_STRING)
            self.assertEqual(
                read_serialized_manifest(file_path), TEST_STRING.encode("utf-8")
            )
            # Not found.
            with self.assertRaises(StateNotFoundException) as _:
                read_serialized_manifest("unknown_path")

    def test_write_unparsed_manifest_to_disk_none_previous_state(self):
        test_file = TestFile()
        test_file.contents = TEST_STRING
        with TemporaryDirectory() as temp_dir:
            environ["__DBT_WORKING_DIR"] = temp_dir
            write_unparsed_manifest_to_disk(TEST_STATE_ID, None, {"a.txt": test_file})
            with open(
                path.join(temp_dir, f"state-{TEST_STATE_ID}", "a.txt"), "r"
            ) as output_file:
                self.assertEqual(output_file.read(), TEST_STRING)

    def test_write_unparsed_manifest_to_disk_same_previous_state(self):
        test_file = TestFile()
        test_file.contents = TEST_STRING
        with TemporaryDirectory() as temp_dir:
            environ["__DBT_WORKING_DIR"] = temp_dir
            write_unparsed_manifest_to_disk(
                TEST_STATE_ID, TEST_STATE_ID, {"a.txt": test_file}
            )
            with open(
                path.join(temp_dir, f"state-{TEST_STATE_ID}", "a.txt"), "r"
            ) as output_file:
                self.assertEqual(output_file.read(), TEST_STRING)

    @mock.patch("dbt_server.services.filesystem_service.copy_file")
    def test_write_unparsed_manifest_to_disk_missing_previous_manifest(
        self, mock_copy_file
    ):
        test_file = TestFile()
        test_file.contents = TEST_STRING
        with TemporaryDirectory() as temp_dir:
            environ["__DBT_WORKING_DIR"] = temp_dir
            write_unparsed_manifest_to_disk(
                TEST_STATE_ID, TEST_PREVIOUS_STATE_ID, {"a.txt": test_file}
            )
            with open(
                path.join(temp_dir, f"state-{TEST_STATE_ID}", "a.txt"), "r"
            ) as output_file:
                self.assertEqual(output_file.read(), TEST_STRING)
            mock_copy_file.assert_not_called()

    @mock.patch("dbt_server.services.filesystem_service.copy_file")
    def test_write_unparsed_manifest_to_disk_copy_previous_manifest(
        self, mock_copy_file
    ):
        test_file = TestFile()
        test_file.contents = TEST_STRING
        with TemporaryDirectory() as temp_dir:
            previous_parse_path = path.join(
                temp_dir,
                f"state-{TEST_PREVIOUS_STATE_ID}",
                "target",
                "partial_parse.msgpack",
            )
            makedirs(path.dirname(previous_parse_path))
            current_parse_path = path.join(
                temp_dir, f"state-{TEST_STATE_ID}", "target", "partial_parse.msgpack"
            )
            with open(previous_parse_path, "w") as previous_manifest_file:
                previous_manifest_file.write(TEST_MANIFEST)
            environ["__DBT_WORKING_DIR"] = temp_dir
            write_unparsed_manifest_to_disk(
                TEST_STATE_ID, TEST_PREVIOUS_STATE_ID, {"a.txt": test_file}
            )
            with open(
                path.join(temp_dir, f"state-{TEST_STATE_ID}", "a.txt"), "r"
            ) as output_file:
                self.assertEqual(output_file.read(), TEST_STRING)
        mock_copy_file.assert_called_once_with(previous_parse_path, current_parse_path)

    def test_get_latest_state_id(self):
        with TemporaryDirectory() as temp_dir:
            environ["__DBT_WORKING_DIR"] = temp_dir
            # State id passed in.
            self.assertEqual(get_latest_state_id(TEST_STATE_ID), TEST_STATE_ID)
            # Missing local persisted state id.
            self.assertIsNone(get_latest_state_id(None))
            # Has local persisted state id.
            with open(path.join(temp_dir, "latest-state-id.txt"), "w") as state_file:
                state_file.write(TEST_STATE_ID)
            self.assertEqual(get_latest_state_id(None), TEST_STATE_ID)

    def test_get_latest_project_path(self):
        # Has local persisted project path.
        with TemporaryDirectory() as temp_dir:
            environ["__DBT_WORKING_DIR"] = temp_dir
            # Missing local persisted project path.
            self.assertIsNone(get_latest_project_path())
            # Found local persisted project path.
            with open(
                path.join(temp_dir, "latest-project-path.txt"), "w"
            ) as project_file:
                project_file.write(TEST_PROJECT_PATH)
            self.assertEqual(get_latest_project_path(), TEST_PROJECT_PATH)

    def test_update_state_id(self):
        with TemporaryDirectory() as temp_dir:
            environ["__DBT_WORKING_DIR"] = temp_dir
            update_state_id(TEST_STATE_ID)
            with open(path.join(temp_dir, "latest-state-id.txt"), "r") as state_file:
                self.assertEqual(state_file.read(), TEST_STATE_ID)

    def test_update_project_path(self):
        with TemporaryDirectory() as temp_dir:
            environ["__DBT_WORKING_DIR"] = temp_dir
            update_project_path(TEST_PROJECT_PATH)
            with open(
                path.join(temp_dir, "latest-project-path.txt"), "r"
            ) as project_path_file:
                self.assertEqual(project_path_file.read(), TEST_PROJECT_PATH)
