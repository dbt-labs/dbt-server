from dbt_server.flags import DBT_PROJECT_DIRECTORY
from dbt_server.views import PostInvocationRequest
from dbt_server.views import post_invocation
from json import loads
from unittest import TestCase
from unittest.mock import patch
from unittest import IsolatedAsyncioTestCase

TEST_COMMAND = ["abc"]
TEST_DIR = "/test"
TEST_COMMAND_WITH_PROJECT_DIR = ["abc", "--project-dir", TEST_DIR]
TEST_TASK_ID = "task_id"
TEST_URL = "test_url"
TEST_LOG_DIR = "/log"


class EmptyClass:
    pass


mock_task_obj = EmptyClass()
mock_task_obj.id = TEST_TASK_ID


class TestPostInvocationRequest(TestCase):
    def test_new_success(self):
        # Project directory from request field.
        PostInvocationRequest(command=["abc"], project_dir="abc")
        # Project directory from env.
        DBT_PROJECT_DIRECTORY.set("/test_dir")
        PostInvocationRequest(command=["abc"])
        # Project directory from command.
        DBT_PROJECT_DIRECTORY.set(None)
        PostInvocationRequest(command=["--project-dir"])

    def test_validate_error_duplicated_project_dir(self):
        DBT_PROJECT_DIRECTORY.set(None)
        with self.assertRaisesRegex(Exception, "Confliction") as _:
            PostInvocationRequest(command=["--project-dir"], project_dir="abc")


@patch("dbt_server.views.invoke")
@patch("dbt_server.views.filesystem_service")
@patch("dbt_server.views.uuid4", lambda: TEST_TASK_ID)
class TestPostInvocation(IsolatedAsyncioTestCase):
    async def test_success(self, mock_filesystem_service, mock_invoke):
        DBT_PROJECT_DIRECTORY.set(None)
        mock_invoke.apply_async.return_value = mock_task_obj
        mock_filesystem_service.get_log_path.return_value = (
            f"{TEST_LOG_DIR}/{TEST_TASK_ID}/dbt.log"
        )
        resp = await post_invocation(
            PostInvocationRequest(
                command=["abc"], project_dir=TEST_DIR, callback_url=TEST_URL
            )
        )
        mock_invoke.apply_async.assert_called_once_with(
            args=[TEST_COMMAND_WITH_PROJECT_DIR, TEST_URL], task_id=TEST_TASK_ID
        )
        mock_invoke.backend.store_result.assert_called_once_with(
            TEST_TASK_ID, None, "PENDING"
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(
            loads(resp.body),
            {"log_path": "/log/task_id/dbt.log", "task_id": TEST_TASK_ID},
        )

    async def test_success_command_project_dir(
        self, mock_filesystem_service, mock_invoke
    ):
        DBT_PROJECT_DIRECTORY.set(None)
        mock_invoke.apply_async.return_value = mock_task_obj
        mock_filesystem_service.get_log_path.return_value = (
            f"{TEST_LOG_DIR}/{TEST_TASK_ID}/dbt.log"
        )
        resp = await post_invocation(
            PostInvocationRequest(
                command=TEST_COMMAND_WITH_PROJECT_DIR, callback_url=TEST_URL
            )
        )
        mock_invoke.apply_async.assert_called_once_with(
            args=[TEST_COMMAND_WITH_PROJECT_DIR, TEST_URL], task_id=TEST_TASK_ID
        )
        mock_invoke.backend.store_result.assert_called_once_with(
            TEST_TASK_ID, None, "PENDING"
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(
            loads(resp.body),
            {"log_path": "/log/task_id/dbt.log", "task_id": TEST_TASK_ID},
        )

    async def test_success_env_var_project_dir(
        self, mock_filesystem_service, mock_invoke
    ):
        DBT_PROJECT_DIRECTORY.set(TEST_DIR)
        mock_invoke.apply_async.return_value = mock_task_obj
        mock_filesystem_service.get_log_path.return_value = (
            f"{TEST_LOG_DIR}/{TEST_TASK_ID}/dbt.log"
        )
        resp = await post_invocation(
            PostInvocationRequest(command=TEST_COMMAND, callback_url=TEST_URL)
        )
        mock_invoke.apply_async.assert_called_once_with(
            args=[TEST_COMMAND_WITH_PROJECT_DIR, TEST_URL], task_id=TEST_TASK_ID
        )
        mock_invoke.backend.store_result.assert_called_once_with(
            TEST_TASK_ID, None, "PENDING"
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(
            loads(resp.body),
            {"log_path": "/log/task_id/dbt.log", "task_id": TEST_TASK_ID},
        )

    async def test_success_input_task_id(self, mock_filesystem_service, mock_invoke):
        DBT_PROJECT_DIRECTORY.set(None)
        mock_invoke.apply_async.return_value = mock_task_obj
        mock_filesystem_service.get_log_path.return_value = (
            f"{TEST_LOG_DIR}/USER_TASK_ID/dbt.log"
        )
        resp = await post_invocation(
            PostInvocationRequest(
                command=TEST_COMMAND, callback_url=TEST_URL, task_id="USER_TASK_ID"
            )
        )
        mock_invoke.apply_async.assert_called_once_with(
            args=[TEST_COMMAND, TEST_URL], task_id="USER_TASK_ID"
        )
        mock_invoke.backend.store_result.assert_called_once_with(
            "USER_TASK_ID", None, "PENDING"
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(
            loads(resp.body),
            {"log_path": "/log/USER_TASK_ID/dbt.log", "task_id": "USER_TASK_ID"},
        )
