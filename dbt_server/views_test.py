from dbt_server.flags import DBT_PROJECT_DIRECTORY
from dbt_server.views import PostInvocationRequest
from dbt_server.views import post_invocation
from dbt_server.views import get_invocation
from dbt_server.views import list_invocation
from dbt_server.views import abort_invocation
from dbt_server.views import _list_all_task_ids_redis
from json import loads
from unittest.mock import patch
from unittest.mock import MagicMock
from unittest import IsolatedAsyncioTestCase
from unittest import TestCase

TEST_COMMAND = ["abc"]
TEST_DIR = "/test"
TEST_COMMAND_WITH_PROJECT_DIR = ["abc", "--project-dir", TEST_DIR]
TEST_TASK_ID = "task_id"
TEST_URL = "test_url"
TEST_LOG_DIR = "/log"
TEST_TASK_KEY_PREFIX = "prefix_"
TEST_TASK_KEY = f"{TEST_TASK_KEY_PREFIX}{TEST_TASK_ID}"


class EmptyClass:
    # Empty class to hold some data class member.
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
            args=[TEST_COMMAND_WITH_PROJECT_DIR, TEST_DIR, TEST_URL],
            task_id=TEST_TASK_ID,
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
            args=[TEST_COMMAND_WITH_PROJECT_DIR, None, TEST_URL], task_id=TEST_TASK_ID
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
            args=[TEST_COMMAND_WITH_PROJECT_DIR, TEST_DIR, TEST_URL],
            task_id=TEST_TASK_ID,
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
            args=[TEST_COMMAND, None, TEST_URL], task_id="USER_TASK_ID"
        )
        mock_invoke.backend.store_result.assert_called_once_with(
            "USER_TASK_ID", None, "PENDING"
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(
            loads(resp.body),
            {"log_path": "/log/USER_TASK_ID/dbt.log", "task_id": "USER_TASK_ID"},
        )


@patch("dbt_server.views.AbortableAsyncResult")
@patch("dbt_server.views.celery_app")
class TestGetInvocation(IsolatedAsyncioTestCase):
    async def test_success(self, mock_celery_app, mock_abortable_async_result):
        mock_celery_app.backend.get_key_for_task.return_value = TEST_TASK_KEY
        mock_celery_app.backend.get.return_value = 1
        mock_result = EmptyClass()
        mock_result.state = "PENDING"
        mock_result.task_id = TEST_TASK_ID
        mock_abortable_async_result.return_value = mock_result
        self.assertEqual(
            loads((await get_invocation(TEST_TASK_ID)).body),
            {"task_id": TEST_TASK_ID, "state": "PENDING"},
        )
        mock_celery_app.backend.get_key_for_task.assert_called_once_with(TEST_TASK_ID)
        mock_celery_app.backend.get.assert_called_once_with(TEST_TASK_KEY)
        mock_abortable_async_result.assert_called_once_with(
            TEST_TASK_ID, app=mock_celery_app
        )

    async def test_not_found(self, mock_celery_app, mock_abortable_async_result):
        mock_celery_app.backend.get_key_for_task.return_value = TEST_TASK_KEY
        mock_celery_app.backend.get.return_value = None
        self.assertEqual(
            loads((await get_invocation(TEST_TASK_ID)).body),
            {"task_id": TEST_TASK_ID, "state": "NOT_FOUND"},
        )
        mock_celery_app.backend.get_key_for_task.assert_called_once_with(TEST_TASK_ID)

    async def test_failure(self, mock_celery_app, mock_abortable_async_result):
        mock_celery_app.backend.get_key_for_task.return_value = TEST_TASK_KEY
        mock_celery_app.backend.get.return_value = 1
        mock_result = EmptyClass()
        mock_result.state = "FAILURE"
        mock_result.task_id = TEST_TASK_ID
        mock_result.result = Exception("test_exception")
        mock_abortable_async_result.return_value = mock_result
        self.assertEqual(
            loads((await get_invocation(TEST_TASK_ID)).body),
            {
                "task_id": TEST_TASK_ID,
                "state": "FAILURE",
                "exc_type": "Exception",
                "exc_message": "test_exception",
            },
        )
        mock_celery_app.backend.get_key_for_task.assert_called_once_with(TEST_TASK_ID)
        mock_celery_app.backend.get.assert_called_once_with(TEST_TASK_KEY)
        mock_abortable_async_result.assert_called_once_with(
            TEST_TASK_ID, app=mock_celery_app
        )


@patch("dbt_server.views.AbortableAsyncResult")
@patch("dbt_server.views.celery_app")
class TestListInvocation(IsolatedAsyncioTestCase):
    @patch("dbt_server.views._list_all_task_ids", return_value=[TEST_TASK_ID])
    async def test_success(self, _, mock_celery_app, mock_abortable_async_result):

        mock_result = EmptyClass()
        mock_result.state = "PENDING"
        mock_result.task_id = TEST_TASK_ID
        mock_abortable_async_result.return_value = mock_result
        self.assertEqual(
            loads((await list_invocation()).body),
            {"invocations": [{"task_id": TEST_TASK_ID, "state": "PENDING"}]},
        )
        mock_abortable_async_result.assert_called_once_with(
            TEST_TASK_ID, app=mock_celery_app
        )

    async def test_list_all_task_ids_redis(
        self, mock_celery_app, mock_abortable_async_result
    ):
        mock_celery_app.backend.get_key_for_task.side_effect = [
            f"{TEST_TASK_KEY_PREFIX}*",
            TEST_TASK_KEY_PREFIX,
        ]
        mock_celery_app.backend.client.keys.return_value = [TEST_TASK_KEY.encode()]
        self.assertEqual(_list_all_task_ids_redis(), [TEST_TASK_ID])

        mock_celery_app.backend.client.keys.assert_called_once_with(
            f"{TEST_TASK_KEY_PREFIX}*"
        )


@patch("dbt_server.views.AbortableAsyncResult")
@patch("dbt_server.views.celery_app")
class TestAbortInvocation(IsolatedAsyncioTestCase):
    async def test_success(self, mock_celery_app, mock_abortable_async_result):
        mock_celery_app.backend.get_key_for_task.return_value = TEST_TASK_KEY
        mock_celery_app.backend.get.return_value = 1
        mock_result_started = EmptyClass()
        mock_result_started.state = "STARTED"
        mock_result_started.task_id = TEST_TASK_ID
        mock_result_started.abort = MagicMock()
        mock_result_aborted = EmptyClass()
        mock_result_aborted.state = "ABORTED"
        mock_result_aborted.task_id = TEST_TASK_ID
        mock_abortable_async_result.side_effect = [
            mock_result_started,
            mock_result_aborted,
        ]
        self.assertEqual(
            loads((await abort_invocation(TEST_TASK_ID)).body),
            {"task_id": TEST_TASK_ID, "state": "ABORTED"},
        )
        mock_result_started.abort.assert_called_once_with()
        mock_celery_app.backend.get_key_for_task.assert_called_once_with(TEST_TASK_ID)
        mock_celery_app.backend.get.assert_called_once_with(TEST_TASK_KEY)
        mock_abortable_async_result.assert_any_call(TEST_TASK_ID, app=mock_celery_app)

    async def test_task_success(self, mock_celery_app, mock_abortable_async_result):
        mock_celery_app.backend.get_key_for_task.return_value = TEST_TASK_KEY
        mock_celery_app.backend.get.return_value = 1
        mock_result_started = EmptyClass()
        mock_result_started.state = "SUCCESS"
        mock_result_started.task_id = TEST_TASK_ID
        mock_abortable_async_result.return_value = mock_result_started
        self.assertEqual(
            loads((await abort_invocation(TEST_TASK_ID)).body),
            {"task_id": TEST_TASK_ID, "state": "SUCCESS"},
        )
        mock_celery_app.backend.get_key_for_task.assert_called_once_with(TEST_TASK_ID)
        mock_celery_app.backend.get.assert_called_once_with(TEST_TASK_KEY)
        mock_abortable_async_result.assert_any_call(TEST_TASK_ID, app=mock_celery_app)

    async def test_task_failure(self, mock_celery_app, mock_abortable_async_result):
        mock_celery_app.backend.get_key_for_task.return_value = TEST_TASK_KEY
        mock_celery_app.backend.get.return_value = 1
        mock_result_started = EmptyClass()
        mock_result_started.state = "FAILURE"
        mock_result_started.task_id = TEST_TASK_ID
        mock_result_started.result = Exception("test error.")
        mock_abortable_async_result.return_value = mock_result_started
        self.assertEqual(
            loads((await abort_invocation(TEST_TASK_ID)).body),
            {
                "task_id": TEST_TASK_ID,
                "state": "FAILURE",
                "exc_type": "Exception",
                "exc_message": "test error.",
            },
        )
        mock_celery_app.backend.get_key_for_task.assert_called_once_with(TEST_TASK_ID)
        mock_celery_app.backend.get.assert_called_once_with(TEST_TASK_KEY)
        mock_abortable_async_result.assert_any_call(TEST_TASK_ID, app=mock_celery_app)

    async def test_task_not_found(self, mock_celery_app, mock_abortable_async_result):
        mock_celery_app.backend.get_key_for_task.return_value = TEST_TASK_KEY
        mock_celery_app.backend.get.return_value = None
        self.assertEqual(
            loads((await abort_invocation(TEST_TASK_ID)).body),
            {"task_id": TEST_TASK_ID, "state": "NOT_FOUND"},
        )
        mock_celery_app.backend.get_key_for_task.assert_called_once_with(TEST_TASK_ID)
        mock_celery_app.backend.get.assert_called_once_with(TEST_TASK_KEY)
