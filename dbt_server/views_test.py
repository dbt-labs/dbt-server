from dbt_server.views import get_invocation
from dbt_server.views import list_invocation
from dbt_server.views import abort_invocation
from dbt_server.views import _list_all_task_ids_redis
from unittest.mock import patch
from unittest.mock import MagicMock
from unittest import IsolatedAsyncioTestCase
from json import loads

TEST_TASK_ID = "test_id"
TEST_TASK_KEY_PREFIX = "prefix_"
TEST_TASK_KEY = f"{TEST_TASK_KEY_PREFIX}{TEST_TASK_ID}"


class EmptyClass:
    # Empty class to hold some data class member.
    pass


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

    async def test_task_not_found(self, mock_celery_app, mock_abortable_async_result):
        mock_celery_app.backend.get_key_for_task.return_value = TEST_TASK_KEY
        mock_celery_app.backend.get.return_value = None
        self.assertEqual(
            loads((await abort_invocation(TEST_TASK_ID)).body),
            {"task_id": TEST_TASK_ID, "state": "NOT_FOUND"},
        )
        mock_celery_app.backend.get_key_for_task.assert_called_once_with(TEST_TASK_ID)
        mock_celery_app.backend.get.assert_called_once_with(TEST_TASK_KEY)
