from dbt_worker.app import app
from dbt_server.logging import DBT_SERVER_LOGGER as logger
from celery.contrib.abortable import AbortableTask
from celery.contrib.abortable import ABORTED
from celery.exceptions import Ignore
from celery.states import PROPAGATE_STATES
from celery.states import FAILURE
from celery.states import SUCCESS
from dbt.cli.main import dbtRunner
from threading import Thread
from typing import Any, Dict, Optional

# How long the timeout that parent thread should join with child dbt invocation
# thread. It's used to poll abort status.
JOIN_INTERVAL_SECONDS = 0.5


def _update_state(
    task: Any,
    task_id: str,
    state: str,
    meta: Dict = None,
    callback_url: Optional[str] = None,
):
    """Updates task state to `state` with `meta` infomation. Triggers callback
    if `callback_url` is set.

    Args:
        task: Celery task object.
        task_id: Which task should be updated. Why do we need this? Because
            celery task.request is a local variable, not shared across thread,
            hence we require task_id to update task state.
        state: Celery worker state.
        callback_url: If set, after state is updated, a callback will be
            triggered."""

    task.update_state(task_id=task_id, state=state, meta=meta)
    # TODO: Add callback logic.


def _invoke_runner(task: Any, task_id: str, command: str, callback_url: Optional[str]):
    """Invokes dbt runner with `command`, update task state if any exception is
    raised.

    Args:
        task: Celery task.
        task_id: Task id, it's required to update task state.
        command: Dbt invocation command.
        callback_url: If set, if core raises any error, a callback will be
            triggered."""
    try:
        dbt = dbtRunner()
        _, _ = dbt.invoke(command)
    except Exception as e:
        _update_state(
            task,
            task_id,
            FAILURE,
            {"exc_type": type(e).__name__, "exc_message": str(e)},
            callback_url,
        )


def _get_task_status(task: Any, task_id: str):
    """Retrieves task state for `task_id` from task backend."""
    return task.AsyncResult(task_id).state


def _invoke(task: Any, command: str, callback_url: Optional[str] = None):
    """Invokes dbt command.
    Args:
        command: Dbt commands that will be executed, e.g.
            "run --project-dir /a/b/jaffle_shop".
        callback_url: String, if set any time the task status is updated, worker
            will make a callback. Notice it's not complete, in some cases task
            status may be updated but we are not able to trigger callback, e.g.
            worker process is killed."""
    task_id = task.request.id
    logger.info(f"Running dbt task ({task_id}) with {command}")
    # TODO: Send callback to infer task start.

    # To support abort, we need to run dbt in a child thread, make parent thread
    # monitor abort signal and join with child thread.
    t = Thread(target=_invoke_runner, args=[task, task_id, command, callback_url])
    t.start()
    while t.is_alive():
        # TODO: Handle abort signal.
        t.join(JOIN_INTERVAL_SECONDS)
    # By the end of execution, a task state might be.
    # - STARTED, everything is fine!
    # - FAILURE, error occurs.
    # - ABORTED, user abort the task.
    task_status = _get_task_status(task, task_id)
    if task_status == ABORTED:
        # TODO: Handle abort.
        pass
    # If task status is not propagatable, we need to mark it as success manually
    # to trigger callback.
    elif task_status not in PROPAGATE_STATES:
        _update_state(task, task_id, SUCCESS, {}, callback_url)
    # Raises Ignore exception to make Celery not automatically set state to
    # SUCCESS.
    raise Ignore()


@app.task(bind=True, track_started=True, base=AbortableTask)
def invoke(self, command: str, callback_url: Optional[str] = None):
    _invoke(self, command, callback_url)