from typing import Optional

from celery.contrib.abortable import AbortableAsyncResult
from celery.states import PENDING
from celery.states import STARTED
from celery.states import SUCCESS
from celery.states import FAILURE
from celery.contrib.abortable import ABORTED
from pydantic import BaseModel

NOT_FOUND = "NOT_FOUND"
INTERNAL_ERROR = "INTERNAL_ERROR"


class Task(BaseModel):
    task_id: str
    state: str
    command: Optional[str] = None
    log_path: Optional[str] = None
    error: Optional[str] = None

    class Config:
        orm_mode = True


class Invocation(BaseModel):
    # Unique task id of invocation, same as request.
    task_id: str
    # Task state, it's one of celery worker state:
    # - NOT_FOUND: We can't find task with given task_id.
    # - PENDING: The task is pending to be executed.
    # - STARTED: The task is started by worker.
    # - SUCCESS: The task is finished by worker.
    # - FAILURE: The task is failed.
    # - ABORTED: The task is aborted by user.
    # - INTERNAL_ERROR: Internal error occurs(rare case).
    state: str
    # Only exists if state = FAILURE, python exception type name that caused
    # task to be killed, e.g. dbtUsageException, WorkerLostError.
    exc_type: Optional[str]
    # Only exists if state = FAILURE, similar to exc_type, it includes error
    # message of exception.
    exc_message: Optional[str]


def get_not_found_invocation(task_id: str):
    """Returns Invocation class that represents a task is not found."""
    return Invocation(task_id=task_id, state=NOT_FOUND)


def convert_celery_result_to_invocation(result: Optional[AbortableAsyncResult]):
    """Converts Celery task result to Invocation class given."""
    state = result.state
    if state in [PENDING, SUCCESS, STARTED, ABORTED]:
        return Invocation(task_id=result.task_id, state=state)
    elif state == FAILURE:
        exception = result.result
        return Invocation(
            task_id=result.task_id,
            state=state,
            exc_type=type(exception).__name__,
            exc_message=str(exception),
        )
    else:
        return Invocation(task_id=result.task_id, state=INTERNAL_ERROR)
