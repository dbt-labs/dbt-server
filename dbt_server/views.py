from collections import deque
import dbt.events.functions
import os
import signal
import uuid
from uuid import uuid4

from celery.backends.redis import RedisBackend
from celery.contrib.abortable import AbortableAsyncResult
from celery.states import UNREADY_STATES
from celery.states import PENDING
from celery.states import FAILURE
from fastapi import FastAPI, BackgroundTasks, Depends, status, HTTPException
from fastapi.exceptions import RequestValidationError
from starlette.requests import Request
from pydantic import BaseModel
from pydantic import validator
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from typing import List, Optional, Dict, Any
from copy import deepcopy

from dbt_server import crud, schemas, tracer, helpers
from dbt_server.services import filesystem_service
from dbt_server.logging import DBT_SERVER_LOGGER as logger
from dbt_server.models import TaskState
from dbt_server.state import StateController

from dbt_server.exceptions import (
    InvalidConfigurationException,
    InvalidRequestException,
    InternalException,
    StateNotFoundException,
)
from dbt_server.schemas import Invocation
from dbt_server.schemas import convert_celery_result_to_invocation
from dbt_server.schemas import get_not_found_invocation
from dbt_worker.app import app as celery_app
from dbt_worker.tasks import append_project_dir, invoke, resolve_project_dir
from dbt_worker.tasks import is_command_has_log_path

# ORM stuff
from sqlalchemy.orm import Session

LOG_PATH_ARGS = "--log-path"

# We need to override the EVENT_HISTORY queue to store
# only a small amount of events to prevent too much memory
# from being used.
dbt.events.functions.EVENT_HISTORY = deque(maxlen=10)


# Enable `ALLOW_ORCHESTRATED_SHUTDOWN` to instruct dbt server to
# ignore a first SIGINT or SIGTERM and enable a `/shutdown` endpoint
ALLOW_ORCHESTRATED_SHUTDOWN = os.environ.get(
    "ALLOW_ORCHESTRATED_SHUTDOWN", "0"
).lower() in ("true", "1", "on")

app = FastAPI()


class FileInfo(BaseModel):
    contents: str
    hash: str
    path: str


class PushProjectArgs(BaseModel):
    state_id: str
    body: Dict[str, FileInfo]


class ParseArgs(BaseModel):
    state_id: Optional[str] = None
    project_path: Optional[str] = None
    version_check: Optional[bool] = None
    profile: Optional[str] = None
    target: Optional[str] = None


class SQLConfig(BaseModel):
    state_id: Optional[str] = None
    sql: str
    target: Optional[str] = None
    profile: Optional[str] = None


class DbtCommandArgs(BaseModel):
    command: List[Any]
    state_id: Optional[str]
    project_path: Optional[str] = None
    # TODO: Need to handle this differently
    profile: Optional[str]
    callback_url: Optional[str]
    task_id: Optional[str]


@app.exception_handler(InvalidConfigurationException)
async def configuration_exception_handler(
    request: Request, exc: InvalidConfigurationException
):
    status_code = status.HTTP_422_UNPROCESSABLE_ENTITY
    exc_str = f"{exc}".replace("\n", " ").replace("   ", " ")
    logger.error(f"Request to {request.url} failed validation: {exc_str}")
    content = {"status_code": status_code, "message": exc_str, "data": None}
    return JSONResponse(content=content, status_code=status_code)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    status_code = status.HTTP_422_UNPROCESSABLE_ENTITY
    exc_str = f"{exc}".replace("\n", " ").replace("   ", " ")
    logger.error(f"Request to {request.url} failed validation: {exc_str}")
    content = {"status_code": status_code, "message": exc_str, "data": None}
    return JSONResponse(content=content, status_code=status_code)


@app.exception_handler(InternalException)
async def unhandled_internal_error(request: Request, exc: InternalException):
    status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
    exc_str = f"{exc}".replace("\n", " ").replace("   ", " ")
    logger.error(f"Request to {request.url} failed with an internal error: {exc_str}")

    content = {"status_code": status_code, "message": exc_str, "data": None}
    return JSONResponse(content=content, status_code=status_code)


@app.exception_handler(InvalidRequestException)
async def handled_dbt_error(request: Request, exc: InvalidRequestException):
    # Missing states get a 422, otherwise they get a 400
    if isinstance(exc, StateNotFoundException):
        status_code = status.HTTP_422_UNPROCESSABLE_ENTITY
    else:
        status_code = status.HTTP_400_BAD_REQUEST

    exc_str = f"{exc}".replace("\n", " ").replace("   ", " ")
    logger.error(f"Request to {request.url} was invalid: {exc_str}")

    content = {"status_code": status_code, "message": exc_str, "data": None}
    return JSONResponse(content=content, status_code=status_code)


if ALLOW_ORCHESTRATED_SHUTDOWN:

    @app.post("/shutdown")
    def shutdown():
        # raising a SIGKILL logs some
        # warnings about leaked semaphores--
        # appears this is a known issue that should be
        # solved once we move to python 3.9:
        # https://bugs.python.org/issue45209
        signal.raise_signal(signal.SIGKILL)
        signal.raise_signal(signal.SIGKILL)
        return JSONResponse(
            status_code=200,
            content={},
        )


def _lookup_abortable_async_result(task_id: str) -> bool:
    """Looks up Celery abortable async result by `task_id`, returns false if not
    found."""
    backend = celery_app.backend
    key = backend.get_key_for_task(task_id)
    return backend.get(key) is not None


def _list_all_task_ids_redis() -> List[str]:
    """Lists all Celery task ids from redis backend."""
    backend = celery_app.backend
    key = backend.get_key_for_task("*")
    # Celery will insert a prefix automatically, we need to remove it.
    celery_prefix = backend.get_key_for_task("")
    return [
        key_bytes.decode()[len(celery_prefix) :]
        for key_bytes in backend.client.keys(key)
    ]


def _list_all_task_ids() -> List[str]:
    """Lists list of all Celery task ids."""
    if isinstance(celery_app.backend, RedisBackend):
        return _list_all_task_ids_redis()
    else:
        raise Exception(
            f"We haven't support {type(celery_app.backend)} in _list_all_task_ids yet."
        )


@app.post("/ready")
async def ready():
    return JSONResponse(status_code=200, content={})


@app.post("/push")
def push_unparsed_manifest(args: PushProjectArgs):
    # Parse / validate it
    previous_state_id = filesystem_service.get_latest_state_id(None)
    state_id = filesystem_service.get_latest_state_id(args.state_id)

    size_in_files = len(args.body)
    size_in_bytes = sum(len(file.contents) for file in args.body.values())
    logger.info(f"Recieved manifest {size_in_files} files, {size_in_bytes} bytes")

    path = filesystem_service.get_root_path(state_id)
    reuse = True

    # Stupid example of reusing an existing manifest
    if not os.path.exists(path):
        reuse = False
        filesystem_service.write_unparsed_manifest_to_disk(
            state_id, previous_state_id, args.body
        )

    # Write messagepack repr to disk
    # Return a key that the client can use to operate on it?
    return JSONResponse(
        status_code=200,
        content={
            "state": state_id,
            "bytes": len(args.body),
            "reuse": reuse,
            "path": path,
        },
    )


@app.post("/parse")
def parse_project(args: ParseArgs):
    state = StateController.parse_from_source(args)
    state.serialize_manifest()
    state.update_cache()

    tracer.add_tags_to_current_span({"manifest_size": state.manifest_size})

    return JSONResponse(
        status_code=200,
        content={
            "parsing": state.state_id or state.project_path,
            "path": state.serialize_path,
        },
    )


@app.post("/async/dbt")
async def dbt_entry_async(
    args: DbtCommandArgs,
    background_tasks: BackgroundTasks,
    db: Session = Depends(crud.get_db),
):
    # example body: {"state_id": "123", "command":["run", "--threads", 1]}
    state = StateController.load_state_async(args)

    if args.task_id:
        task_id = args.task_id
    else:
        task_id = str(uuid.uuid4())

    log_path = filesystem_service.get_log_path(task_id, state.state_id)
    task = schemas.Task(
        task_id=task_id,
        state=TaskState.PENDING,
        command=(" ").join(str(param) for param in args.command),
        log_path=log_path,
    )

    db_task = crud.get_task(db, task_id)
    if db_task:
        raise HTTPException(status_code=400, detail="Task already registered")

    background_tasks.add_task(
        state.execute_async_command, task_id, args.command, db, args.callback_url
    )
    created_task = crud.create_task(db, task)
    return JSONResponse(
        status_code=200,
        content={
            "task_id": created_task.task_id,
            "state_id": state.state_id,
            "state": created_task.state,
            "command": created_task.command,
            "log_path": created_task.log_path,
        },
    )


@app.post("/sync/dbt")
async def dbt_entry_sync(args: DbtCommandArgs):
    # example body: {"command":["list", "--output", "json"]}
    state = StateController.load_state(args)
    # TODO: See what if any useful info is returned when there's no success
    results, _ = state.execute_sync_command(args.command)
    try:
        encoded_results = jsonable_encoder(results.to_dict())
    except AttributeError:
        encoded_results = jsonable_encoder(results)
    return JSONResponse(
        status_code=200,
        content={
            "parsing": state.state_id or state.project_path,
            "path": state.serialize_path,
            "command": (" ").join(str(param) for param in args.command),
            "res": encoded_results,
        },
    )


@app.post("/preview")
async def preview_sql(sql: SQLConfig):
    state = StateController.load_state(sql)
    result = state.execute_query(sql.sql)
    compiled_code = helpers.extract_compiled_code_from_node(result)

    tag_request_span(state)
    return JSONResponse(
        status_code=200,
        content={
            "parsing": state.state_id,
            "path": state.serialize_path,
            "res": jsonable_encoder(result),
            "compiled_code": compiled_code,
        },
    )


@app.post("/compile")
def compile_sql(sql: SQLConfig):
    state = StateController.load_state(sql)
    result = state.compile_query(sql.sql)
    compiled_code = helpers.extract_compiled_code_from_node(result)

    tag_request_span(state)

    return JSONResponse(
        status_code=200,
        content={
            "parsing": state.state_id,
            "path": state.serialize_path,
            "res": jsonable_encoder(result),
            "compiled_code": compiled_code,
        },
    )


def tag_request_span(state):
    manifest_metadata = get_manifest_metadata(state)
    tracer.add_tags_to_current_span(manifest_metadata)


def get_manifest_metadata(state):
    return {
        "manifest_size": state.manifest_size,
        "is_manifest_cached": state.is_manifest_cached,
    }


@app.get("/status/{task_id}")
def get_task_status(
    task_id: str,
    db: Session = Depends(crud.get_db),
):
    task = crud.get_task(db, task_id)
    return JSONResponse(status_code=200, content={"status": task.state})


class PostInvocationRequest(BaseModel):
    # Dbt command that will be sent to dbt worker for execution, e.g. [
    #   "--log-format", "json", "run", "--profiles_dir", "testdir"].
    command: List[str]
    # If set, dbt worker will use it as task_id, otherwise dbt server will
    # generate a random one and returned. Notice client needs to ensure task_id
    # uniqueness, post multiple invocations with the same task_id will cause
    # undetermined behavior.
    task_id: Optional[str]
    # Dbt project directory, if set --project-dir args will be appended into
    # command list. If not set, dbt server will fallback to environment
    # variable. The process logic is: (top one will override bottom)
    # - User command --project-dir args. We always respect user input at highest
    #   priority.
    # - Request project_dir field. Will append args to input command.
    # - Dbt server flags from env var(check details in dbt_server/flags.py).
    #   Will append args to input command.
    # - Implicit: task worker flag from env var."""
    project_dir: Optional[str]
    # Optional, if set dbt worker will trigger callback with task id and task
    # status when task status is updated.
    callback_url: Optional[str]

    @validator("project_dir", always=True)
    def check_project_dir(cls, project_dir, values):
        resolve_project_dir(values["command"], project_dir)
        # We don't change incoming request, only validate it.
        return project_dir


class PostInvocationResponse(BaseModel):
    # Unique task id of invocation.
    task_id: str
    # Absolute local path pointed to dbt.log file for the corresponding task.
    # It's available if user doesn't pass in --log-path args in command.
    # Notice it's not guaranteed dbt.log will always be generated by core(e.g.
    # critical error occurs).
    log_path: Optional[str]


@app.post("/invocations")
async def post_invocation(args: PostInvocationRequest):
    """Accepts user dbt invocation request, creates a task in task queue."""
    command = deepcopy(args.command)
    project_dir = resolve_project_dir(command, args.project_dir)
    append_project_dir(command, args.project_dir)
    task_id = str(uuid4()) if args.task_id is None else args.task_id
    # Manually store PENDING status in backend otherwise we can't tell apart
    # if task_id is missed or haven't been picked up by worker.
    invoke.backend.store_result(task_id, None, PENDING)

    try:
        logger.info(f"Invoke: {command}, task_id: {task_id}")
        invoke.apply_async(
            args=[command, project_dir, args.callback_url], task_id=task_id
        )
    except Exception as e:
        # If invocation is failed, change state to FAILURE. In strange case
        # that below store_result is failed, the request will always be PENDING.
        invoke.backend.store_result(
            task_id,
            {
                {"exc_type": type(e).__name__, "exc_message": str(e)},
            },
            FAILURE,
        )

    response = PostInvocationResponse(
        task_id=task_id,
        log_path=None
        if is_command_has_log_path(command)
        else filesystem_service.get_log_path(task_id, None),
    )
    return JSONResponse(
        status_code=200,
        content=response.dict(exclude_unset=True),
    )


@app.get("/invocations/{task_id}")
async def get_invocation(task_id: str):
    """Gets invocation entity by `task_id`."""
    invocation = (
        convert_celery_result_to_invocation(
            AbortableAsyncResult(task_id, app=celery_app)
        )
        if _lookup_abortable_async_result(task_id)
        else get_not_found_invocation(task_id)
    )

    return JSONResponse(status_code=200, content=invocation.dict(exclude_unset=True))


class ListInvocationResponse(BaseModel):
    # List of all invocations.
    invocations: List[Invocation]


@app.get("/invocations")
async def list_invocation():
    """Gets invocation entity by `task_id`."""
    return JSONResponse(
        status_code=200,
        content=ListInvocationResponse(
            invocations=[
                convert_celery_result_to_invocation(
                    AbortableAsyncResult(task_id, app=celery_app)
                )
                for task_id in _list_all_task_ids()
            ]
        ).dict(exclude_unset=True),
    )


@app.post("/invocation/{task_id}/abort")
async def abort_invocation(task_id: str):
    """Aborts tasks. Notice it's best effort, task may still finish or fail.
    Returns invocation model."""
    if not _lookup_abortable_async_result(task_id):
        return JSONResponse(
            status_code=200,
            content=get_not_found_invocation(task_id).dict(exclude_unset=True),
        )

    task = AbortableAsyncResult(task_id, app=celery_app)
    # UNREADY_STATES includes all Celery states that are not finalized yet(i.e.
    # it may be updated later).
    # If task is not finalized, we are able to abort it, otherwise we should not
    # abort it.
    if task.state in UNREADY_STATES:
        task.abort()

    # Re-pull task result from backend.
    return JSONResponse(
        status_code=200,
        content=convert_celery_result_to_invocation(
            AbortableAsyncResult(task_id, app=celery_app)
        ).dict(exclude_unset=True),
    )
