from collections import deque
import dbt.events.functions
import os
import signal
import uuid

from fastapi import FastAPI, BackgroundTasks, Depends, status, HTTPException
from fastapi.exceptions import RequestValidationError
from starlette.requests import Request
from pydantic import BaseModel
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from typing import List, Optional, Dict, Any

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

# ORM stuff
from sqlalchemy.orm import Session

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
    state = StateController.load_state(args)

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
    # List of dbt command that will be sent to dbt worker for execution.
    # E.g. ["--log-format", "json", "run", "--profiles_dir", "testdir"]
    command: List[str]
    # Dbt project path, if set --project-dir args will be appended into command
    # list. If not set, dbt server will fallback to environment variable and
    # append it. If environment variable is empty, request will be rejected.
    # Notice user can specify --project-dir args in command directly, in that
    # case, we will respect user request and won't append anything.
    project_path: Optional[str]
    # Optional, if set dbt worker will trigger callback with task id and task
    # status when task status is updated.
    callback_url: Optional[str]


class PostInvocationResponse(BaseModel):
    # Unique task id of invocation.
    task_id: str
    # Absolute local path pointed to dbt.log file for the corresponding task.
    # It's available if user doesn't pass in --log-path args in command.
    # Notice it's not guaranteed dbt.log will always be generated by core(e.g.
    # critical error occurs).
    log_path: Optional[str]


@app.post("/invocation")
async def post_invocation(args: PostInvocationRequest):
    """Accepts user dbt invocation request, creates a task in task queue."""
    # TODO: Implement.
    return JSONResponse(status_code=200, content={})


class GetInvocationRequest(BaseModel):
    # Unique task id of invocation.
    task_id: str


class GetInvocationResponse(BaseModel):
    # Unique task id of invocation, same as request.
    task_id: str
    # Task state, it's one of celery worker state:
    # - PENDING: The task is pending to be executed or never been created yet.
    #   Due to celery backend design, we can't tell if task is pending or never
    #   been created yet. Definitely there is a workaround, but we don't support
    #   right now.
    # - STARTED: The task is started by worker.
    # - SUCCESS: The task is finished by worker.
    # - FAILURE: The task is failed.
    # - ABORTED: The task is aborted by user.
    state: str
    # Only exists if state = FAILURE, python exception type name that causes
    # task is killed, e.g. dbtUsageException, WorkerLostError.
    exc_type: Optional[str]
    # Only exists if state = FAILURE, similar to exc_type, it includes error
    # message of exception.
    exc_message: Optional[str]


@app.get("/invocation")
async def get_invocation(args: GetInvocationRequest):
    """Gets task state and other metadata."""
    # TODO: Implement.
    return JSONResponse(status_code=200, content={})


class AbortInvocationRequest(BaseModel):
    # Unique task id of invocation to be aborted.
    task_id: str


@app.post("/invocation/abort")
async def abort_invocation(args: AbortInvocationRequest):
    """Aborts tasks. Notice it's best effort, task may still finish or fail."""
    # TODO: Implement.
    return JSONResponse(status_code=200, content={})
