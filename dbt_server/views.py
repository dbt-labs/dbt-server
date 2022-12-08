from collections import deque
import dbt.events.functions
import os
import signal

from sse_starlette.sse import EventSourceResponse
from fastapi import FastAPI, BackgroundTasks, Depends, status
from fastapi.exceptions import RequestValidationError
from starlette.requests import Request
from pydantic import BaseModel, Field
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from typing import List, Optional, Union, Dict

from dbt_server.state import StateController
from dbt_server import crud, schemas, helpers
from dbt_server import tracer

from dbt_server.services import (
    filesystem_service,
    dbt_service,
    task_service,
)

from dbt_server.exceptions import (
    InvalidConfigurationException,
    InvalidRequestException,
    InternalException,
    StateNotFoundException,
)
from dbt_server.logging import GLOBAL_LOGGER as logger

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


@app.middleware("http")
async def log_request_start(request: Request, call_next):
    logger.debug(f"Received request: {request.method} {request.url.path}")
    response = await call_next(request)
    return response


class FileInfo(BaseModel):
    contents: str
    hash: str
    path: str


class PushProjectArgs(BaseModel):
    state_id: str
    body: Dict[str, FileInfo]


class ParseArgs(BaseModel):
    state_id: str
    version_check: Optional[bool] = None
    profile: Optional[str] = None
    target: Optional[str] = None


class BuildArgs(BaseModel):
    state_id: str
    profile: Optional[str] = None
    target: Optional[str] = None
    single_threaded: Optional[bool] = None
    resource_types: Optional[List[str]] = None
    select: Union[None, str, List[str]] = None
    threads: Optional[int] = None
    exclude: Union[None, str, List[str]] = None
    selector_name: Optional[str] = None
    state: Optional[str] = None
    defer: Optional[bool] = None
    fail_fast: Optional[bool] = None
    full_refresh: Optional[bool] = None
    store_failures: Optional[bool] = None
    indirect_selection: str = ""
    version_check: Optional[bool] = None


class RunArgs(BaseModel):
    state_id: str
    profile: Optional[str] = None
    target: Optional[str] = None
    single_threaded: Optional[bool] = None
    threads: Optional[int] = None
    models: Union[None, str, List[str]] = None
    select: Union[None, str, List[str]] = None
    exclude: Union[None, str, List[str]] = None
    selector_name: Optional[str] = None
    state: Optional[str] = None
    defer: Optional[bool] = None
    fail_fast: Optional[bool] = None
    full_refresh: Optional[bool] = None
    version_check: Optional[bool] = None


class TestArgs(BaseModel):
    state_id: str
    profile: Optional[str] = None
    target: Optional[str] = None
    single_threaded: Optional[bool] = None
    threads: Optional[int] = None
    data_type: bool = Field(False, alias="data")
    schema_type: bool = Field(False, alias="schema")
    models: Union[None, str, List[str]] = None
    select: Union[None, str, List[str]] = None
    exclude: Union[None, str, List[str]] = None
    selector_name: Optional[str] = None
    state: Optional[str] = None
    defer: Optional[bool] = None
    fail_fast: Optional[bool] = None
    store_failures: Optional[bool] = None
    full_refresh: Optional[bool] = None
    indirect_selection: str = ""
    version_check: Optional[bool] = None


class SeedArgs(BaseModel):
    state_id: str
    profile: Optional[str] = None
    target: Optional[str] = None
    single_threaded: Optional[bool] = None
    threads: Optional[int] = None
    models: Union[None, str, List[str]] = None
    select: Union[None, str, List[str]] = None
    exclude: Union[None, str, List[str]] = None
    selector_name: Optional[str] = None
    show: Optional[bool] = None
    state: Optional[str] = None
    selector_name: Optional[str] = None
    full_refresh: Optional[bool] = None
    version_check: Optional[bool] = None


class ListArgs(BaseModel):
    state_id: str
    profile: Optional[str] = None
    target: Optional[str] = None
    single_threaded: Optional[bool] = None
    resource_types: Optional[List[str]] = None
    models: Union[None, str, List[str]] = None
    exclude: Union[None, str, List[str]] = None
    select: Union[None, str, List[str]] = None
    selector_name: Optional[str] = None
    output: Optional[str] = "name"
    output_keys: Union[None, str, List[str]] = None
    state: Optional[str] = None
    indirect_selection: str = "eager"


class SnapshotArgs(BaseModel):
    state_id: str
    profile: Optional[str] = None
    target: Optional[str] = None
    single_threaded: Optional[bool] = None
    threads: Optional[int] = None
    resource_types: Optional[List[str]] = None
    models: Union[None, str, List[str]] = None
    select: Union[None, str, List[str]] = None
    exclude: Union[None, str, List[str]] = None
    selector_name: Optional[str] = None
    state: Optional[str] = None
    defer: Optional[bool] = None


class RunOperationArgs(BaseModel):
    state_id: str
    profile: Optional[str] = None
    target: Optional[str] = None
    macro: str
    single_threaded: Optional[bool] = None
    args: str = Field(default="{}")


class SQLConfig(BaseModel):
    state_id: Optional[str] = None
    sql: str
    target: Optional[str] = None
    profile: Optional[str] = None


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
    async def shutdown():
        # raise 2 SIGTERM signals, just to
        # make sure this really shuts down.
        # raising a SIGKILL logs some
        # warnings about leaked semaphores
        signal.raise_signal(signal.SIGTERM)
        signal.raise_signal(signal.SIGTERM)
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
    state_id = filesystem_service.get_latest_state_id(args.state_id)

    size_in_files = len(args.body)
    size_in_bytes = sum(len(file.contents) for file in args.body.values())
    logger.info(f"Recieved manifest {size_in_files} files, {size_in_bytes} bytes")

    path = filesystem_service.get_root_path(state_id)
    reuse = True

    # Stupid example of reusing an existing manifest
    if not os.path.exists(path):
        reuse = False
        filesystem_service.write_unparsed_manifest_to_disk(state_id, args.body)

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
    state = StateController.parse_from_source(args.state_id, args)
    state.serialize_manifest()
    state.update_state_id()
    state.update_cache()

    tracer.add_tags_to_current_span({"manifest_size": state.manifest_size})

    return JSONResponse(
        status_code=200,
        content={"parsing": args.state_id, "path": state.serialize_path},
    )


@app.post("/run")
async def run_models(args: RunArgs):
    state_id = filesystem_service.get_latest_state_id(args.state_id)
    path = filesystem_service.get_root_path(state_id)
    serialize_path = filesystem_service.get_path(state_id, "manifest.msgpack")

    manifest = dbt_service.deserialize_manifest(serialize_path)
    results = dbt_service.dbt_run(path, args, manifest)
    encoded_results = jsonable_encoder(results.to_dict())
    return JSONResponse(
        status_code=200,
        content={
            "parsing": args.state_id,
            "path": serialize_path,
            "res": encoded_results,
        },
    )


@app.post("/list")
async def list_resources(args: ListArgs):
    state_id = filesystem_service.get_latest_state_id(args.state_id)
    path = filesystem_service.get_root_path(state_id)
    serialize_path = filesystem_service.get_path(state_id, "manifest.msgpack")

    manifest = dbt_service.deserialize_manifest(serialize_path)
    results = dbt_service.dbt_list(path, args, manifest)

    encoded_results = jsonable_encoder(results)

    return JSONResponse(
        status_code=200,
        content={
            "parsing": args.state_id,
            "path": serialize_path,
            "res": encoded_results,
        },
    )


@app.post("/run-async")
async def run_models_async(
    args: RunArgs,
    background_tasks: BackgroundTasks,
    response_model=schemas.Task,
    db: Session = Depends(crud.get_db),
):
    return task_service.run_async(background_tasks, db, args)


@app.post("/test-async")
async def test_async(
    args: TestArgs,
    background_tasks: BackgroundTasks,
    response_model=schemas.Task,
    db: Session = Depends(crud.get_db),
):
    return task_service.test_async(background_tasks, db, args)


@app.post("/seed-async")
async def seed_async(
    args: SeedArgs,
    background_tasks: BackgroundTasks,
    response_model=schemas.Task,
    db: Session = Depends(crud.get_db),
):
    return task_service.seed_async(background_tasks, db, args)


@app.post("/build-async")
async def build_async(
    args: BuildArgs,
    background_tasks: BackgroundTasks,
    response_model=schemas.Task,
    db: Session = Depends(crud.get_db),
):
    return task_service.build_async(background_tasks, db, args)


@app.post("/snapshot-async")
async def snapshot_async(
    args: SnapshotArgs,
    background_tasks: BackgroundTasks,
    response_model=schemas.Task,
    db: Session = Depends(crud.get_db),
):
    return task_service.snapshot_async(background_tasks, db, args)


@app.post("/run-operation-async")
async def run_operation_async(
    args: RunOperationArgs,
    background_tasks: BackgroundTasks,
    response_model=schemas.Task,
    db: Session = Depends(crud.get_db),
):
    return task_service.run_operation_async(background_tasks, db, args)


@app.post("/preview")
async def preview_sql(sql: SQLConfig):
    state = StateController.load_state(sql.state_id, sql)
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
    state = StateController.load_state(sql.state_id, sql)
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


class Task(BaseModel):
    task_id: str


@app.get("/stream-logs/{task_id}")
async def log_endpoint(
    task_id: str,
    request: Request,
    db: Session = Depends(crud.get_db),
):
    event_generator = task_service.tail_logs_for_path(db, task_id, request)
    return EventSourceResponse(event_generator, ping=2)
