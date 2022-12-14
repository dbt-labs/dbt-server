import os
import signal
from dbt.contracts.sql import RemoteRunResult, RemoteCompileResult
from dbt.exceptions import CompilationException

from sse_starlette.sse import EventSourceResponse
from fastapi import FastAPI, BackgroundTasks, Depends, status, Query
from fastapi.exceptions import RequestValidationError
from starlette.requests import Request
from pydantic import BaseModel, Field
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from typing import List, Optional, Union, Dict

from .services import filesystem_service
from .services import dbt_service
from .services import task_service
from .logging import GLOBAL_LOGGER as logger

from dbt_server.exceptions import InvalidConfigurationException


import click
import os
import sys
from typing import Optional



# ORM stuff
from sqlalchemy.orm import Session
from . import crud
from . import schemas

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
    install_deps: Optional[bool] = False


class DepsArgs(BaseModel):
    packages: Optional[str] = None
    profile: Optional[str] = None
    target: Optional[str] = None


class ParseArgs(BaseModel):
    state_id: str
    version_check: Optional[bool] = None
    profile: Optional[str] = None
    target: Optional[str] = None











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
    output: Optional[str] = ""
    output_keys: Union[None, str, List[str]] = None
    state: Optional[str] = None
    indirect_selection: str = ""


class SQLConfig(BaseModel):
    state_id: Optional[str] = None
    sql: str


@app.exception_handler(InvalidConfigurationException)
async def configuration_exception_handler(
    request: Request, exc: InvalidConfigurationException
):
    exc_str = f"{exc}".replace("\n", " ").replace("   ", " ")
    logger.error(f"Request to {request.url} failed validation: {exc_str}")
    content = {"status_code": 422, "message": exc_str, "data": None}
    return JSONResponse(
        content=content, status_code=status.HTTP_422_UNPROCESSABLE_ENTITY
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    exc_str = f"{exc}".replace("\n", " ").replace("   ", " ")
    logger.error(f"Request to {request.url} failed validation: {exc_str}")
    content = {"status_code": 422, "message": exc_str, "data": None}
    return JSONResponse(
        content=content, status_code=status.HTTP_422_UNPROCESSABLE_ENTITY
    )


@app.get("/")
async def test(tasks: BackgroundTasks):
    return {"abc": 123, "tasks": tasks.tasks}


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
async def push_unparsed_manifest(args: PushProjectArgs):
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

    if args.install_deps:
        logger.info("Installing deps")
        path = filesystem_service.get_root_path(state_id)
        dbt_service.dbt_deps(path)
        logger.info("Done installing deps")

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
async def parse_project(args: ParseArgs):
    state_id = filesystem_service.get_latest_state_id(args.state_id)
    path = filesystem_service.get_root_path(state_id)
    serialize_path = filesystem_service.get_path(state_id, "manifest.msgpack")

    logger.info("Parsing manifest from filetree")
    logger.info(f"{state_id=}")
    manifest = dbt_service.parse_to_manifest(path, args)

    logger.info("Serializing as messagepack file")
    dbt_service.serialize_manifest(manifest, serialize_path)
    filesystem_service.update_state_id(state_id)

    return JSONResponse(
        status_code=200, content={"parsing": args.state_id, "path": serialize_path}
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


@app.post("/seed-async")
async def seed_async(
    args: SeedArgs,
    background_tasks: BackgroundTasks,
    response_model=schemas.Task,
    db: Session = Depends(crud.get_db),
):
    return task_service.seed_async(background_tasks, db, args)


import uuid
from dbt_server.models import TaskState
from dbt.cli.main import dbtRunner
from fastapi import HTTPException
import uuid
from dbt.exceptions import RuntimeException

from dbt_server import crud, schemas
from dbt_server.services import dbt_service, filesystem_service
from dbt_server.logging import GLOBAL_LOGGER as logger, LogManager
from dbt_server.models import TaskState
from dbt.lib import load_profile_project



class dbtCommandArgs(BaseModel):
    state_id: str
    command: list[str]


def invoke_dbt(task_id, args:dbtCommandArgs, db):
    db_task = crud.get_task(db, task_id)

    path = filesystem_service.get_root_path(args.state_id)
    serialize_path = filesystem_service.get_path(args.state_id, "manifest.msgpack")
    log_path = filesystem_service.get_path(args.state_id, task_id, "logs.stdout")

    log_manager = LogManager(log_path)
    log_manager.setup_handlers()

    logger.info(f"Running dbt ({task_id}) - deserializing manifest {serialize_path}")

    manifest = dbt_service.deserialize_manifest(serialize_path)
    profile, project = load_profile_project(path, os.getenv("DBT_PROFILE_NAME", "user"),)

    crud.set_task_running(db, db_task)

    logger.info(f"Running dbt ({task_id}) - kicking off task")

    try:
        dbt = dbtRunner(project, profile, manifest)
        # TODO we might need to surface this to shipment later on
        res, success = dbt.invoke(args.command)
    except RuntimeException as e:
        crud.set_task_errored(db, db_task, str(e))
        raise e

    logger.info(f"Running dbt ({task_id}) - done")

    log_manager.cleanup()

    crud.set_task_done(db, db_task)

@app.post("/async/dbt/")
async def dbt_entry(
    # background_tasks: BackgroundTasks,
    args: dbtCommandArgs,
    # request: Request,
    # commons: list = Depends(common_parameters),
    # args: list,
    # response_model=schemas.Task,
    background_tasks: BackgroundTasks,
    response_model=schemas.Task,
    db: Session = Depends(crud.get_db),
):  

    # example request: Post http://127.0.0.1:8580/async/dbt
    # with body {"state_id": "123", "command":["run"]}
    task_id = str(uuid.uuid4())
    log_path = filesystem_service.get_path(args.state_id, task_id, "logs.stdout")

    task = schemas.Task(
        task_id=task_id,
        state=TaskState.PENDING,
        command="dbt run-operation",
        log_path=log_path,
    )

    db_task = crud.get_task(db, task_id)
    if db_task:
        raise HTTPException(status_code=400, detail="Task already registered")

    background_tasks.add_task(invoke_dbt, task_id, args, db)
    return crud.create_task(db, task)



@app.post("/preview")
async def preview_sql(sql: SQLConfig):
    state_id = filesystem_service.get_latest_state_id(sql.state_id)
    if state_id is None:
        return JSONResponse(
            status_code=422,
            content={
                "message": "No historical record of a successfully parsed project for this user environment."
            },
        )
    path = filesystem_service.get_root_path(state_id)
    serialize_path = filesystem_service.get_path(state_id, "manifest.msgpack")

    manifest = dbt_service.deserialize_manifest(serialize_path)
    result = dbt_service.execute_sql(manifest, path, sql.sql)
    if type(result) != RemoteRunResult:
        # Theoretically this shouldn't happen-- handling just in case
        return JSONResponse(
            status_code=400,
            content={
                "message": "Something went wrong with sql execution-- please contact support."
            },
        )
    result = result.to_dict()
    encoded_results = jsonable_encoder(result)

    return JSONResponse(
        status_code=200,
        content={
            "parsing": state_id,
            "path": serialize_path,
            "res": encoded_results,
        },
    )


@app.post("/compile")
def compile_sql(sql: SQLConfig):
    state_id = filesystem_service.get_latest_state_id(sql.state_id)
    if state_id is None:
        return JSONResponse(
            status_code=422,
            content={
                "message": "No historical record of a successfully parsed project for this user environment."
            },
        )
    path = filesystem_service.get_root_path(state_id)
    serialize_path = filesystem_service.get_path(state_id, "manifest.msgpack")

    manifest = dbt_service.deserialize_manifest(serialize_path)

    try:
        result = dbt_service.compile_sql(manifest, path, sql.sql)
    except CompilationException as e:
        logger.error(
            f"Failed to compile sql for state_id: {state_id}. Compilation Error: {repr(e)}"
        )
        return JSONResponse(
            status_code=400,
            content={"message": repr(e)},
        )

    if type(result) != RemoteCompileResult:
        # Theoretically this shouldn't happen-- handling just in case
        return JSONResponse(
            status_code=400,
            content={
                "message": "Something went wrong with sql compilation-- please contact support."
            },
        )
    result = result.to_dict()
    encoded_results = jsonable_encoder(result)

    return JSONResponse(
        status_code=200,
        content={
            "parsing": state_id,
            "path": serialize_path,
            "res": encoded_results,
        },
    )


@app.post("/deps")
async def tar_deps(args: DepsArgs):
    package_data = dbt_service.render_package_data(args.packages)
    if not package_data:
        return JSONResponse(
            status_code=400,
            content={
                "message": (
                    "No hub packages found for installation. "
                    "\nPlease contact support if you are receiving this message in error."
                )
            },
        )
    packages = dbt_service.get_package_details(package_data)
    return JSONResponse(status_code=200, content={"res": jsonable_encoder(packages)})


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
