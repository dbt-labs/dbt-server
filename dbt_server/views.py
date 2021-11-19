import os
import json
from dbt.exceptions import RuntimeException
from requests.exceptions import HTTPError

from sse_starlette.sse import EventSourceResponse
from fastapi import FastAPI, BackgroundTasks, Depends
from starlette.requests import Request
from pydantic import BaseModel
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from typing import List, Optional


from .services import filesystem_service
from .services import dbt_service
from .services import task_service
from .logging import GLOBAL_LOGGER as logger

# ORM stuff
from sqlalchemy.orm import Session
from . import crud
from . import schemas

app = FastAPI()


class UnparsedManifestBlob(BaseModel):
    state_id: str
    body: str


class State(BaseModel):
    state_id: str


class DepsArgs(BaseModel):
    packages: Optional[str] = None


class RunArgs(BaseModel):
    state_id: str
    select: Optional[List[str]] = None
    exclude: Optional[List[str]] = None
    single_threaded: bool = False
    state: Optional[str] = None
    selector_name: Optional[str] = None
    defer: Optional[bool] = None
    threads: int = 4


class ListArgs(BaseModel):
    state_id: str
    models: Optional[List[str]] = None
    exclude: Optional[List[str]] = None
    single_threaded: bool = False
    output: str = 'path'
    state: Optional[str] = None
    select: Optional[str] = None
    selector_name: Optional[str] = None
    resource_types: Optional[str] = None
    output_keys: Optional[List[str]] = None
    threads: int = 4


class SQLConfig(BaseModel):
    state_id: Optional[str] = None
    sql: str


@app.exception_handler(RuntimeException)
async def runtime_exception_handler(request: Request, exc: RuntimeException):
    logger.debug(str(exc))
    # TODO: We should look at dbt-cloud's ResponseEnvelope and decide whether or not
    #  to use the same response structure for continuity
    return JSONResponse(
        status_code=400,
        content={"message": str(exc)},
    )


@app.exception_handler(HTTPError)
async def runtime_exception_handler(request: Request, exc: HTTPError):
    logger.debug(str(exc))
    return JSONResponse(
        status_code=exc.response.status_code,
        content={"message": str(exc)},
    )


@app.get("/")
async def test(tasks: BackgroundTasks):
    return {"abc": 123, "tasks": tasks.tasks}


@app.post("/ready")
async def ready():
    return JSONResponse(
        status_code=200,
        content={}
    )


@app.post("/push")
async def push_unparsed_manifest(manifest: UnparsedManifestBlob):
    # Parse / validate it
    state_id = filesystem_service.get_latest_state_id(manifest.state_id)
    body = manifest.body

    logger.info(f"Recieved manifest {len(body)} bytes")

    path = filesystem_service.get_root_path(state_id)
    reuse = True

    # Stupid example of reusing an existing manifest
    if not os.path.exists(path):
        reuse = False
        unparsed_manifest_dict = json.loads(body)
        filesystem_service.write_unparsed_manifest_to_disk(state_id, unparsed_manifest_dict)

    # Write messagepack repr to disk
    # Return a key that the client can use to operate on it?
    return JSONResponse(
        status_code=200,
        content={
            "state": state_id,
            "bytes": len(body),
            "reuse": reuse,
            "path": path,
        }
    )


@app.post("/parse")
def parse_project(state: State):
    state_id = filesystem_service.get_latest_state_id(state.state_id)
    path = filesystem_service.get_root_path(state_id)
    serialize_path = filesystem_service.get_path(state_id, 'manifest.msgpack')

    logger.info("Parsing manifest from filetree")
    manifest = dbt_service.parse_to_manifest(path)

    logger.info("Serializing as messagepack file")
    dbt_service.serialize_manifest(manifest, serialize_path)
    filesystem_service.update_state_id(state_id)

    return JSONResponse(
        status_code=200,
        content={"parsing": state.state_id, "path": serialize_path}
    )


@app.post("/run")
async def run_models(args: RunArgs):
    state_id = filesystem_service.get_latest_state_id(args.state_id)
    path = filesystem_service.get_root_path(state_id)
    serialize_path = filesystem_service.get_path(state_id, 'manifest.msgpack')

    manifest = dbt_service.deserialize_manifest(serialize_path)
    results = dbt_service.dbt_run_sync(path, args, manifest)

    encoded_results = jsonable_encoder(results)

    return JSONResponse(
        status_code=200,
        content={
            "parsing": args.state_id,
            "path": serialize_path,
            "res": encoded_results,
        }
    )

@app.post("/list")
async def list_resources(args: ListArgs):
    state_id = filesystem_service.get_latest_state_id(args.state_id)
    path = filesystem_service.get_root_path(state_id)
    serialize_path = filesystem_service.get_path(state_id, 'manifest.msgpack')

    manifest = dbt_service.deserialize_manifest(serialize_path)
    results = dbt_service.dbt_list(path, args, manifest)

    encoded_results = jsonable_encoder(results)

    return JSONResponse(
        status_code=200,
        content={
            "parsing": args.state_id,
            "path": serialize_path,
            "res": encoded_results,
        }
    )


@app.post("/run-async")
async def run_models_async(
    args: RunArgs,
    background_tasks: BackgroundTasks,
    response_model=schemas.Task,
    db: Session = Depends(crud.get_db)
):
    return task_service.run_async(background_tasks, db, args)


@app.post("/preview")
async def preview_sql(sql: SQLConfig):
    state_id = filesystem_service.get_latest_state_id(sql.state_id)
    path = filesystem_service.get_root_path(state_id)
    serialize_path = filesystem_service.get_path(state_id, 'manifest.msgpack')

    manifest = dbt_service.deserialize_manifest(serialize_path)
    result = dbt_service.execute_sql(manifest, path, sql.sql)
    encoded_results = jsonable_encoder(result)

    return JSONResponse(
        status_code=200,
        content={
            "parsing": state_id,
            "path": serialize_path,
            "res": encoded_results,
        }
    )


@app.post("/compile")
async def compile_sql(sql: SQLConfig):
    state_id = filesystem_service.get_latest_state_id(sql.state_id)
    path = filesystem_service.get_root_path(state_id)
    serialize_path = filesystem_service.get_path(state_id, 'manifest.msgpack')

    manifest = dbt_service.deserialize_manifest(serialize_path)
    result = dbt_service.compile_sql(manifest, path, sql.sql)
    encoded_results = jsonable_encoder(result)

    return JSONResponse(
        status_code=200,
        content={
            "parsing": state_id,
            "path": serialize_path,
            "res": encoded_results,
        }
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
            }
        )
    packages = dbt_service.get_package_details(package_data)
    return JSONResponse(
        status_code=200,
        content={
            "res": jsonable_encoder(packages)
        }
    )


class Task(BaseModel):
    task_id: str


@app.get('/stream-logs/{task_id}')
async def log_endpoint(
    task_id: str,
    request: Request,
    db: Session = Depends(crud.get_db),
):
    event_generator = task_service.tail_logs_for_path(db, task_id, request)
    return EventSourceResponse(event_generator, ping=2)
