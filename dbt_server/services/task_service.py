import uuid
from dbt.exceptions import RuntimeException

from dbt_server import crud, schemas
from dbt_server.services import dbt_service, filesystem_service
from dbt_server.logging import GLOBAL_LOGGER as logger, LogManager, ServerLog
from dbt_server.models import TaskState

from fastapi import HTTPException
import asyncio
import io


def run_task(task_name, task_id, args, db):
    db_task = crud.get_task(db, task_id)

    path = filesystem_service.get_root_path(args.state_id)
    serialize_path = filesystem_service.get_path(args.state_id, "manifest.msgpack")
    log_path = filesystem_service.get_path(args.state_id, task_id, "logs.stdout")

    log_manager = LogManager(log_path)
    log_manager.setup_handlers()

    logger.info(f"Running dbt ({task_id}) - deserializing manifest {serialize_path}")

    manifest = dbt_service.deserialize_manifest(serialize_path)

    crud.set_task_running(db, db_task)

    logger.info(f"Running dbt ({task_id}) - kicking off task")

    try:
        if task_name == "run":
            dbt_service.dbt_run(path, args, manifest)
        elif task_name == "seed":
            dbt_service.dbt_seed(path, args, manifest)
        elif task_name == "test":
            dbt_service.dbt_test(path, args, manifest)
        elif task_name == "build":
            dbt_service.dbt_build(path, args, manifest)
        elif task_name == "snapshot":
            dbt_service.dbt_snapshot(path, args, manifest)
        elif task_name == "run_operation":
            dbt_service.dbt_run_operation(path, args, manifest)
        else:
            raise RuntimeException("Not an actual task")
    except RuntimeException as e:
        crud.set_task_errored(db, db_task, str(e))
        raise e

    logger.info(f"Running dbt ({task_id}) - done")

    log_manager.cleanup()

    crud.set_task_done(db, db_task)


def run_async(background_tasks, db, args):
    task_id = str(uuid.uuid4())
    log_path = filesystem_service.get_path(args.state_id, task_id, "logs.stdout")

    task = schemas.Task(
        task_id=task_id, state=TaskState.PENDING, command="dbt run", log_path=log_path
    )

    db_task = crud.get_task(db, task_id)
    if db_task:
        raise HTTPException(status_code=400, detail="Task already registered")

    background_tasks.add_task(run_task, "run", task_id, args, db)
    return crud.create_task(db, task)


def test_async(background_tasks, db, args):
    task_id = str(uuid.uuid4())
    log_path = filesystem_service.get_path(args.state_id, task_id, "logs.stdout")

    task = schemas.Task(
        task_id=task_id, state=TaskState.PENDING, command="dbt test", log_path=log_path
    )

    db_task = crud.get_task(db, task_id)
    if db_task:
        raise HTTPException(status_code=400, detail="Task already registered")

    background_tasks.add_task(run_task, "test", task_id, args, db)
    return crud.create_task(db, task)


def seed_async(background_tasks, db, args):
    task_id = str(uuid.uuid4())
    log_path = filesystem_service.get_path(args.state_id, task_id, "logs.stdout")

    task = schemas.Task(
        task_id=task_id, state=TaskState.PENDING, command="dbt seed", log_path=log_path
    )

    db_task = crud.get_task(db, task_id)
    if db_task:
        raise HTTPException(status_code=400, detail="Task already registered")

    background_tasks.add_task(run_task, "seed", task_id, args, db)
    return crud.create_task(db, task)


def build_async(background_tasks, db, args):
    task_id = str(uuid.uuid4())
    log_path = filesystem_service.get_path(args.state_id, task_id, "logs.stdout")

    task = schemas.Task(
        task_id=task_id, state=TaskState.PENDING, command="dbt build", log_path=log_path
    )

    db_task = crud.get_task(db, task_id)
    if db_task:
        raise HTTPException(status_code=400, detail="Task already registered")

    background_tasks.add_task(run_task, "build", task_id, args, db)
    return crud.create_task(db, task)


def run_operation_async(background_tasks, db, args):
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

    background_tasks.add_task(run_task, "run_operation", task_id, args, db)
    return crud.create_task(db, task)


def snapshot_async(background_tasks, db, args):
    task_id = str(uuid.uuid4())
    log_path = filesystem_service.get_path(args.state_id, task_id, "logs.stdout")

    task = schemas.Task(
        task_id=task_id,
        state=TaskState.PENDING,
        command="dbt snapshot",
        log_path=log_path,
    )

    db_task = crud.get_task(db, task_id)
    if db_task:
        raise HTTPException(status_code=400, detail="Task already registered")

    background_tasks.add_task(run_task, "snapshot", task_id, args, db)
    return crud.create_task(db, task)


async def _wait_for_file(path):
    for _ in range(10):
        try:
            return open(path)
        except FileNotFoundError:
            # TODO : Remove / debugging
            logger.info(f"Waiting for file handle @ {path}")
            await asyncio.sleep(0.5)
            continue
    else:
        raise RuntimeError("No log file appeared in designated timeout")


async def _read_until_empty(fh):
    while True:
        line = fh.readline()
        if len(line) == 0:
            break
        else:
            yield line


async def tail_logs_for_path(db, task_id, request, live=True):
    db_task = crud.get_task(db, task_id)
    logger.info(f"Waiting for file @ {db_task.log_path}")
    fh = await _wait_for_file(db_task.log_path)

    if live:
        fh.seek(0, io.SEEK_END)
    try:
        while db_task.state not in (TaskState.ERROR, TaskState.FINISHED):
            if await request.is_disconnected():
                logger.debug("Log request disconnected")
                break
            async for log in _read_until_empty(fh):
                yield log
            await asyncio.sleep(0.5)
            db.refresh(db_task)

        # Drain any lines accumulated after end of task
        # If we didn't do this, some lines could be omitted
        logger.info("Draining logs from file")
        async for log in _read_until_empty(fh):
            yield log

    finally:
        yield ServerLog(state=db_task.state, error=db_task.error).to_json()
        fh.close()
