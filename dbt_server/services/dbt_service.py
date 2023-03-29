import os
import threading
import uuid
from inspect import getmembers, isfunction
from typing import List, Optional, Any

# dbt Core imports
import dbt.tracking
import dbt.lib
import dbt.adapters.factory
import requests
from requests.adapters import HTTPAdapter

from sqlalchemy.orm import Session
from urllib3 import Retry

# These exceptions were removed in v1.4
try:
    from dbt.exceptions import (
        ValidationException,
        CompilationException,
        InvalidConnectionException,
    )
except (ModuleNotFoundError, ImportError):
    from dbt.exceptions import (
        DbtValidationError as ValidationException,
        CompilationError as CompilationException,
        InvalidConnectionError as InvalidConnectionException,
    )

from dbt.lib import (
    get_dbt_config as dbt_get_dbt_config,
    parse_to_manifest as dbt_parse_to_manifest,
    execute_sql as dbt_execute_sql,
    deserialize_manifest as dbt_deserialize_manifest,
    serialize_manifest as dbt_serialize_manifest,
    SqlCompileRunnerNoIntrospection,
)


from dbt.parser.sql import SqlBlockParser
from dbt.parser.manifest import process_node

from dbt.contracts.sql import (
    RemoteRunResult,
    RemoteCompileResult,
)

# dbt Server imports
from dbt_server.services import filesystem_service
from dbt_server.logging import DBT_SERVER_LOGGER as logger
from dbt_server.helpers import get_profile_name
from dbt_server import crud, tracer, models
from dbt.lib import load_profile_project
from dbt.cli.main import dbtRunner


from dbt_server.exceptions import (
    InvalidConfigurationException,
    InternalException,
    dbtCoreCompilationException,
    UnsupportedQueryException,
)
from pydantic import BaseModel

ALLOW_INTROSPECTION = str(os.environ.get("__DBT_ALLOW_INTROSPECTION", "1")).lower() in (
    "true",
    "1",
    "on",
)

CONFIG_GLOBAL_LOCK = threading.Lock()


class Args(BaseModel):
    profile: str = None


def inject_dd_trace_into_core_lib():

    for attr_name, attr in getmembers(dbt.lib):
        if not isfunction(attr):
            continue

        setattr(dbt.lib, attr_name, tracer.wrap(attr))


def handle_dbt_compilation_error(func):
    def inner(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.exception("Unhandled error from dbt Core")
            raise dbtCoreCompilationException(str(e))

    return inner


def patch_adapter_config(config):
    # This is a load-bearing assignment. Adapters cache the config they are
    # provided (oh my!) and they are represented as singletons, so it's not
    # actually possible to invoke dbt twice concurrently with two different
    # configs that differ substantially. Fortunately, configs _mostly_ carry
    # credentials. The risk here is around changes to other config-type code,
    # like packages.yml contents or the name of the project.
    #
    # This quickfix is intended to support sequential requests with
    # differing project names. It will _not_ work with concurrent requests, as one
    # of those requests will get a surprising project_name in the adapter's
    # cached RuntimeConfig object.
    #
    # If this error is hit in practice, it will manifest as something like:
    #      Node package named fishtown_internal_analytics not found!
    #
    # because Core is looking for packages in the wrong namespace. This
    # unfortunately isn't something we can readily catch programmatically
    # & it must be fixed upstream in dbt Core.
    adapter = dbt.adapters.factory.get_adapter(config)
    adapter.config = config
    return adapter


def generate_node_name():
    return str(uuid.uuid4()).replace("-", "_")


@tracer.wrap
def get_sql_parser(config, manifest):
    return SqlBlockParser(
        project=config,
        manifest=manifest,
        root_project=config,
    )


@tracer.wrap
def create_dbt_config(project_path, args=None):
    try:
        if not args:
            args = Args()
        if hasattr(args, "profile"):
            args.profile = get_profile_name(args)
        # This needs a lock to prevent two threads from mutating an adapter concurrently
        with CONFIG_GLOBAL_LOCK:
            return dbt_get_dbt_config(project_path, args)
    except ValidationException:
        # Some types of dbt config exceptions may contain sensitive information
        # eg. contents of a profiles.yml file for invalid/malformed yml.
        # Raise a new exception to mask the original backtrace and suppress
        # potentially sensitive information.
        raise InvalidConfigurationException(
            "Invalid dbt config provided. Check that your credentials are configured"
            " correctly and a valid dbt project is present"
        )


def disable_tracking():
    # TODO: why does this mess with stuff

    dbt.tracking.disable_tracking()


@tracer.wrap
def parse_to_manifest(project_path, args):
    try:
        config = create_dbt_config(project_path, args)
        patch_adapter_config(config)
        return dbt_parse_to_manifest(config)
    except CompilationException as e:
        logger.error(
            f"Failed to parse manifest at {project_path}. Compilation Error: {repr(e)}"
        )
        raise dbtCoreCompilationException(e)


@tracer.wrap
def serialize_manifest(manifest, serialize_path):
    manifest_msgpack = dbt_serialize_manifest(manifest)
    filesystem_service.write_file(serialize_path, manifest_msgpack)


@tracer.wrap
def deserialize_manifest(serialize_path):
    manifest_packed = filesystem_service.read_serialized_manifest(serialize_path)
    return dbt_deserialize_manifest(manifest_packed)


@handle_dbt_compilation_error
@tracer.wrap
def execute_sql(manifest, project_path, sql):
    try:
        node_name = generate_node_name()
        result = dbt_execute_sql(manifest, project_path, sql, node_name)
    except CompilationException as e:
        logger.error(
            f"Failed to compile sql at {project_path}. Compilation Error: {repr(e)}"
        )
        raise dbtCoreCompilationException(e)

    if type(result) != RemoteRunResult:
        # Theoretically this shouldn't happen-- handling just in case
        raise InternalException(
            f"Got unexpected result type ({type(result)}) from dbt Core"
        )

    return result.to_dict()


@handle_dbt_compilation_error
@tracer.wrap
def compile_sql(manifest, config, parser, sql):
    try:
        node_name = generate_node_name()
        adapter = patch_adapter_config(config)

        sql_node = parser.parse_remote(sql, node_name)
        process_node(config, manifest, sql_node)

        runner = SqlCompileRunnerNoIntrospection(config, adapter, sql_node, 1, 1)
        result = runner.safe_run(manifest)

    except InvalidConnectionException:
        if ALLOW_INTROSPECTION:
            # Raise original error if introspection is not disabled
            # and therefore errors are unexpected
            raise
        else:
            msg = "This dbt server environment does not support introspective queries. \n Hint: typically introspective queries use the 'run_query' jinja command or a macro that invokes that command"
            logger.exception(msg)
            raise UnsupportedQueryException(msg)
    except CompilationException as e:
        logger.error(f"Failed to compile sql. Compilation Error: {repr(e)}")
        raise dbtCoreCompilationException(e)

    if type(result) != RemoteCompileResult:
        # Theoretically this shouldn't happen-- handling just in case
        raise InternalException(
            f"Got unexpected result type ({type(result)}) from dbt Core"
        )

    return result.to_dict()


def execute_async_command(
    command: List,
    task_id: str,
    root_path: str,
    manifest: Any,
    db: Session,
    state_id: Optional[str] = None,
    callback_url: Optional[str] = None,
) -> None:
    db_task = crud.get_task(db, task_id)
    # For commands, only the log file destination directory is sent to --log-path
    log_dir_path = filesystem_service.get_task_artifacts_path(task_id, state_id)

    # Temporary solution for structured log formatting until core adds a cleaner interface
    new_command = []
    new_command.append("--log-format")
    new_command.append("json")
    new_command.append("--log-path")
    new_command.append(log_dir_path)
    new_command += command

    logger.info(f"Running dbt ({task_id})")

    # TODO: this is a tmp solution to set profile_dir to global flags
    # we should provide a better programatical interface of core to sort out
    # the creation of project, profile
    from dbt.flags import set_from_args
    from argparse import Namespace
    from dbt.cli.resolvers import default_profiles_dir

    if os.getenv("DBT_PROFILES_DIR"):
        profiles_dir = os.getenv("DBT_PROFILES_DIR")
    else:
        profiles_dir = default_profiles_dir()
    set_from_args(Namespace(profiles_dir=profiles_dir), None)

    # TODO: If a command contains a --profile flag, how should we access/pass it?
    profile_name = get_profile_name()
    profile, project = load_profile_project(root_path, profile_name)

    update_task_status(db, db_task, callback_url, models.TaskState.RUNNING, None)

    logger.info(f"Running dbt ({task_id}) - kicking off task")

    # Passing a custom target path is not currently working through the
    # core API. As a result, the target is defaulting to a relative `./dbt_packages`
    # This chdir action is taken in core for several commands, but not for others,
    # which can result in a packages dir creation at the app root.
    # Until custom target paths are supported, this will ensure package folders are created
    # at the project root.
    dbt_server_root = os.getcwd()
    try:
        os.chdir(root_path)
        dbt = dbtRunner(project, profile, manifest)
        _, _ = dbt.invoke(new_command)
    except Exception as e:
        update_task_status(db, db_task, callback_url, models.TaskState.ERROR, str(e))
        raise e
    finally:
        # Return to dbt server root
        os.chdir(dbt_server_root)

    logger.info(f"Running dbt ({task_id}) - done")

    update_task_status(db, db_task, callback_url, models.TaskState.FINISHED, None)


@tracer.wrap
def execute_sync_command(command: List, root_path: str, manifest: Any):
    str_command = (" ").join(str(param) for param in command)
    logger.info(
        f"Running dbt ({str_command}) - deserializing manifest found at {root_path}"
    )

    # TODO: If a command contains a --profile flag, how should we access/pass it?
    profile_name = get_profile_name()
    profile, project = load_profile_project(root_path, profile_name)

    logger.info(f"Running dbt ({str_command})")

    dbt = dbtRunner(project, profile, manifest)
    return dbt.invoke(command)


def update_task_status(db, db_task, callback_url, status, error):
    crud.set_task_state(db, db_task, status, error)

    if callback_url:
        retries = Retry(total=5, allowed_methods=frozenset(["POST"]))

        session = requests.Session()
        session.mount("http://", HTTPAdapter(max_retries=retries))
        session.post(callback_url, json={"task_id": db_task.task_id, "status": status})
