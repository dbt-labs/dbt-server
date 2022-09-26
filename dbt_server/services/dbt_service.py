import os
import threading

from dbt_server.services import filesystem_service
from dbt_server.exceptions import (
    InvalidConfigurationException,
    InternalException,
    dbtCoreCompilationException,
    UnsupportedQueryException,
)
from dbt_server import tracer

from dbt.exceptions import (
    ValidationException,
    CompilationException,
)
from dbt.lib import (
    create_task,
    get_dbt_config,
    parse_to_manifest as dbt_parse_to_manifest,
    execute_sql as dbt_execute_sql,
    compile_sql as dbt_compile_sql,
    deserialize_manifest as dbt_deserialize_manifest,
    serialize_manifest as dbt_serialize_manifest,
)
from dbt.contracts.sql import (
    RemoteRunResult,
    RemoteCompileResult,
)

from dbt_server.logging import GLOBAL_LOGGER as logger
from dbt.exceptions import InvalidConnectionException


# Temporary default to match dbt-cloud behavior
PROFILE_NAME = os.getenv("DBT_PROFILE_NAME", "user")
ALLOW_INTROSPECTION = str(os.environ.get("__DBT_ALLOW_INTROSPECTION", "1")).lower() in (
    "true",
    "1",
    "on",
)

CONFIG_GLOBAL_LOCK = threading.Lock()


def handle_dbt_compilation_error(func):
    def inner(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.exception("Unhandled error from dbt Core")
            raise dbtCoreCompilationException(str(e))

    return inner


@tracer.wrap
def _get_dbt_config(project_path, args):
    # This function exists to trace the underlying dbt call
    return get_dbt_config(project_path, args)


@tracer.wrap
def create_dbt_config(project_path, args):
    args.profile = PROFILE_NAME

    # Some types of dbt config exceptions may contain sensitive information
    # eg. contents of a profiles.yml file for invalid/malformed yml.
    # Raise a new exception to mask the original backtrace and suppress
    # potentially sensitive information.
    try:
        with CONFIG_GLOBAL_LOCK:
            return _get_dbt_config(project_path, args)
    except ValidationException:
        raise InvalidConfigurationException(
            "Invalid dbt config provided. Check that your credentials are configured"
            " correctly and a valid dbt project is present"
        )


def disable_tracking():
    # TODO: why does this mess with stuff
    import dbt.tracking

    dbt.tracking.disable_tracking()


@tracer.wrap
def parse_to_manifest(project_path, args):
    try:
        config = create_dbt_config(project_path, args)
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
    manifest_packed = filesystem_service.read_file(serialize_path)
    return dbt_deserialize_manifest(manifest_packed)


def dbt_run(project_path, args, manifest):
    config = create_dbt_config(project_path, args)
    task = create_task("run", args, manifest, config)
    return task.run()


def dbt_test(project_path, args, manifest):
    config = create_dbt_config(project_path, args)
    task = create_task("test", args, manifest, config)
    return task.run()


def dbt_list(project_path, args, manifest):
    config = create_dbt_config(project_path, args)
    task = create_task("list", args, manifest, config)
    return task.run()


def dbt_seed(project_path, args, manifest):
    config = create_dbt_config(project_path, args)
    task = create_task("seed", args, manifest, config)
    return task.run()


def dbt_build(project_path, args, manifest):
    config = create_dbt_config(project_path, args)
    task = create_task("build", args, manifest, config)
    return task.run()


def dbt_run_operation(project_path, args, manifest):
    config = create_dbt_config(project_path, args)
    task = create_task("run_operation", args, manifest, config)
    return task.run()


def dbt_snapshot(project_path, args, manifest):
    config = create_dbt_config(project_path, args)
    task = create_task("snapshot", args, manifest, config)
    return task.run()


@handle_dbt_compilation_error
@tracer.wrap
def execute_sql(manifest, project_path, sql):
    try:
        result = dbt_execute_sql(manifest, project_path, sql)
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
def compile_sql(manifest, project_path, sql):
    try:
        result = dbt_compile_sql(manifest, project_path, sql)
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
        logger.error(
            f"Failed to compile sql at {project_path}. Compilation Error: {repr(e)}"
        )
        raise dbtCoreCompilationException(e)

    if type(result) != RemoteCompileResult:
        # Theoretically this shouldn't happen-- handling just in case
        raise InternalException(
            f"Got unexpected result type ({type(result)}) from dbt Core"
        )

    return result.to_dict()
