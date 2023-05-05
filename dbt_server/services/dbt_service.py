import os
import threading

from datetime import datetime

# dbt Core imports
import dbt.tracking
import dbt.adapters.factory
from dbt.contracts.graph.manifest import Manifest

try:
    from dbt.cli.main import dbtRunnerResult
except (ModuleNotFoundError, ImportError):
    dbtRunnerResult = None
from dbt_server.helpers import get_profile_name

# These exceptions were removed in v1.4
try:
    from dbt.exceptions import (
        CompilationException,
        InvalidConnectionException,
    )
except (ModuleNotFoundError, ImportError):
    from dbt.exceptions import (
        CompilationError as CompilationException,
        InvalidConnectionError as InvalidConnectionException,
    )


from dbt.contracts.sql import RemoteCompileResult

# dbt Server imports
from dbt_server.services import filesystem_service
from dbt_server.logging import DBT_SERVER_LOGGER as logger
from dbt_server import tracer
from dbt.cli.main import dbtRunner


from dbt_server.exceptions import (
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


def handle_dbt_compilation_error(func):
    def inner(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.exception("Unhandled error from dbt Core")
            raise dbtCoreCompilationException(str(e))

    return inner


def disable_tracking():
    # TODO: why does this mess with stuff
    dbt.tracking.disable_tracking()


@tracer.wrap
def parse_to_manifest(project_path, args):
    try:
        profile_name = get_profile_name(args)
        # Target can be specified at the semantic layer level. If this field comes over as
        # an empty string instead of null, dbt will not use the default target name and
        # parsing will fail
        target_name = os.environ.get("__DBT_TARGET_NAME", None)
        if target_name == "":
            target_name = None

        # Parse command return a manifest in result.result
        # We can also specify target dir to control where manifest.msgpack is saved
        result = dbtRunner().invoke(
            ["parse"],
            project_dir=project_path,
            profile=profile_name,
            send_anonymous_usage_stats=False,
            target=target_name,
            write_json=False,
            write_manifest=False,
        )
        if result and type(result) == dbtRunnerResult and not result.success:
            # If task had unhandled errors, raise
            if result.exception:
                raise result.exception
        return result.result
    except CompilationException as e:
        logger.error(
            f"Failed to parse manifest at {project_path}. Compilation Error: {repr(e)}"
        )
        raise dbtCoreCompilationException(e)


# TODO: it would be nice to just save manifest using parse command from dbt-core, but there seems
# to be a lot of state management that I don't want to touch.
@tracer.wrap
def serialize_manifest(manifest, serialize_path):
    filesystem_service.write_file(serialize_path, manifest.to_msgpack())


@tracer.wrap
def deserialize_manifest(serialize_path):
    manifest_packed = filesystem_service.read_serialized_manifest(serialize_path)
    return Manifest.from_msgpack(manifest_packed)


@handle_dbt_compilation_error
@tracer.wrap
def compile_sql(manifest, project_dir, sql_config):
    # Currently this command will load project and profile from disk
    # It uses the manifest passed in
    try:
        profile_name = get_profile_name(sql_config)
        # Invoke dbtRunner to compile SQL code
        # TODO skip relational cache.
        run_result = dbtRunner(manifest=manifest).invoke(
            ["compile", "--inline", sql_config.sql],
            profile=profile_name,
            project_dir=project_dir,
            introspect=False,
            send_anonymous_usage_stats=False,
            populate_cache=False,
            write_json=False,
            write_manifest=False,
        )
        # dbt-core 1.5.0-latest changes the return type from a tuple to a
        # dbtRunnerResult obj and no longer raises exceptions on invoke
        if (
            run_result
            and type(run_result) == dbtRunnerResult
            and not run_result.success
        ):
            # If task had unhandled errors, raise
            if run_result.exception:
                raise run_result.exception
        # convert to RemoteCompileResult to keep original return format
        node_result = run_result.result.results[0]
        result = RemoteCompileResult(
            raw_code=node_result.node.raw_code,
            compiled_code=node_result.node.compiled_code,
            node=node_result.node,
            timing=node_result.timing,
            logs=[],
            generated_at=datetime.utcnow(),
        )

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
