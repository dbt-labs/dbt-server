import os
import threading
import uuid
from inspect import getmembers, isfunction

from datetime import datetime

# dbt Core imports
import dbt.tracking
import dbt.adapters.factory
from dbt.contracts.graph.manifest import Manifest

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


from dbt.contracts.sql import RemoteCompileResult

# dbt Server imports
from dbt_server.services import filesystem_service
from dbt_server.logging import DBT_SERVER_LOGGER as logger
from dbt_server import tracer
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

import os
from dbt.config.project import Project
from dataclasses import dataclass
from dbt.cli.resolvers import default_profiles_dir
from dbt.config.runtime import load_profile, load_project
from dbt.flags import set_from_args


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
        
        # If no profile name is passed in args, we will attempt to get it from env vars
        # If profile is None, dbt will default to reading from dbt_project.yml
        env_profile_name = os.getenv("DBT_PROFILE_NAME")
        if args and hasattr(args, "profile") and args.profile:
            profile_name = args.profile
        elif env_profile_name:
            profile_name = env_profile_name
        else:
            profile_name = None

        # TODO is this actually needed?
        target_name = os.environ.get("__DBT_TARGET_NAME", None)
        if target_name == "":
            target_name = None
        
        # Parse command return a manifest in result.result
        result = dbtRunner().invoke(
            ["parse"],
            project_dir=project_path,
            profile=profile_name,
            send_anonymous_usage_stats=False,
            target=target_name,
        )
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
def compile_sql(manifest, project_dir, sql):
    # Currently this command will load project and profile from disk
    # It uses the manifest passed in
    try:
        # Invoke dbtRunner to compile SQL code
        # TODO skip relational cache.
        run_result = dbtRunner(
            manifest=manifest
        ).invoke(
            ["compile", "--inline", sql],
            project_dir=project_dir,
            introspect=False,
            send_anonymous_usage_stats=False,
            populate_cache=False,
        )
        # core will not raise an exception in runner, it will just return it in the result
        if not run_result.success:
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