import os
import threading
import uuid
from inspect import getmembers, isfunction

from datetime import datetime

# dbt Core imports
import dbt.tracking
import dbt.adapters.factory

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
    deserialize_manifest as dbt_deserialize_manifest,
    serialize_manifest as dbt_serialize_manifest,
)

from dbt.contracts.sql import RemoteCompileResult

# dbt Server imports
from dbt_server.services import filesystem_service
from dbt_server.logging import DBT_SERVER_LOGGER as logger
from dbt_server.helpers import get_profile_name
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


# TODO replace then following 2 function with just getting manifest from dbt parse and specify path to save manifest
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
def compile_sql(manifest, project_dir, sql):
    # Currently this command will load project and profile from disk
    # It uses the manifest passed in
    try:
        # Invoke dbtRunner to compile SQL code
        # TODO skip relational cache.
        run_result, _ = dbtRunner(
            manifest=manifest
        ).invoke(
            ["compile", "--inline", sql],
            project_dir=project_dir,
            introspect=False,
            send_anonymous_usage_stats=False
        )
        # convert to RemoteCompileResult to keep original return format
        node_result = run_result.results[0]
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