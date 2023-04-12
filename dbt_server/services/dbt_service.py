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

import os
from dbt.config.project import Project
from dataclasses import dataclass
from dbt.cli.resolvers import default_profiles_dir
from dbt.config.runtime import load_profile, load_project
from dbt.flags import set_from_args


@dataclass
class RuntimeArgs:
    project_dir: str
    profiles_dir: str
    single_threaded: bool
    profile: str
    target: str

@tracer.wrap
def load_profile_project(project_dir, profile_name_override=None):
    profile = load_profile(project_dir, {}, profile_name_override)
    project = load_project(project_dir, False, profile, {})
    return profile, project

@tracer.wrap
def dbt_get_dbt_config(project_dir, args=None, single_threaded=False):
    from dbt.config.runtime import RuntimeConfig
    import dbt.adapters.factory
    import dbt.events.functions

    if os.getenv("DBT_PROFILES_DIR"):
        profiles_dir = os.getenv("DBT_PROFILES_DIR")
    else:
        profiles_dir = default_profiles_dir()

    profile_name = getattr(args, "profile", None)

    runtime_args = RuntimeArgs(
        project_dir=project_dir,
        profiles_dir=profiles_dir,
        single_threaded=single_threaded,
        profile=profile_name,
        target=getattr(args, "target", None),
    )

    # set global flags from arguments
    set_from_args(runtime_args, None)
    profile, project = load_profile_project(project_dir, profile_name)
    assert type(project) is Project

    config = RuntimeConfig.from_parts(project, profile, runtime_args)

    # the only thing this set_from_args does differently than
    # the one above is that it pass runtime config over, I don't think
    # we need that. but leaving this for now for future reference

    # flags.set_from_args(runtime_args, config)

    # This is idempotent, so we can call it repeatedly
    dbt.adapters.factory.register_adapter(config)

    # Make sure we have a valid invocation_id
    dbt.events.functions.set_invocation_id()
    dbt.events.functions.reset_metadata_vars()

    return config

@tracer.wrap
def dbt_parse_to_manifest(config):
    from dbt.parser.manifest import ManifestLoader

    return ManifestLoader.get_full_manifest(config)

@tracer.wrap
def dbt_deserialize_manifest(manifest_msgpack):
    from dbt.contracts.graph.manifest import Manifest

    return Manifest.from_msgpack(manifest_msgpack)

@tracer.wrap
def dbt_serialize_manifest(manifest):
    return manifest.to_msgpack()


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