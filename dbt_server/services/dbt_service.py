import json
import os
from collections import namedtuple
from typing import Optional

from pydantic import BaseModel

from dbt_server.services import filesystem_service
from dbt_server.exceptions import InvalidConfigurationException

from dbt.clients.registry import package_version, get_available_versions
from dbt import semver
from dbt.exceptions import (
    VersionsNotCompatibleException,
    ValidationException,
    DependencyException,
    package_version_not_found,
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
from dbt_server.logging import GLOBAL_LOGGER as logger


# Temporary default to match dbt-cloud behavior
PROFILE_NAME = os.getenv("DBT_PROFILE_NAME", "user")


def create_dbt_config(project_path, args):
    args.profile = PROFILE_NAME

    # Some types of dbt config exceptions may contain sensitive information
    # eg. contents of a profiles.yml file for invalid/malformed yml.
    # Raise a new exception to mask the original backtrace and suppress
    # potentially sensitive information.
    try:
        return get_dbt_config(project_path, args)
    except ValidationException:
        raise InvalidConfigurationException(
            "Invalid dbt config provided. Check that your credentials are configured"
            " correctly and a valid dbt project is present"
        )


def disable_tracking():
    # TODO: why does this mess with stuff
    import dbt.tracking

    dbt.tracking.disable_tracking()


def parse_to_manifest(project_path, args):
    config = create_dbt_config(project_path, args)
    return dbt_parse_to_manifest(config)


def serialize_manifest(manifest, serialize_path):
    manifest_msgpack = dbt_serialize_manifest(manifest)
    filesystem_service.write_file(serialize_path, manifest_msgpack)


def deserialize_manifest(serialize_path):
    manifest_packed = filesystem_service.read_file(serialize_path)
    return dbt_deserialize_manifest(manifest_packed)


def dbt_deps(project_path):
    # TODO: add this to dbt lib! this is not great
    from dbt.task.deps import DepsTask
    from dbt.config.runtime import UnsetProfileConfig
    from dbt import flags
    import dbt.adapters.factory
    import dbt.events.functions

    disable_tracking()

    class Args(BaseModel):
        profile: Optional[str] = None
        target: Optional[str] = None
        single_threaded: Optional[bool] = None
        threads: Optional[int] = None

    if os.getenv("DBT_PROFILES_DIR"):
        profiles_dir = os.getenv("DBT_PROFILES_DIR")
    else:
        profiles_dir = os.path.expanduser("~/.dbt")

    RuntimeArgs = namedtuple(
        "RuntimeArgs", "project_dir profiles_dir single_threaded which"
    )

    # Construct a phony config
    try:
        config = UnsetProfileConfig.from_args(
            RuntimeArgs(project_path, profiles_dir, True, "deps")
        )
    except ValidationException:
        raise InvalidConfigurationException(
            "Invalid dbt config provided. Check that your credentials are configured"
            " correctly and a valid dbt project is present"
        )
    # Clear previously registered adapters--
    # this fixes cacheing behavior on the dbt-server
    flags.set_from_args("", config)
    dbt.adapters.factory.reset_adapters()
    # Set invocation id
    dbt.events.functions.set_invocation_id()
    task = DepsTask(Args(), config)

    # TODO: 🤦 reach into dbt-core
    task.config.packages_install_path = os.path.join(project_path, "dbt_packages")

    return task.run()


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


def execute_sql(manifest, project_path, sql):
    return dbt_execute_sql(manifest, project_path, sql)


def compile_sql(manifest, project_path, sql):
    return dbt_compile_sql(manifest, project_path, sql)


def render_package_data(packages):
    return json.loads(packages)


def get_package_details(package_data):
    packages = []
    for package in package_data.get("packages", {}):
        full_name = package.get("package")
        version = resolve_version(package)
        if not full_name or not version:
            # TODO: Something better than this for detecting Hub packages?
            logger.debug(
                f"Skipping package: {package}. "
                "Either non-hub package or missing package version."
            )
            continue
        tar_name = "{}.{}.tar.gz".format(full_name, version)
        package_details = package_version(full_name, version)
        tarball = package_details.get("downloads", {}).get("tarball")
        name = package_details.get("name")
        packages.append(
            {
                # Hack to imitate core package name
                "package": f"{full_name}@{version}",
                "name": name,
                "version": version,
                "tar_name": tar_name,
                "tarball": tarball,
            }
        )
    return packages


def resolve_version(package) -> str:
    versions = package.get("version")
    install_prerelease = package.get("install-prerelease")
    if not isinstance(versions, list):
        return versions
    try:
        range_ = semver.reduce_versions(*versions)
    except VersionsNotCompatibleException as e:
        new_msg = "Version error for package {}: {}".format(package.get("package"), e)
        raise DependencyException(new_msg) from e

    available = get_available_versions(package.get("package"))
    prerelease_version_specified = any(
        bool(semver.VersionSpecifier.from_version_string(version).prerelease)
        for version in versions
    )
    installable = semver.filter_installable(
        available, install_prerelease or prerelease_version_specified
    )
    target = semver.resolve_to_specific_version(range_, installable)
    if not target:
        package_version_not_found(package, range_, installable)
    return target
