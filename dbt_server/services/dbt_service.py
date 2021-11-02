import json
from . import filesystem_service
from dbt.clients.registry import package_version
from dbt.lib import (
    create_task,
    get_dbt_config,
    parse_to_manifest as dbt_parse_to_manifest,
    execute_sql as dbt_execute_sql,
    compile_sql as dbt_compile_sql,
    deserialize_manifest as dbt_deserialize_manifest,
    serialize_manifest as dbt_serialize_manifest
)


def disable_tracking():
    # TODO: why does this mess with stuff
    import dbt.tracking
    dbt.tracking.disable_tracking()


def parse_to_manifest(project_path):
    config = get_dbt_config(project_path)
    return dbt_parse_to_manifest(config)


def serialize_manifest(manifest, serialize_path):
    manifest_msgpack = dbt_serialize_manifest(manifest)
    filesystem_service.write_file(serialize_path, manifest_msgpack)


def deserialize_manifest(serialize_path):
    manifest_packed = filesystem_service.read_file(serialize_path)
    return dbt_deserialize_manifest(manifest_packed)


def dbt_run_sync(project_path, args, manifest):
    config = get_dbt_config(project_path)
    task = create_task('run', args, manifest, config)
    return task.run()


def dbt_list(project_path, args, manifest):
    config = get_dbt_config(project_path)
    task = create_task('list', args, manifest, config)
    return task.run()


def execute_sql(manifest, project_path, sql):
    return dbt_execute_sql(manifest, project_path, sql)


def compile_sql(manifest, project_path, sql):
    return dbt_compile_sql(manifest, project_path, sql)


def render_package_data(packages):
    return json.loads(packages)

def get_tarballs(package_data):
    tarballs = []
    for package in package_data.get('packages', {}):
        name = package.get('package')
        version = package.get('version')
        tar_name = '{}.{}.tar.gz'.format(name, version)

        package_details = package_version(name, version)
        tarball = package_details.get('downloads', {}).get('tarball')
        
        tarballs.append({
            "name": name,
            "version": version,
            "tar_name": tar_name,
            "tarball": tarball
        })         
    
    return tarballs