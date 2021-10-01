
from . import filesystem_service

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
    # This messes with my stuff
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
    # TODO: what do we truely need to kick off a task
    task = create_task('run', args, manifest, config)
    return task.run()


def dbt_list(project_path, args, manifest):
    config = _get_dbt_config(project_path)
    def no_op(*args, **kwargs):
        pass

    # Create the task
    task = ListTask(args, config)
    # Wow! We can monkeypatch taskCls.load_manifest to return _our_ manifest
    # TODO : Let's update Core to support this kind of thing more natively?
    task.load_manifest = no_op
    task.manifest = manifest

    return task.run()


def execute_sql(manifest, project_path, sql):
    return dbt_execute_sql(manifest, project_path, sql)


def compile_sql(manifest, project_path, sql):
    return dbt_compile_sql(manifest, project_path, sql)
