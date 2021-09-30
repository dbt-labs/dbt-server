
from . import filesystem_service

import dbt
import dbt.tracking
import dbt.adapters.factory
from dbt.lib import create_task, get_dbt_config
from dbt.parser.manifest import ManifestLoader, process_node
from dbt.contracts.graph.manifest import Manifest

from dbt.parser.rpc import RPCCallParser
from dbt.rpc.node_runners import RPCExecuteRunner, RPCCompileRunner


def disable_tracking():
    # This messes with my stuff
    # TODO: why does this mess with stuff
    dbt.tracking.disable_tracking()


def parse_to_manifest(project_path):
    config = get_dbt_config(project_path)
    return ManifestLoader.get_full_manifest(config)


def serialize_manifest(manifest, serialize_path):
    filesystem_service.write_file(serialize_path, manifest.to_msgpack())


def deserialize_manifest(serialize_path):
    manifest_packed = filesystem_service.read_file(serialize_path)
    return Manifest.from_msgpack(manifest_packed)


def dbt_run_sync(project_path, args, manifest):
    config = get_dbt_config(project_path)
    # TODO: what do we truely need to kick off a task
    task = create_task(args, manifest, config)
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


def _get_operation_node(manifest, project_path, sql):
    config = get_dbt_config(project_path)
    rpc_parser = RPCCallParser(
        project=config,
        manifest=manifest,
        root_project=config,
    )

    adapter = dbt.adapters.factory.get_adapter(config)
    # TODO : This needs a real name?
    rpc_node = rpc_parser.parse_remote(sql, 'name')
    process_node(config, manifest, rpc_node)
    return config, rpc_node, adapter


def compile_sql(manifest, project_path, sql):
    config, node, adapter = _get_operation_node(manifest, project_path, sql)
    runner = RPCCompileRunner(config, adapter, node, 1, 1)
    compiled = runner.compile(manifest)
    return runner.execute(compiled, manifest)


def execute_sql(manifest, project_path, sql):
    config, node, adapter = _get_operation_node(manifest, project_path, sql)
    runner = RPCExecuteRunner(config, adapter, node, 1, 1)
    compiled = runner.compile(manifest)
    return runner.execute(compiled, manifest)
