
import json
import os
import yaml
import boto3
import botocore.exceptions
import time
from base64 import b64decode
from typing import Dict
from . import filesystem_service
from dbt_server.logging import GLOBAL_LOGGER as logger

import dbt
import dbt.tracking
import dbt.adapters.factory
from dbt.config.runtime import RuntimeConfig
from dbt.parser.manifest import ManifestLoader, process_node
from dbt.contracts.graph.manifest import Manifest

from dbt.task.run import RunTask
from dbt.task.list import ListTask
from dbt.parser.rpc import RPCCallParser
from dbt.rpc.node_runners import RPCExecuteRunner, RPCCompileRunner

from pydantic import BaseModel


class dbtConfig(BaseModel):
    project_dir: str
    profiles_dir: str
    single_threaded: bool = False

    @classmethod
    def new(cls, project_dir):
        # TODO: How do we handle creds more.... dynamically?
        if os.getenv('DBT_PROFILES_DIR'):
            profiles_dir = os.getenv('DBT_PROFILES_DIR')
        else:
            profiles_dir = os.path.expanduser("~/.dbt")
        if os.getenv('PROFILES_YML_CONTENTS'):
            response = decrypt_kms(
                os.getenv('PROFILES_YML_CONTENTS'),
                # We probably need some kind of UID to pass with context
                {
                    "type": "profiles_yaml",
                    # "user_id": f"{user_id}",
                },)
            if response:
                with open(f'{profiles_dir}/profiles.yml', 'w+') as profiles_yml:
                    profiles = json.loads(response)
                    yaml.dump(profiles, profiles_yml)


        logger.info(f"Using profile path @ {profiles_dir}")

        return cls(
            project_dir=project_dir,
            profiles_dir=profiles_dir,
        )
      
def decrypt_kms(
      ciphertext: str,
      context: Dict,
      attempt_number: int = 1,
      max_retries: int = 5,
  ) -> str:
      if attempt_number > max_retries:
          raise Exception("Exceeded maximum number of retries.")
      try:
          client = boto3.client("kms", region_name="us-east-1")
          response = client.decrypt(CiphertextBlob=b64decode(ciphertext), EncryptionContext=context)
      except botocore.exceptions.BotoCoreError as e:
          time.sleep(0.1)
          return decrypt_kms(ciphertext, context, attempt_number + 1, max_retries)
      except BaseException as e:
          raise e
      return response.get("Plaintext").decode("utf-8")

def disable_tracking():
    # This messes with my stuff
    dbt.tracking.disable_tracking()

def _get_dbt_config(project_path):
    # Construct a phony config
    config = RuntimeConfig.from_args(dbtConfig.new(project_path))

    # Load the relevant adapter
    dbt.adapters.factory.register_adapter(config)

    return config

def parse_to_manifest(project_path):
    config = _get_dbt_config(project_path)
    return ManifestLoader.get_full_manifest(config)

def serialize_manifest(manifest, serialize_path):
    filesystem_service.write_file(serialize_path, manifest.to_msgpack())

def deserialize_manifest(serialize_path):
    manifest_packed = filesystem_service.read_file(serialize_path)
    return Manifest.from_msgpack(manifest_packed)

def dbt_run_sync(project_path, args, manifest):
    config = _get_dbt_config(project_path)

    def no_op(*args, **kwargs):
        pass

    # Create the task
    task = RunTask(args, config)

    # Wow! We can monkeypatch taskCls.load_manifest to return _our_ manifest
    # TODO : Let's update Core to support this kind of thing more natively?
    task.load_manifest = no_op
    task.manifest = manifest

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
    config = _get_dbt_config(project_path)
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
