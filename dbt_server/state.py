import os
from dbt_server.services import filesystem_service, dbt_service
from dbt_server.exceptions import StateNotFoundException
from dbt_server.logging import DBT_SERVER_LOGGER as logger
from dbt_server import crud, tracer
from dbt.lib import load_profile_project
from dbt.cli.main import dbtRunner
from dbt.exceptions import RuntimeException

from dataclasses import dataclass
from typing import Optional, Any
import threading


MANIFEST_LOCK = threading.Lock()


@dataclass
class CachedManifest:
    state_id: Optional[str] = None
    project_path: Optional[str] = None
    root_path: Optional[str] = None
    manifest: Optional[Any] = None
    manifest_size: Optional[int] = None

    config: Optional[Any] = None
    parser: Optional[Any] = None

    def set_last_parsed_manifest(self, state_id, project_path, root_path, manifest, manifest_size, config):
        with MANIFEST_LOCK:
            self.state_id = state_id
            self.project_path = project_path
            self.root_path = root_path
            self.manifest = manifest
            self.manifest_size = manifest_size
            self.config = config

            self.parser = dbt_service.get_sql_parser(self.config, self.manifest)

    def lookup(self, state_id):
        with MANIFEST_LOCK:
            if self.manifest is None:
                return None
            elif state_id in (None, self.state_id):
                # TODO: verify this conditional catches project path cache hits
                return self
            else:
                return None

    # used for testing...
    def reset(self):
        with MANIFEST_LOCK:
            self.state_id = None
            self.project_path = None
            self.root_path = None
            self.manifest = None
            self.manifest_size = None
            self.config = None
            self.parser = None


LAST_PARSED = CachedManifest()


class StateController(object):
    def __init__(
        self, state_id, project_path, root_path, manifest, config, parser, manifest_size, is_manifest_cached
    ):
        self.state_id = state_id
        self.project_path = project_path
        self.root_path = root_path
        self.manifest = manifest
        self.config = config
        self.parser = parser
        self.manifest_size = manifest_size
        self.is_manifest_cached = is_manifest_cached

        self.serialize_path = filesystem_service.get_path(root_path, "manifest.msgpack")
        self.partial_parse_path = filesystem_service.get_path(root_path, "target", filesystem_service.PARTIAL_PARSE_FILE)

    @classmethod
    @tracer.wrap
    def from_parts(cls, state_id, project_path, manifest, root_path, manifest_size, args=None):
        config = dbt_service.create_dbt_config(root_path, args)
        parser = dbt_service.get_sql_parser(config, manifest)

        return cls(
            state_id=state_id,
            project_path=project_path,
            root_path=root_path,
            manifest=manifest,
            config=config,
            parser=parser,
            manifest_size=manifest_size,
            is_manifest_cached=False,
        )

    @classmethod
    @tracer.wrap
    def from_cached(cls, cached):
        return cls(
            state_id=cached.state_id,
            project_path=cached.project_path,
            root_path=cached.root_path,
            manifest=cached.manifest,
            config=cached.config,
            parser=cached.parser,
            manifest_size=cached.manifest_size,
            is_manifest_cached=True,
        )

    @classmethod
    @tracer.wrap
    def parse_from_source(cls, parse_args=None):
        """
        Loads a manifest from source code in a specified directory based on the
        provided state_id. This method will cache the parsed manifest in memory
        before returning.
        """
        root_path = filesystem_service.get_root_path(parse_args.state_id, parse_args.project_path)
        log_details = generate_log_details(parse_args.state_id, parse_args.project_path)
        logger.info(f"Parsing manifest from filetree ({log_details})")

        manifest = dbt_service.parse_to_manifest(root_path, parse_args)


        logger.info(f"Done parsing from source ({log_details})")
        return cls.from_parts(parse_args.state_id, parse_args.project_path, manifest, root_path, 0, parse_args)

    @classmethod
    @tracer.wrap
    def load_state(cls, args=None):
        """
        Loads a manifest given a state_id from an in-memory cache if present,
        or from disk at a location specified by the state_id argument. The
        manifest cached in-memory will updated on every /parse request, so
        state_ids which are None (ie. "latest") or exactly matching the latest
        parsed state_id will be cache hits.
        """
        cached = LAST_PARSED.lookup(args.state_id)
        if cached:
            logger.info(f"Loading manifest from cache ({generate_log_details(cached.state_id, cached.root_path)})")
            return cls.from_cached(cached)
        # Not in cache - need to go to filesystem to deserialize it
        logger.info(f"Manifest cache miss ({generate_log_details(args.state_id, cls.project_path)})")
        state_id = filesystem_service.get_latest_state_id(args.state_id)
        project_path = filesystem_service.get_latest_project_path()

        # No state_id provided, and no latest-state-id.txt found
        if state_id is None and project_path is None:
            raise StateNotFoundException(
                f"Provided state_id does not exist, no previous state_id or project_path found ({generate_log_details(state_id, project_path)})"
            )

        root_path = filesystem_service.get_root_path(state_id, project_path)

        # Don't cache on deserialize - that's only for /parse
        manifest_path = filesystem_service.get_path(root_path, "manifest.msgpack")
        logger.info(f"Loading manifest from file system ({manifest_path})")
        manifest = dbt_service.deserialize_manifest(manifest_path)
        manifest_size = filesystem_service.get_size(manifest_path)

        return cls.from_parts(state_id, project_path, manifest, root_path, manifest_size, args)

    @tracer.wrap
    def serialize_manifest(self):
        logger.info(f"Serializing manifest to file system ({self.serialize_path})")
        dbt_service.serialize_manifest(self.manifest, self.serialize_path, self.partial_parse_path)
        self.manifest_size = filesystem_service.get_size(self.serialize_path)

    @tracer.wrap
    def update_state_id(self):
        if self.state_id is not None:
            logger.info(f"Updating latest state id ({self.state_id})")
            filesystem_service.update_state_id(self.state_id)
    
    @tracer.wrap
    def update_project_path(self):
        if self.project_path is not None:
            logger.info(f"Updating latest project path ({self.project_path})")
            filesystem_service.update_project_path(self.project_path)

    @tracer.wrap
    def compile_query(self, query):
        return dbt_service.compile_sql(
            self.manifest,
            self.config,
            self.parser,
            query,
        )

    @tracer.wrap
    def execute_query(self, query):
        return dbt_service.execute_sql(self.manifest, self.root_path, query)

    @tracer.wrap
    def update_cache(self):
        logger.info(f"Updating cache ({generate_log_details(self.state_id, self.project_path)})")
        self.update_state_id()
        self.update_project_path()
        LAST_PARSED.set_last_parsed_manifest(
            self.state_id, self.project_path, self.root_path, self.manifest, self.manifest_size, self.config
        )

    @tracer.wrap
    def execute_async_command(self, task_id, command, db):
        db_task = crud.get_task(db, task_id)
        log_path = filesystem_service.get_log_path(task_id, self.state_id)

        # Temporary solution for structured log formatting until core adds a cleaner interface
        new_command = []
        new_command.append("--log-format")
        new_command.append("json")
        new_command.append("--log-path")
        new_command.append(log_path)
        new_command += command

        logger.info(f"Running dbt ({task_id}) - deserializing manifest {self.serialize_path}")

        profile, project = load_profile_project(self.root_path, os.getenv("DBT_PROFILE_NAME", "user"),)

        crud.set_task_running(db, db_task)

        logger.info(f"Running dbt ({task_id}) - kicking off task")

        try:
            dbt = dbtRunner(project, profile, self.manifest)
            # TODO we might need to surface this to shipment later on
            res, success = dbt.invoke(new_command)
        except RuntimeException as e:
            crud.set_task_errored(db, db_task, str(e))
            raise e

        logger.info(f"Running dbt ({task_id}) - done")

        crud.set_task_done(db, db_task)


def generate_log_details(state_id, project_path):
    if state_id is None and project_path:
        return f"project_path={project_path}"
    return f"state_id={state_id}"
    