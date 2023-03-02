from dbt_server.services import filesystem_service, dbt_service
from dbt_server.exceptions import StateNotFoundException
from dbt_server.logging import DBT_SERVER_LOGGER as logger
from dbt_server import tracer


from dataclasses import dataclass
from typing import Optional, Any, Tuple, List
import threading


MANIFEST_LOCK = threading.Lock()


@dataclass
class CachedManifest:
    """CachedManifest holds a local in memory cache of latest manifest and
    other project metadata.

    This class is singleton.
    This class is thread-compatible as most write operations are guarded by
    lock."""

    # State id is a string generated by client of dbt-server, dbt-server takes
    # it as a manifest version. It's optional and can be None.
    state_id: Optional[str] = None
    # Indicates local dbt project path and is provided on parse. It might be None when
    # clients push source code to dbt-server and thus provide a state_id instead. 
    # Either state_id or project_path must be provided.
    project_path: Optional[str] = None
    # Root path points to dbt project path, it equals to project_path when
    # project_path is not None, otherwise it points to the root of the directory
    # containing the latest project state.
    root_path: Optional[str] = None
    # Dbt core Manifest object. If it's none, it indicates the cache is not set
    # yet.
    manifest: Optional[Any] = None
    # Size of serialized manifest object file, byte unit. It might be 0 even if
    # manifest exists when we never serialize manifest to local.
    manifest_size: Optional[int] = None

    # Dbt core RuntimeConfig, parsed from project metadata.
    config: Optional[Any] = None
    # Dbt core SqlBlockParser, generated by config and manifest.
    parser: Optional[Any] = None

    def set_last_parsed_manifest(
        self, state_id, project_path, root_path, manifest, manifest_size, config
    ):
        """Updates global singleton manifest cache with lock protected. See
        class member comment for args definition."""
        with MANIFEST_LOCK:
            self.state_id = state_id
            self.project_path = project_path
            self.root_path = root_path
            self.manifest = manifest
            self.manifest_size = manifest_size
            self.config = config

            # Get parser from config and manifest.
            self.parser = dbt_service.get_sql_parser(self.config, self.manifest)

    def lookup(self, state_id):
        """Checks if required manifest hits cached one, returns None if not
        found.
        - If `state_id` is None, we skip checking cached state_id and return
        the local cached manifest.
        - If `state_id` is not None, we will check cached state_id and return the
        local cached manifest only if state_id is matched."""
        with MANIFEST_LOCK:
            if self.manifest is None:
                return None
            elif state_id in (None, self.state_id):
                return self
            else:
                return None

    def reset(self):
        """Testing purpose."""
        with MANIFEST_LOCK:
            self.state_id = None
            self.project_path = None
            self.root_path = None
            self.manifest = None
            self.manifest_size = None
            self.config = None
            self.parser = None


# Global singleton manifest cache object.
LAST_PARSED = CachedManifest()


class StateController(object):
    """StateController stores all dbt core required state including project
    metadata and manifest, and provides core interface to caller. Any core
    invocations will process according to the current state."""

    def __init__(
        self,
        state_id,
        project_path,
        root_path,
        manifest,
        config,
        parser,
        manifest_size,
        is_manifest_cached,
    ):
        self.state_id = state_id
        self.project_path = project_path
        self.root_path = root_path
        self.manifest = manifest
        self.config = config
        self.parser = parser
        self.manifest_size = manifest_size
        # If it's true, the menifest is loaded from cache.
        self.is_manifest_cached = is_manifest_cached

        # Serialized manifest.msgpack path.
        self.serialize_path = filesystem_service.get_path(root_path, "manifest.msgpack")

    @classmethod
    @tracer.wrap
    def _from_parts(
        cls, state_id, project_path, manifest, root_path, manifest_size, args=None
    ):
        """Returns StateController object that stores current dbt core state."""
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
        """Returns StateController object that is created from local cache."""
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
        """Loads a manifest from source code in a specified directory based on the
        provided state_id. This method will cache the parsed manifest in memory
        before returning."""
        root_path = filesystem_service.get_root_path(
            parse_args.state_id, parse_args.project_path
        )
        log_details = _generate_log_details(
            parse_args.state_id, parse_args.project_path
        )
        logger.info(f"Parsing manifest from filetree ({log_details})")

        manifest = dbt_service.parse_to_manifest(root_path, parse_args)

        logger.info(f"Done parsing from source {log_details}")
        return cls._from_parts(
            parse_args.state_id,
            parse_args.project_path,
            manifest,
            root_path,
            0,
            parse_args,
        )

    @classmethod
    @tracer.wrap
    def load_state(cls, args=None):
        """Loads a manifest given a state_id from an in-memory cache if present,
        or from disk at a location specified by the state_id argument. The
        manifest cached in-memory will updated on every /parse request, so
        state_ids which are None (ie. "latest") or exactly matching the latest
        parsed state_id will be cache hits."""
        cached = LAST_PARSED.lookup(args.state_id)
        if cached:
            logger.info(
                f"Loading manifest from cache {_generate_log_details(cached.state_id, cached.root_path)}"
            )
            return cls.from_cached(cached)
        # Not in cache - need to go to filesystem to deserialize it
        state_id = filesystem_service.get_latest_state_id(args.state_id)
        project_path = filesystem_service.get_latest_project_path()

        logger.info(
            f"Manifest cache miss ({_generate_log_details(state_id, project_path)})"
        )

        # No state_id provided, and no latest-state-id.txt found
        if state_id is None and project_path is None:
            raise StateNotFoundException(
                f"Provided state_id does not exist, no previous state_id or project_path found {_generate_log_details(state_id, project_path)}"
            )

        root_path = filesystem_service.get_root_path(state_id, project_path)

        # Don't cache on deserialize - that's only for /parse
        manifest_path = filesystem_service.get_path(root_path, "manifest.msgpack")
        logger.info(f"Loading manifest from file system ({manifest_path})")
        manifest = dbt_service.deserialize_manifest(manifest_path)
        manifest_size = filesystem_service.get_size(manifest_path)

        return cls._from_parts(
            state_id, project_path, manifest, root_path, manifest_size, args
        )

    @tracer.wrap
    def serialize_manifest(self):
        """Serialize manifest object to local serialized_path and partial parse
        path, updates manifest_size to the file size."""
        logger.info(f"Serializing manifest to file system ({self.serialize_path})")
        partial_parse_path = filesystem_service.get_partial_parse_path()
        dbt_service.serialize_manifest(
            self.manifest, self.serialize_path, partial_parse_path
        )
        self.manifest_size = filesystem_service.get_size(self.serialize_path)

    @tracer.wrap
    def _update_state_id(self):
        """If current state_id != None, persists it into local storage."""
        if self.state_id is not None:
            logger.info(f"Updating latest state id ({self.state_id})")
            filesystem_service.update_state_id(self.state_id)

    @tracer.wrap
    def _update_project_path(self):
        """If current project_path != None, persists it into local storage."""
        if self.project_path is not None:
            logger.info(f"Updating latest project path ({self.project_path})")
            filesystem_service.update_project_path(self.project_path)

    @tracer.wrap
    def update_cache(self):
        """Updates local manifest cache and persists state_id and project_path
        into local storage."""
        logger.info(
            f"Updating cache {_generate_log_details(self.state_id, self.project_path)}"
        )
        self._update_state_id()
        self._update_project_path()
        LAST_PARSED.set_last_parsed_manifest(
            self.state_id,
            self.project_path,
            self.root_path,
            self.manifest,
            self.manifest_size,
            self.config,
        )

    @tracer.wrap
    def compile_query(self, query):
        """Returns a result dict of compiling `query` according to current
        state."""
        return dbt_service.compile_sql(
            self.manifest,
            self.config,
            self.parser,
            query,
        )

    @tracer.wrap
    def execute_query(self, query):
        """Returns a result dict of executing `query` according to current
        state."""
        return dbt_service.execute_sql(self.manifest, self.root_path, query)

    @tracer.wrap
    def execute_async_command(
        self, task_id: str, command: List[str], db: Any, callback_url: str
    ) -> None:
        """Executes core async command. Raises any exception got by core.

        Args:
            task_id: unique identifier of the task will be executed.
            command: list of string that will be sent to core.
            db: local sqlite session, to store task status locally.
            callback_url: string, will be called back eventually."""
        dbt_service.execute_async_command(
            command,
            task_id,
            self.root_path,
            self.manifest,
            db,
            self.state_id,
            callback_url,
        )

    @tracer.wrap
    def execute_sync_command(self, command: List[str]) -> Tuple:
        """Executes core sync command. Raises any exception got by core.

        Args:
            command: list of string that will be sent to core."""
        return dbt_service.execute_sync_command(command, self.root_path, self.manifest)


def _generate_log_details(state_id: str, project_path: str):
    """Helper function to generate a log string including `state_id` and
    `project_path`.
    """
    if state_id is None and project_path:
        return f"(project_path={project_path})"
    elif state_id:
        return f"(state_id={state_id})"
    return ""
