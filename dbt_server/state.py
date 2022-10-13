from dbt_server.services import filesystem_service, dbt_service
from dbt_server.exceptions import StateNotFoundException
from dbt_server.logging import GLOBAL_LOGGER as logger
from dbt_server import tracer

from dataclasses import dataclass
from typing import Optional, Any
import threading


MANIFEST_LOCK = threading.Lock()


@dataclass
class CachedManifest:
    state_id: Optional[str] = None
    manifest: Optional[Any] = None
    manifest_size: Optional[int] = None

    config: Optional[Any] = None
    parser: Optional[Any] = None

    def set_last_parsed_manifest(self, state_id, manifest, project_path, manifest_size):
        with MANIFEST_LOCK:
            self.state_id = state_id
            self.manifest = manifest
            self.manifest_size = manifest_size

            self.config = dbt_service.create_dbt_config(project_path)
            self.parser = dbt_service.get_sql_parser(self.config, self.manifest)

    def lookup(self, state_id):
        with MANIFEST_LOCK:
            if self.manifest is None:
                return None
            elif state_id in (None, self.state_id):
                return self
            else:
                return None

    # used for testing...
    def reset(self):
        with MANIFEST_LOCK:
            self.state_id = None
            self.manifest = None
            self.manifest_size = None
            self.config = None
            self.parser = None


LAST_PARSED = CachedManifest()


class StateController(object):
    def __init__(
        self, state_id, manifest, config, parser, manifest_size, is_manifest_cached
    ):
        self.state_id = state_id
        self.manifest = manifest
        self.config = config
        self.parser = parser
        self.manifest_size = manifest_size
        self.is_manifest_cached = is_manifest_cached

        self.root_path = filesystem_service.get_root_path(state_id)
        self.serialize_path = filesystem_service.get_path(state_id, "manifest.msgpack")

    @classmethod
    @tracer.wrap
    def from_parts(cls, state_id, manifest, source_path, manifest_size):
        config = dbt_service.create_dbt_config(source_path)
        parser = dbt_service.get_sql_parser(config, manifest)

        return cls(
            state_id=state_id,
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
            manifest=cached.manifest,
            config=cached.config,
            parser=cached.parser,
            manifest_size=cached.manifest_size,
            is_manifest_cached=True,
        )

    @classmethod
    @tracer.wrap
    def parse_from_source(cls, state_id, parse_args):
        """
        Loads a manifest from source code in a specified directory based on the
        provided state_id. This method will cache the parsed manifest in memory
        before returning.
        """
        source_path = filesystem_service.get_root_path(state_id)
        logger.info(f"Parsing manifest from filetree (state_id={state_id})")
        manifest = dbt_service.parse_to_manifest(source_path, parse_args)

        logger.info(f"Done parsing from source (state_id={state_id})")
        return cls.from_parts(state_id, manifest, source_path, 0)

    @classmethod
    @tracer.wrap
    def load_state(cls, state_id):
        """
        Loads a manifest given a state_id from an in-memory cache if present,
        or from disk at a location specified by the state_id argument. The
        manifest cached in-memory will updated on every /parse request, so
        state_ids which are None (ie. "latest") or exactly matching the latest
        parsed state_id will be cache hits.
        """
        cached = LAST_PARSED.lookup(state_id)
        if cached:
            logger.info(f"Loading manifest from cache ({cached.state_id})")
            return cls.from_cached(cached)

        # Not in cache - need to go to filesystem to deserialize it
        logger.info(f"Manifest cache miss (state_id={state_id})")
        state_id = filesystem_service.get_latest_state_id(state_id)

        # No state_id provided, and no latest-state-id.txt found
        if state_id is None:
            raise StateNotFoundException(
                f"Provided state_id does not exist or is not found ({state_id})"
            )

        # Don't cache on deserialize - that's only for /parse
        manifest_path = filesystem_service.get_path(state_id, "manifest.msgpack")
        logger.info(f"Loading manifest from file system ({manifest_path})")
        manifest = dbt_service.deserialize_manifest(manifest_path)
        manifest_size = filesystem_service.get_size(manifest_path)

        source_path = filesystem_service.get_root_path(state_id)
        return cls.from_parts(state_id, manifest, source_path, manifest_size)

    @tracer.wrap
    def serialize_manifest(self):
        logger.info(f"Serializing manifest to file system ({self.serialize_path})")
        dbt_service.serialize_manifest(self.manifest, self.serialize_path)
        self.manifest_size = filesystem_service.get_size(self.serialize_path)

    @tracer.wrap
    def update_state_id(self):
        logger.info(f"Updating latest state id ({self.state_id})")
        filesystem_service.update_state_id(self.state_id)

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
        logger.info(f"Updating cache (state_id={self.state_id})")
        LAST_PARSED.set_last_parsed_manifest(
            self.state_id, self.manifest, self.root_path, self.manifest_size
        )
