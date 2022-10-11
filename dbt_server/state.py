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
    size: Optional[int] = None

    def set_last_parsed_manifest(self, state_id, manifest, size):
        with MANIFEST_LOCK:
            self.state_id = state_id
            self.manifest = manifest
            self.size = size

    def lookup(self, state_id):
        with MANIFEST_LOCK:
            if self.manifest is None:
                return None
            elif state_id in (None, self.state_id):
                return CachedManifest(state_id=self.state_id, manifest=self.manifest, size=self.size)
            else:
                return None

    # used for testing...
    def reset(self):
        with MANIFEST_LOCK:
            self.state_id = None
            self.manifest = None
            self.size = None


LAST_PARSED = CachedManifest()


class StateController(object):
    def __init__(self, state_id, manifest, size):
        self.state_id = state_id
        self.manifest = manifest

        self.root_path = filesystem_service.get_root_path(state_id)
        self.serialize_path = filesystem_service.get_path(state_id, "manifest.msgpack")

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
        # Every parse updates the in-memory manifest cache
        logger.info(f"Updating cache (state_id={state_id})")
        # TODO jp add size calculation here
        LAST_PARSED.set_last_parsed_manifest(state_id, manifest, 0)

        logger.info(f"Done parsing from source (state_id={state_id})")
        return cls(state_id, manifest, 0)

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
            return cls(cached.state_id, cached.manifest, cached.size)

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
        # TODO jp add size calculation here

        return cls(state_id, manifest, 0)

    @tracer.wrap
    def serialize_manifest(self):
        logger.info(f"Serializing manifest to file system ({self.serialize_path})")
        dbt_service.serialize_manifest(self.manifest, self.serialize_path)

    @tracer.wrap
    def update_state_id(self):
        logger.info(f"Updating latest state id ({self.state_id})")
        filesystem_service.update_state_id(self.state_id)

    @tracer.wrap
    def compile_query(self, query):
        return dbt_service.compile_sql(self.manifest, self.root_path, query)

    @tracer.wrap
    def execute_query(self, query):
        return dbt_service.execute_sql(self.manifest, self.root_path, query)
