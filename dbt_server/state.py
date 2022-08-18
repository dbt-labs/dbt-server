from dbt_server.services import filesystem_service, dbt_service
from dbt_server.exceptions import StateNotFoundException, InternalException
from dbt_server.logging import GLOBAL_LOGGER as logger
from collections import OrderedDict

# LIFO queue - earlier members are popped to make space for new ones
MANIFEST_CACHE = OrderedDict()
MANIFEST_CACHE_SIZE = 3


def cache_add(key, val):
    MANIFEST_CACHE[key] = val
    if len(MANIFEST_CACHE) > MANIFEST_CACHE_SIZE:
        MANIFEST_CACHE.popitem(last=False)


def cache_get(key):
    return MANIFEST_CACHE.get(key)


class StateController(object):
    def __init__(self, state_id, manifest):
        self.state_id = state_id
        self.manifest = manifest

        self.root_path = filesystem_service.get_root_path(state_id)
        self.serialize_path = filesystem_service.get_path(state_id, "manifest.msgpack")

    @classmethod
    def parse_from_source(cls, state_id, parse_args):
        source_path = filesystem_service.get_root_path(state_id)
        logger.info(f"Parsing manifest from filetree (state_id={state_id})")
        manifest = dbt_service.parse_to_manifest(source_path, parse_args)
        cache_add(state_id, manifest)
        return cls(state_id, manifest)

    @classmethod
    def load_state(cls, state_id):
        state_id = filesystem_service.get_latest_state_id(state_id)
        if state_id is None:
            raise StateNotFoundException(
                f"Provided state_id does not exist or is not found ({state_id})"
            )

        cached = cache_get(state_id)
        if cached:
            logger.info(f"Loading manifest from cache ({state_id})")
            return cls(state_id, cached)
        else:
            manifest_path = filesystem_service.get_path(state_id, "manifest.msgpack")
            logger.info(f"Loading manifest from file system ({manifest_path})")
            manifest = dbt_service.deserialize_manifest(manifest_path)
            return cls(state_id, manifest)

    def serialize_manifest(self):
        logger.info(f"Serializing manifest to file system ({self.serialize_path})")
        dbt_service.serialize_manifest(self.manifest, self.serialize_path)

    def update_state_id(self):
        logger.info(f"Updating latest state id ({self.state_id})")
        filesystem_service.update_state_id(self.state_id)

    def compile_query(self, query):
        return dbt_service.compile_sql(self.manifest, self.root_path, query)

    def execute_query(self, query):
        return dbt_service.execute_sql(self.manifest, self.root_path, query)
