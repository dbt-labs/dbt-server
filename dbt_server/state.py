from dbt_server.services import filesystem_service, dbt_service

from dbt_server.exceptions import StateNotFoundException


class StateController(object):
    def __init__(self, state_id=None):
        self.state_id = filesystem_service.get_latest_state_id(state_id)
        if self.state_id is None:
            raise StateNotFoundException(
                f"Provided state_id does not exist or is not found ({state_id})"
            )

        self.path = filesystem_service.get_root_path(self.state_id)
        self.serialize_path = filesystem_service.get_path(
            self.state_id, "manifest.msgpack"
        )
        self.manifest = self.load_manifest(self.serialize_path)

    @classmethod
    def load_manifest(cls, serialize_path):
        return dbt_service.deserialize_manifest(serialize_path)

    def compile_query(self, query):
        return dbt_service.compile_sql(self.manifest, self.path, query)

    def execute_query(self, query):
        return dbt_service.execute_sql(self.manifest, self.path, query)
