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
        self.config_serialize_path = filesystem_service.get_path(
            self.state_id, "config.json"
        )

        self.manifest = self.load_manifest(self.serialize_path)

        # from dbt.config.runtime import RuntimeConfig
        # from dbt.config.project import Project
        # import dbt.adapters.factory

        # self.runtime_config = RuntimeConfig.from_parts(
        #     project=my_project,
        #     profile=my_profile,
        #     args=my_args,
        # )

        # self.adapter = dbt.adapters.factory.get_adapter(self.runtime_config)

    @classmethod
    def load_manifest(cls, serialize_path):
        return dbt_service.deserialize_manifest(serialize_path)

    def compile_query(self, query, user, password):
        import dbt.adapters.factory
        config = dbt_service.deserialize_config(self.config_serialize_path, user, password)
        dbt.adapters.factory.register_adapter(config)
        adapter = dbt.adapters.factory.get_adapter(config)

        return dbt_service.hack_dbt_compile(
            self.manifest,
            config,
            adapter,
            query,
        )

    def execute_query(self, query):
        return dbt_service.execute_sql(self.manifest, self.path, query)
