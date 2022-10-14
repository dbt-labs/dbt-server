from fastapi.testclient import TestClient
import unittest
from unittest.mock import patch, ANY, Mock

from dbt_server.server import app
from dbt_server.state import StateController, CachedManifest

from dbt_server.exceptions import dbtCoreCompilationException, StateNotFoundException


client = TestClient(app)


class CompilationInterfaceTests(unittest.TestCase):
    def test_compilation_interface_no_sql(self):
        with patch("dbt_server.state.StateController.load_state") as state:
            response = client.post(
                "/compile",
                json={
                    "sql": None,
                    "state_id": "abc123",
                },
            )

            assert not state.called
            assert response.status_code == 422

    def test_compilation_interface_invalid_state_id(self):
        response = client.post(
            "/compile",
            json={
                "sql": "select {{ 1 + 1 }}",
                "state_id": "badid",
            },
        )
        # Compile with invalid state id returns a 422
        assert response.status_code == 422

    def test_compilation_interface_valid_state_id(self):
        state_id = "goodid"
        source_query = "select {{ 1 + 1 }}"
        compiled_query = "select 2 as id"

        state_mock = Mock(
            return_value=StateController(
                state_id=state_id,
                manifest=None,
                config=None,
                parser=None,
                manifest_size=0,
                is_manifest_cached=False,
            )
        )

        query_mock = Mock(return_value={"compiled_code": compiled_query})

        with patch.multiple(
            "dbt_server.state.StateController",
            load_state=state_mock,
            compile_query=query_mock,
        ):
            response = client.post(
                "/compile",
                json={
                    "sql": source_query,
                    "state_id": state_id,
                },
            )

            state_mock.assert_called_once_with(state_id)
            query_mock.assert_called_once_with(source_query)
            assert response.status_code == 200

            expected = {
                "parsing": state_id,
                "path": "./working-dir/state-goodid/manifest.msgpack",
                "res": ANY,
                "compiled_code": compiled_query,
            }
            assert response.json() == expected

    def test_compilation_interface_compilation_error(self):
        state_id = "badid"
        query = "select {{ exceptions.raise_compiler_error('bad')}}"

        with patch(
            "dbt_server.state.StateController.load_state",
            side_effect=dbtCoreCompilationException("Compilation error"),
        ) as state:
            response = client.post(
                "/compile",
                json={
                    "sql": query,
                    "state_id": state_id,
                },
            )

            state.assert_called_once_with(state_id)
            assert response.status_code == 400

            expected = {
                "data": None,
                "message": "Compilation error",
                "status_code": 400,
            }
            assert response.json() == expected

    def test_compilation_interface_cache_hit(self):
        # Cache hit for load_state
        with patch("dbt_server.state.LAST_PARSED") as last_parsed:
            state = StateController.load_state("abc123")
            last_parsed.lookup.assert_called_once_with("abc123")
            assert state.manifest is not None

    def test_compilation_interface_cache_miss(self):
        # Cache miss
        with patch("dbt_server.state.LAST_PARSED.lookup", return_value=None) as lookup:
            # We expect this to raise because abc123 is not a real state...
            # that's fine for this test, we just want to make sure that we lookup abc123
            with self.assertRaises(StateNotFoundException):
                StateController.load_state("abc123")

            lookup.assert_called_once_with("abc123")

    def test_compilation_interface_cache_mutation(self):
        cached = CachedManifest()
        assert cached.state_id is None
        assert cached.manifest is None
        assert cached.manifest_size is None

        path = "working-dir/state-abc123/"

        cache_miss = cached.lookup("abc123")
        assert cache_miss is None

        cache_miss = cached.lookup(None)
        assert cache_miss is None

        # Update cache (ie. on /parse)
        manifest_mock = Mock()

        with patch.multiple(
            "dbt_server.services.dbt_service",
            create_dbt_config=Mock(),
            get_sql_parser=Mock(),
        ):
            cached.set_last_parsed_manifest("abc123", manifest_mock, path, 512)

            assert cached.state_id == "abc123"
            assert cached.manifest is not None
            assert cached.manifest_size == 512
            assert cached.config is not None
            assert cached.parser is not None

        assert cached.lookup(None) is not None
        manifest_mock.reset_mock()

        assert cached.lookup("abc123") is not None
        manifest_mock.reset_mock()

        assert cached.lookup("def456") is None

        # Re-update cache (ie. on subsequent /parse)
        new_manifest_mock = Mock()

        with patch.multiple(
            "dbt_server.services.dbt_service",
            create_dbt_config=Mock(),
            get_sql_parser=Mock(),
        ):
            cached.set_last_parsed_manifest("def456", new_manifest_mock, path, 1024)
            assert cached.state_id == "def456"
            assert cached.manifest is not None
            assert cached.manifest_size == 1024
            assert cached.config is not None
            assert cached.parser is not None

        assert cached.lookup(None) is not None
        assert cached.lookup("def456") is not None
        assert cached.lookup("abc123") is None
