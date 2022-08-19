from fastapi.testclient import TestClient
import unittest
from unittest.mock import patch, ANY, Mock

from dbt_server.server import app
from dbt_server.state import StateController

from dbt_server.exceptions import dbtCoreCompilationException


client = TestClient(app)


class CompilationInterfaceTests(unittest.TestCase):
    def test_compilation_interface_no_sql(self):
        with patch("dbt_server.views.StateController.load_state") as state:
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
            return_value=StateController(state_id=state_id, manifest=None)
        )

        query_mock = Mock(return_value={"compiled_code": compiled_query})

        with patch.multiple(
            "dbt_server.views.StateController",
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
            "dbt_server.views.StateController.load_state",
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

    @patch("dbt.lib.compile_sql", side_effect=ZeroDivisionError)
    def test_compilation_interface_unhandled_dbt_error(self, compile_sql):
        # TODO
        pass

    def test_compilation_interface_use_cache(self):
        # TODO
        pass

    def test_compilation_interface_cache_mutation(self):
        # TODO
        pass
