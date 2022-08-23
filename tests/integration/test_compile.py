from fastapi.testclient import TestClient
import unittest
from unittest.mock import patch, ANY

from dbt_server.server import app

from dbt_server.exceptions import dbtCoreCompilationException

from .fixtures import (
    get_state_mock,
    FIXTURE_SERIALIZE_PATH,
    FIXTURE_STATE_ID,
    FIXTURE_SOURCE_CODE,
    FIXTURE_COMPILED_CODE,
)


client = TestClient(app)


class CompilationInterfaceTests(unittest.TestCase):
    def test_compilation_interface_no_sql(self):
        with patch(
            "dbt_server.views.StateController",
            return_value=get_state_mock(),
        ) as state:
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
        state_id = FIXTURE_STATE_ID
        path = FIXTURE_SERIALIZE_PATH
        query = FIXTURE_SOURCE_CODE

        with patch(
            "dbt_server.views.StateController",
            return_value=get_state_mock(),
        ) as state:
            response = client.post(
                "/compile",
                json={
                    "sql": query,
                    "state_id": state_id,
                },
            )

            state.assert_called_once_with(state_id)
            assert response.status_code == 200

            expected = {
                "parsing": state_id,
                "path": path,
                "res": ANY,
                "compiled_code": FIXTURE_COMPILED_CODE,
            }
            assert response.json() == expected

    def test_compilation_interface_compilation_error(self):
        state_id = FIXTURE_STATE_ID
        query = FIXTURE_SOURCE_CODE
        exc = dbtCoreCompilationException("Compilation error")

        with patch(
            "dbt_server.views.StateController",
            return_value=get_state_mock(exception=exc),
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
        pass
