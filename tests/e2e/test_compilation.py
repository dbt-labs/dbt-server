from fastapi.testclient import TestClient
from unittest import TestCase

from dbt_server.server import app
from dbt_server.services import filesystem_service
from .helpers import profiles_dir
from .fixtures import simple, invalid, Profiles

import hashlib
import json
import tempfile

client = TestClient(app)


class ManifestBuildingTestCase(TestCase):
    def setUp(self):
        # Override working-dir path to keep things clean in dev...
        self.temp_dir = tempfile.TemporaryDirectory()
        self.old_root_path = filesystem_service.ROOT_PATH
        filesystem_service.ROOT_PATH = self.temp_dir.name

    def tearDown(self):
        self.temp_dir.cleanup()
        filesystem_service.ROOT_PATH = self.old_root_path

    @classmethod
    def push_fixture_data(cls, file_dict):
        manifest = {
            key: {
                "contents": value,
                "hash": hashlib.md5(value.encode()).hexdigest(),
                "path": key,
            }
            for (key, value) in file_dict.items()
        }

        unparsed = json.dumps(manifest)
        state_id = hashlib.md5(unparsed.encode()).hexdigest()

        response = client.post("/push", json={"state_id": state_id, "body": manifest})

        return response

    @classmethod
    def parse_fixture_data(cls, state_id):
        response = client.post(
            "/parse",
            json={
                "state_id": state_id,
                "profile": "user",
            },
        )

        return response

    @classmethod
    def compile_against_state(cls, state_id, sql):
        response = client.post(
            "/compile",
            json={
                "state_id": state_id,
                "sql": sql,
            },
        )

        return response


class ValidManifestBuildingTestCase(ManifestBuildingTestCase):
    def setUp(self):
        super().setUp()
        # Stub out profiles.yml file
        with profiles_dir(Profiles.Postgres):
            # Push project code
            resp_push = self.push_fixture_data(simple.FILES)
            self.assertEqual(resp_push.status_code, 200)
            data = resp_push.json()
            self.state_id = data["state"]

            # parse project code
            resp_parse = self.parse_fixture_data(self.state_id)
            self.assertEqual(resp_parse.status_code, 200)

    def test_valid_query(self):
        # Compile a query with state
        valid_query = "select {{ 1 + 1 }}"
        with profiles_dir(Profiles.Postgres):
            resp = self.compile_against_state(self.state_id, valid_query)
        data = resp.json()
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(data["compiled_code"], "select 2")

    def test_valid_query_implicit_state(self):
        # Compile a query with implicit latest state
        valid_query = "select {{ 2 + 2 }}"
        with profiles_dir(Profiles.Postgres):
            resp = self.compile_against_state(None, valid_query)
        data = resp.json()
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(data["compiled_code"], "select 4")

    def test_valid_model_reference(self):
        # Compile a query which results in a dbt compilation error
        valid_query = "select * from {{ ref('model_1') }}"
        with profiles_dir(Profiles.Postgres):
            resp = self.compile_against_state(self.state_id, valid_query)
        data = resp.json()
        self.assertEqual(resp.status_code, 200)
        compiled = 'select * from "analytics"."analytics"."model_1"'
        self.assertEqual(data["compiled_code"], compiled)

    def test_invalid_query_python_error(self):
        # Compile a query which results in a python error
        invalid_query = "select {{ 1 / 0 }}"
        with profiles_dir(Profiles.Postgres):
            resp = self.compile_against_state(self.state_id, invalid_query)
        data = resp.json()
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(data["message"], "division by zero")

    def test_invalid_query_dbt_compilation_error(self):
        # Compile a query which results in a dbt compilation error
        invalid_query = "select * from {{ ref('not_a_model') }}"
        with profiles_dir(Profiles.Postgres):
            resp = self.compile_against_state(self.state_id, invalid_query)
        data = resp.json()
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(
            data["message"],
            """\
Compilation Error in sql operation name (from remote system) Sql Operation \
'sql operation.my_new_project.name' (from remote system) depends on a node named \
'not_a_model' which was not found""",
        )

    def test_valid_query_call_macro(self):
        # Compile a query that calls a dbt user-space macro
        valid_macro_query = "select '{{ my_macro('josh wills') }}'"
        with profiles_dir(Profiles.Postgres):
            resp = self.compile_against_state(self.state_id, valid_macro_query)
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        assert "compiled_code" in data
        self.assertEqual(data["compiled_code"], "select 'hey, josh wills!'")

    def test_invalid_query_call_macro(self):
        valid_macro_query = "select '{{ my_macro(unexpected=true) }}'"
        with profiles_dir(Profiles.Postgres):
            resp = self.compile_against_state(self.state_id, valid_macro_query)
        self.assertEqual(resp.status_code, 400)
        data = resp.json()
        self.maxDiff = None
        self.assertEqual(
            data["message"],
            """\
Compilation Error in sql operation name (from remote system) macro \
'dbt_macro__my_macro' takes no keyword argument 'unexpected'\
  > in macro my_macro (macros/my_macro.sql)\
 > called by sql operation name (from remote system)""",
        )


class InvalidManifestBuildingTestCase(ManifestBuildingTestCase):
    def test_compilation_with_invalid_manifest(self):
        # Stub out profiles.yml file
        with profiles_dir(Profiles.Postgres):
            # Push project code
            resp_push = self.push_fixture_data(invalid.FILES)
            self.assertEqual(resp_push.status_code, 200)
            data = resp_push.json()
            state_id = data["state"]

            # parse project code
            resp_parse = self.parse_fixture_data(state_id)

            self.assertEqual(resp_parse.status_code, 400)
            data = resp_parse.json()
            self.assertEqual(
                data["message"],
                """\
Compilation Error in model model_2 (models/model_2.sql) Model \
'model.my_new_project.model_2' (models/model_2.sql) depends on a \
node named 'notfound' which was not found""",
            )

            valid_query = "select {{ 1 + 1 }}"
            resp = self.compile_against_state(state_id, valid_query)
            data = resp.json()
            self.assertEqual(resp.status_code, 422)
            self.assertTrue(
                data["message"].startswith("[Errno 2] No such file or directory")
            )
