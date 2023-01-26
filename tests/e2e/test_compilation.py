import re
from fastapi.testclient import TestClient
from unittest import TestCase

from dbt_server.server import app
from .helpers import profiles_dir
from .fixtures import simple, simple2, invalid, Profiles

import hashlib
import json
import tempfile
import os

client = TestClient(app)


class ManifestBuildingTestCase(TestCase):
    def setUp(self):
        # Override working-dir path to keep things clean in dev...
        self.temp_dir = tempfile.TemporaryDirectory()
        os.environ["__DBT_WORKING_DIR"] = self.temp_dir.name

    def tearDown(self):
        self.temp_dir.cleanup()
        del os.environ["__DBT_WORKING_DIR"]

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
        assert bool(re.match("compilation error", data["message"], re.I))

    def test_valid_query_call_macro(self):
        # Compile a query that calls a dbt user-space macro
        valid_macro_query = "select '{{ my_new_project.my_macro('josh wills') }}'"
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
        assert bool(re.match("compilation error", data["message"], re.I))

    def test_cached_compilation(self):
        # Test that compilation which uses the `graph` context variable
        # succeeds when using a cached manifest. Calling into `graph.nodes`
        # is significant because `graph` comes from the `flat_graph` property
        # of the manifest which is "special" in dbt Core. If the `flat_graph`
        # is not present on the manifest, this would fail with:
        #
        # 'dict object' has no attribute 'nodes'
        #
        # This test ensures that this property is accessible on the cached
        # manifest
        with profiles_dir(Profiles.Postgres):
            resp_push = self.push_fixture_data(simple.FILES)
            self.assertEqual(resp_push.status_code, 200)
            data = resp_push.json()
            state_id = data["state"]

            resp_parse = self.parse_fixture_data(state_id)
            self.assertEqual(resp_parse.status_code, 200)

            valid_macro_query = "select '{{ graph.nodes.values() }}'"
            resp = self.compile_against_state(state_id, valid_macro_query)

            self.assertEqual(resp.status_code, 200)
            data = resp.json()
            assert "compiled_code" in data


class CodeChangeTestCase(ManifestBuildingTestCase):
    def test_changing_code(self):
        """
        This test exists to ensure that manifest/config caching does not prevent callers
        of the dbt-server from osciallating between different states with each request.
        While only one of these states will be cached in memory at each time, callers
        should be able to compile queries against a state of their choosing in arbitrary order.
        """
        with profiles_dir(Profiles.Postgres):
            # Push project code (first project)
            resp_push = self.push_fixture_data(simple.FILES)
            self.assertEqual(resp_push.status_code, 200)
            data = resp_push.json()
            state_id_1 = data["state"]

            # parse project code
            resp_parse = self.parse_fixture_data(state_id_1)
            self.assertEqual(resp_parse.status_code, 200)

            # Compile a query with state
            valid_query = "select * from {{ ref('model_1') }}"
            resp = self.compile_against_state(state_id_1, valid_query)
            data = resp.json()
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(
                data["compiled_code"], 'select * from "analytics"."analytics"."model_1"'
            )

            # ------- reparse with different code -------#

            # Push project code (second project)
            resp_push = self.push_fixture_data(simple2.FILES)
            self.assertEqual(resp_push.status_code, 200)
            data = resp_push.json()
            state_id_2 = data["state"]

            # parse project code
            resp_parse = self.parse_fixture_data(state_id_2)
            self.assertEqual(resp_parse.status_code, 200)

            # Compile a query with state
            valid_query = "select * from {{ ref('model_1') }}"
            resp = self.compile_against_state(state_id_2, valid_query)
            data = resp.json()
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(
                data["compiled_code"], 'select * from "analytics"."analytics"."model_1"'
            )

            assert state_id_1 != state_id_2

            # ------- compile with initial state-------#

            valid_query = "select * from {{ ref('model_1') }}"
            resp = self.compile_against_state(state_id_1, valid_query)
            data = resp.json()
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(
                data["compiled_code"], 'select * from "analytics"."analytics"."model_1"'
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
            self.assertTrue(bool(re.match("compilation error", data["message"], re.I)))

            valid_query = "select {{ 1 + 1 }}"
            resp = self.compile_against_state(state_id, valid_query)
            data = resp.json()
            self.assertEqual(resp.status_code, 422)
            self.assertTrue(
                data["message"].startswith("[Errno 2] No such file or directory")
            )
