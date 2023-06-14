import datetime
from typing import Union

import agate
from agate import Row
from dbt.cli.main import dbtRunnerResult
from dbt.contracts.files import FileHash
from dbt.contracts.graph.model_config import NodeConfig, ContractConfig
from dbt.contracts.graph.nodes import ResultNode, ParsedNode, CompiledNode, SqlNode, SourceDefinition, ManifestNode, \
    RefArgs, DependsOn, Contract
from dbt.contracts.graph.unparsed import Docs
from dbt.contracts.results import RunExecutionResult, RunResult, TimingInfo, RunStatus
from dbt.node_types import NodeType, ModelLanguage
from fastapi.testclient import TestClient
import unittest
from unittest.mock import patch, ANY, Mock

from dbt_server.server import app
from dbt_server.state import StateController, CachedManifest

from dbt_server.exceptions import dbtCoreCompilationException, StateNotFoundException
from dbt_server.views import SQLConfig

client = TestClient(app)


class PreviewSqlInterfaceTests(unittest.TestCase):

    def test_preview_sql_interface_no_sql(self):
        with patch("dbt_server.state.StateController.load_state") as state:
            response = client.post(
                "/preview",
                json={
                    "sql": None,
                    "state_id": "abc123",
                },
            )

            assert not state.called
            assert response.status_code == 422

    def test_preview_sql_interface_invalid_state_id(self):
        response = client.post(
            "/preview",
            json={
                "sql": "select {{ 1 + 1 }}",
                "state_id": "badid",
            },
        )
        # Compile with invalid state id returns a 422
        assert response.status_code == 422

    @patch("dbt_server.views.dbt_service.preview_sql")
    def test_preview_sql_interface_valid_state_id(self, preview_mock):
        state_id = "goodid"
        source_query = "select {{ 1 + 1 }}"
        compiled_query = "select 2 as id"

        state_mock = Mock(
            return_value=StateController(
                state_id=state_id,
                project_path=None,
                root_path=f"./working-dir/state-{state_id}",
                manifest=None,
                manifest_size=0,
                is_manifest_cached=False,
            )
        )

        preview_mock.return_value = {"compiled_code": compiled_query}
        with patch.multiple(
                "dbt_server.state.StateController",
                load_state=state_mock,
        ):
            response = client.post(
                "/preview",
                json={
                    "sql": source_query,
                    "state_id": state_id,
                    "target": "new_target",
                },
            )
            expected_preview_args = SQLConfig(
                state_id="goodid",
                sql="select {{ 1 + 1 }}",
                target="new_target",
                profile=None,
            )
            state_mock.assert_called_once_with(expected_preview_args)
            preview_mock.assert_called_once_with(
                None, "./working-dir/state-goodid", expected_preview_args
            )
            assert response.status_code == 200

            expected = {
                "parsing": state_id,
                "path": "./working-dir/state-goodid/manifest.msgpack",
                "res": ANY,
                "compiled_code": compiled_query,
            }
            assert response.json() == expected

    def test_preview_sql_interface_compilation_error(self):
        state_id = "badid"
        query = "select {{ exceptions.raise_compiler_error('bad')}}"

        with patch(
                "dbt_server.state.StateController.load_state",
                side_effect=dbtCoreCompilationException("Compilation error"),
        ) as state:
            response = client.post(
                "/preview",
                json={
                    "sql": query,
                    "state_id": state_id,
                },
            )

            state.assert_called_once_with(
                SQLConfig(
                    state_id="badid",
                    sql="select {{ exceptions.raise_compiler_error('bad')}}",
                    target=None,
                ),
            )
            assert response.status_code == 400

            expected = {
                "data": None,
                "message": "Compilation error",
                "status_code": 400,
            }
            assert response.json() == expected

    @patch("dbt.cli.main.dbtRunner.invoke")
    def test_preview_sql_interface_run_success(self, show_inline_mock):
        state_id = "goodid"
        source_query = "select {{ 1 + 1 }}"
        compiled_query = "select 2 as id"

        state_mock = Mock(
            return_value=StateController(
                state_id=state_id,
                project_path=None,
                root_path=f"./working-dir/state-{state_id}",
                manifest=None,
                manifest_size=0,
                is_manifest_cached=False,
            )
        )

        show_inline_mock.return_value = dbtRunnerResult(
            success=True,
            result=RunExecutionResult(
                elapsed_time=0.1104879379272461,
                results=[
                    RunResult(
                        agate_table=agate.Table(
                            rows=[2],
                            column_names=("id",),
                            column_types=[agate.data_types.number.Number()],
                            _is_fork=True
                        ),
                        node=SqlNode(database='postgres',
                                     schema='naive_demo',
                                     name='inline_query',
                                     resource_type=NodeType.SqlOperation,
                                     package_name='dbt_project',
                                     path='sql/inline_query',
                                     original_file_path='from remote system.sql',
                                     unique_id='sql operation.dbt_project.inline_query',
                                     fqn=['dbt_project', 'sql', 'inline_query'],
                                     alias='inline_query',
                                     checksum=FileHash(name='sha256',
                                                       checksum='f166cbc36fc8e5d3a56733612a9fa1fe8109fcef20b27cd0e4b248550762c55b'),
                                     config=NodeConfig(_extra={}, enabled=True, alias=None, schema=None, database=None,
                                                       tags=[], meta={}, group=None, materialized='table',
                                                       incremental_strategy=None, persist_docs={}, post_hook=[],
                                                       pre_hook=[], quoting={}, column_types={}, full_refresh=None,
                                                       unique_key=None, on_schema_change='ignore', grants={},
                                                       packages=[], docs=Docs(show=True, node_color=None),
                                                       contract=ContractConfig(enforced=False)),
                                     _event_status={},
                                     tags=[],
                                     description='', columns={}, meta={}, group=None,
                                     docs=Docs(show=True, node_color=None), patch_path=None, build_path=None,
                                     deferred=False,
                                     unrendered_config={'materialized': 'table'}, created_at=1686712012.076523,
                                     config_call_dict={},
                                     relation_name=None,
                                     raw_code=source_query,
                                     language=ModelLanguage.sql,
                                     refs=[RefArgs(name='tmp_etl_task', package=None, version=None)],
                                     sources=[], metrics=[],
                                     depends_on=DependsOn(macros=[], nodes=['model.dbt_project.tmp_etl_task']),
                                     compiled_path='target/compiled/dbt_project/from remote system.sql/sql/inline_query',
                                     compiled=True,
                                     compiled_code=compiled_query,
                                     extra_ctes_injected=True, extra_ctes=[], _pre_injected_sql=None,
                                     contract=Contract(enforced=False, checksum=None)
                                     ),
                        timing=[TimingInfo(name='compile', started_at=datetime.datetime(2023, 6, 14, 3, 6, 52, 93766),
                                           completed_at=datetime.datetime(2023, 6, 14, 3, 6, 52, 96044)),
                                TimingInfo(name='execute', started_at=datetime.datetime(2023, 6, 14, 3, 6, 52, 96641),
                                           completed_at=datetime.datetime(2023, 6, 14, 3, 6, 52, 198098))],
                        status=RunStatus.Success,
                        thread_id=None,
                        execution_time=0.004255056381225586,
                        adapter_response={},
                        message=None,
                        failures=None,
                    )
                ],
                args={},
                generated_at=datetime.datetime.now()
            )
        )

        with patch.multiple(
                "dbt_server.state.StateController",
                load_state=state_mock,
        ):
            response = client.post(
                "/preview",
                json={
                    "sql": source_query,
                    "state_id": state_id,
                    "target": "new_target",
                },
            )
            expected_preview_args = SQLConfig(
                state_id="goodid",
                sql="select {{ 1 + 1 }}",
                target="new_target",
                profile=None,
            )
            state_mock.assert_called_once_with(expected_preview_args)
            assert response.status_code == 200

            expected = {
                "parsing": state_id,
                "path": "./working-dir/state-goodid/manifest.msgpack",
                "res": ANY,
                "compiled_code": compiled_query,
            }
            assert response.json() == expected
