import json
import shutil
import unittest
from unittest.mock import patch
import uuid
import tempfile
from fastapi.testclient import TestClient

from dbt_server import views, crud
from dbt_server.state import StateController
from dbt_server.models import Task, Base
from dbt_server.services.filesystem_service import DBT_LOG_FILE_NAME
import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dbt_server.views import app


class TestDbtEntry(unittest.TestCase):
    
    def setUp(self):
        self.client = TestClient(app)
        self.temp_dir = tempfile.TemporaryDirectory()
        os.environ['__DBT_WORKING_DIR'] = self.temp_dir.name

        self.state_id = "test123"
        self.state_dir = f'{self.temp_dir.name}/state-{self.state_id}'
        shutil.copytree('tests/e2e/fixtures/test-project', self.state_dir)

        self.engine = create_engine(f'sqlite:///{self.temp_dir.name}/sql_app.db', echo=True, connect_args={'check_same_thread': False})
        Base.metadata.create_all(bind=self.engine, tables=[Task.__table__])
        self.SessionLocal = sessionmaker(bind=self.engine)
        self.SessionLocal.configure(bind=self.engine, expire_on_commit=False)
        self.db = self.SessionLocal()
        app.dependency_overrides[crud.get_db] = self.mock_get_db

    def tearDown(self):
        self.db.close()
        self.temp_dir.cleanup()

    def mock_get_db(self):
        return self.SessionLocal()

    @patch('dbt.parser.manifest.ManifestLoader.track_project_load')
    def test_dbt_entry_project_path(self, mock_tracking):
        """
        Test that parse with a state-id results in manifest cacheing and
        subsequent call of the async command endpoint pulls the correct manifest.
        
        Also test that expected log file is created and populated with valid json logs
        """
        args = views.ParseArgs(project_path=self.state_dir)
        state = StateController.parse_from_source(args)
        state.serialize_manifest()
        state.update_cache()
        
        args = views.dbtCommandArgs(command=['run', '--threads', 1])
        response = self.client.post("/async/dbt", json=args.dict())

        self.assertEqual(response.status_code, 200)
        json_response = response.json()

        # check that the task_id is a valid uuid
        self.assertTrue(isinstance(uuid.UUID(json_response['task_id']), uuid.UUID))

        self.assertEqual(json_response['state'], 'pending')
        self.assertEqual(json_response['command'], 'run --threads 1')

        expected_log_path = f'{self.temp_dir.name}/{json_response.get("task_id")}/{DBT_LOG_FILE_NAME}'
        self.assertEqual(json_response['log_path'], expected_log_path)

        # check that the task is added to the database
        task = self.db.query(Task).filter_by(task_id=json_response['task_id']).first()
        self.assertIsNotNone(task)
        self.assertEqual(task.command, 'run --threads 1')
        self.assertEqual(task.log_path, expected_log_path)

        # check that log file is populated with valid json logs
        data = []
        with open(expected_log_path) as f:
            for line in f:
                data.append(json.loads(line))

        self.assertTrue(data)
    
    @patch('dbt_server.views.StateController.execute_async_command')
    @patch('dbt.parser.manifest.ManifestLoader.track_project_load')
    def test_dbt_entry_state_id(self, mock_tracking, mock_execute):
        """
        Test that parse with a state-id results in manifest cacheing and
        subsequent call of the async command endpoint pulls the correct manifest.

        Mocks actual command execution to prevent log files being written in
        permanent directory
        """
        args = views.ParseArgs(state_id=self.state_id)
        state = StateController.parse_from_source(args)
        state.serialize_manifest()
        state.update_cache()
        
        args = views.dbtCommandArgs(command=['run', '--threads', 1])
        response = self.client.post("/async/dbt", json=args.dict())

        self.assertEqual(response.status_code, 200)
        json_response = response.json()

        # check that the task_id is a valid uuid
        self.assertTrue(isinstance(uuid.UUID(json_response['task_id']), uuid.UUID))

        self.assertEqual(json_response['state'], 'pending')
        self.assertEqual(json_response['command'], 'run --threads 1')

        expected_log_path = f'{self.state_dir}/{json_response.get("task_id")}/dbt.log'
        self.assertEqual(json_response['log_path'], expected_log_path)

        # check that the task is added to the database
        task = self.db.query(Task).filter_by(task_id=json_response['task_id']).first()
        self.assertIsNotNone(task)
        self.assertEqual(task.command, 'run --threads 1')
        self.assertEqual(task.log_path, expected_log_path)


    def test_dbt_entry_no_state_found(self):
        """
        Test that calling the async/dbt endpoint without first calling parse
        results in a properly handled StateNotFoundException
        """
        args = views.dbtCommandArgs(command=['run', '--threads', 1])
        response = self.client.post("/async/dbt", json=args.dict())
        self.assertEqual(response.status_code, 422)
