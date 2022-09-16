from unittest import TestCase, mock

import os
from dbt_server.services import filesystem_service


class FilesystemServiceTest(TestCase):
    def test_dbt_working_dir_default(self):
        self.assertEqual(
            filesystem_service.get_root_path("abc123"), "./working-dir/state-abc123"
        )

        self.assertEqual(
            filesystem_service.get_latest_state_file_path(),
            "./working-dir/latest-state-id.txt",
        )

    @mock.patch.dict(os.environ, {"__DBT_WORKING_DIR": "/opt/code/working-dir"})
    def test_dbt_working_dir_override(self):
        self.assertEqual(
            filesystem_service.get_root_path("abc123"),
            "/opt/code/working-dir/state-abc123",
        )

        self.assertEqual(
            filesystem_service.get_latest_state_file_path(),
            "/opt/code/working-dir/latest-state-id.txt",
        )
