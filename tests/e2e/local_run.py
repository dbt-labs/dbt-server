# It's pretty similar to smoke_test.py that it can sends commands to your local
# dbt-server for testing purpose.
#
# Example run:
# # Under root directory.
# python3 -m tests.e2e.local_run --local_task_db_path=working-dir/sql_app.db \
# --dbt_local_server_port=8580 --dbt_project_path=tests/e2e/fixtures/jaffle_shop
#
from absl import app
from absl import flags
import logging
from dbt_server.models import TaskState
from tests.e2e.smoke_test_utils import DbtServerSmokeTest
from tests.e2e.smoke_test_utils import read_testcase_file

flags.DEFINE_integer(
    "command_exec_timeout_seconds",
    60,
    "How many seconds shall we wait after command is issued. Test will fail if"
    "timeout happens.",
)

flags.DEFINE_integer(
    "dbt_local_server_port", 8580, "Dbt local server port for testing."
)

flags.DEFINE_string(
    "local_task_db_path",
    None,
    "A local path point to dbt-server sqlite db file, task status will be " "checked.",
)

flags.DEFINE_string(
    "testcase_path", "tests/e2e/testcases/local_run.txt", "Testcase file path."
)

flags.DEFINE_string("dbt_project_path", None, "Dbt project path.")

flags.mark_flag_as_required("local_task_db_path")
flags.mark_flag_as_required("dbt_project_path")


def main(argv):
    del argv  # Unused.
    dbt_local_server_port = flags.FLAGS.dbt_local_server_port
    local_task_db_path = flags.FLAGS.local_task_db_path
    dbt_project_path = flags.FLAGS.dbt_project_path
    testcase_path = flags.FLAGS.testcase_path
    command_exec_timeout_seconds = flags.FLAGS.command_exec_timeout_seconds

    smoke_test = DbtServerSmokeTest(
        dbt_project_path, dbt_local_server_port, local_task_db_path
    )
    # Manually parse the project first.
    smoke_test.parse()
    commands = read_testcase_file(testcase_path)
    for command in commands:
        try:
            smoke_test.run_async_testcase(
                command.split(), TaskState.FINISHED, command_exec_timeout_seconds
            )
        except Exception as e:
            logging.error(str(e))


if __name__ == "__main__":
    app.run(main)
