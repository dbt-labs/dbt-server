import os
import shutil
from dbt_server.exceptions import StateNotFoundException
from dbt_server import tracer


PARTIAL_PARSE_FILE = "partial_parse.msgpack"
DEFAULT_WORKING_DIR = "./working-dir"
DEFAULT_TARGET_DIR = "./target"
DATABASE_FILE_NAME = "sql_app.db"
# This is defined in dbt-core-- dir path is configurable but not filename
DBT_LOG_FILE_NAME = "dbt.log"


def get_working_dir():
    return os.environ.get("__DBT_WORKING_DIR", DEFAULT_WORKING_DIR)


def get_target_path():
    # TODO: The --target-path flag should override this, but doesn't
    # appear to be working on invoke. When it does, need to revisit
    # how partial parsing is working
    return os.environ.get("DBT_TARGET_PATH", DEFAULT_TARGET_DIR)


def get_root_path(state_id=None, project_path=None):
    if project_path is not None:
        return os.path.abspath(project_path)
    if state_id is None:
        return None
    working_dir = get_working_dir()
    return os.path.join(working_dir, f"state-{state_id}")


def get_task_artifacts_path(task_id, state_id=None):
    working_dir = get_working_dir()
    if state_id is None:
        return os.path.join(working_dir, task_id)
    return os.path.join(working_dir, f"state-{state_id}", task_id)


def get_log_path(task_id, state_id=None):
    artifacts_path = get_task_artifacts_path(task_id, state_id)
    return os.path.join(artifacts_path, DBT_LOG_FILE_NAME)


def get_partial_parse_path():
    target_path = get_target_path()
    return os.path.join(target_path, PARTIAL_PARSE_FILE)


def get_db_path():
    working_dir = get_working_dir()
    path = os.path.join(working_dir, DATABASE_FILE_NAME)
    ensure_dir_exists(path)
    return path


def get_latest_state_file_path():
    working_dir = get_working_dir()
    return os.path.join(working_dir, "latest-state-id.txt")


def get_latest_project_path_file_path():
    working_dir = get_working_dir()
    return os.path.join(working_dir, "latest-project-path.txt")


def get_path(*path_parts):
    return os.path.join(*path_parts)


@tracer.wrap
def get_size(path):
    return os.path.getsize(path)


@tracer.wrap
def ensure_dir_exists(path):
    dirname = os.path.dirname(path)
    if not os.path.exists(dirname):
        os.makedirs(dirname)


@tracer.wrap
def write_file(path, contents):
    ensure_dir_exists(path)

    with open(path, "wb") as fh:
        if isinstance(contents, str):
            contents = contents.encode("utf-8")
        fh.write(contents)


@tracer.wrap
def copy_file(source_path, dest_path):
    ensure_dir_exists(dest_path)
    shutil.copyfile(source_path, dest_path)


@tracer.wrap
def read_serialized_manifest(path):
    try:
        with open(path, "rb") as fh:
            return fh.read()
    except FileNotFoundError as e:
        raise StateNotFoundException(e)


@tracer.wrap
def write_unparsed_manifest_to_disk(state_id, previous_state_id, filedict):
    root_path = get_root_path(state_id)
    if os.path.exists(root_path):
        shutil.rmtree(root_path)

    for filename, file_info in filedict.items():
        path = get_path(root_path, filename)
        write_file(path, file_info.contents)

    if previous_state_id and state_id != previous_state_id:
        #  TODO: The target folder is usually created during command runs and won't exist on push/parse
        #  of a new state. It can also be named by env var or flag -- hardcoding as this will change
        #  with the click API work. This bypasses the DBT_TARGET_PATH env var.
        previous_partial_parse_path = get_path(
            get_root_path(previous_state_id), "target", PARTIAL_PARSE_FILE
        )
        new_partial_parse_path = get_path(root_path, "target", PARTIAL_PARSE_FILE)
        if not os.path.exists(previous_partial_parse_path):
            return
        copy_file(previous_partial_parse_path, new_partial_parse_path)


@tracer.wrap
def get_latest_state_id(state_id):
    if not state_id:
        path = os.path.abspath(get_latest_state_file_path())
        if not os.path.exists(path):
            return None
        with open(path, "r") as latest_path_file:
            state_id = latest_path_file.read().strip()
    return state_id


@tracer.wrap
def get_latest_project_path():
    path = os.path.abspath(get_latest_project_path_file_path())
    if not os.path.exists(path):
        return None
    with open(path, "r") as latest_path_file:
        project_path = latest_path_file.read().strip()
    return project_path


@tracer.wrap
def update_state_id(state_id):
    path = os.path.abspath(get_latest_state_file_path())
    ensure_dir_exists(path)
    with open(path, "w+") as latest_path_file:
        latest_path_file.write(state_id)


@tracer.wrap
def update_project_path(project_path):
    path = os.path.abspath(get_latest_project_path_file_path())
    ensure_dir_exists(path)
    with open(path, "w+") as latest_path_file:
        latest_path_file.write(project_path)
