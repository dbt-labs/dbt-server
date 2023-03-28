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

#
# File system tree structure.
#

# Case 1: with local dbt project.
# root_path(dbt project)
#
# target_path
# |
# - partial_parse_path(partial_parse.msgpack)
#
# working_dir
# |
# - task_artifacts_path
# |        |
# |        - log_path(dbt.log)
# |
# - db_path(sqlite)
# |
# - latest_state_file_path
# |
# - latest_project_path_file_path

# Case 2: with state_id(i.e. without local dbt project)
# root_path(dbt project)
#
# target_path
# |
# - partial_parse_path(partial_parse.msgpack)
#
# working_dir
# |
# - root_path(per state_id)
# |        |
# |        |
# |        - task_artifacts_path
# |                 |
# |                 - log_path(dbt.log)
# |
# - db_path(sqlite)
# |
# - latest_state_file_path
# |
# - latest_project_path_file_path


def get_working_dir():
    """Returns dbt-server working directory which has dbt-server generated
    files."""
    return os.path.abspath(os.environ.get("__DBT_WORKING_DIR", DEFAULT_WORKING_DIR))


def get_target_path():
    """Returns dbt-core compiled target path."""
    # TODO: The --target-path flag should override this, but doesn't
    # appear to be working on invoke. When it does, need to revisit
    # how partial parsing is working
    return os.path.abspath(os.environ.get("DBT_TARGET_PATH", DEFAULT_TARGET_DIR))


def get_root_path(state_id: str = None, project_path: str = None):
    """Returns root path of dbt project. Only one of `state_id` and
    `project_path` should be passed in. If both arguments are None, returns
    None.

    Args:
        state_id: if set, we infer the dbt project is not stored locally,
            instead it's pushed by client. Hence root path is a directory
            that stores the current state of the pushed dbt project.
        project_path: if set, we infer the dbt project is stored in a local
            fixed directory which is the root path."""
    if project_path is not None:
        return os.path.abspath(project_path)
    if state_id is None:
        return None
    working_dir = get_working_dir()
    return os.path.abspath(os.path.join(working_dir, f"state-{state_id}"))


def get_task_artifacts_path(task_id, state_id=None):
    """Returns artifacts path of dbt-core async style invocation.
    Args:
        task_id: identify which async task the log file belongs to.
        state_id: optional, only used for pushed style dbt project."""
    working_dir = get_working_dir()
    if state_id is None:
        path = os.path.join(working_dir, task_id)
    else:
        path = os.path.join(working_dir, f"state-{state_id}", task_id)
    return os.path.abspath(path)


def get_log_path(task_id, state_id=None):
    """Returns path of dbt-core generated log file for async style invocation.

    Args:
        task_id: identify which async task the log file belongs to.
        state_id: optional, only used for pushed style dbt project."""
    artifacts_path = get_task_artifacts_path(task_id, state_id)
    return os.path.join(artifacts_path, DBT_LOG_FILE_NAME)


def get_db_path():
    """Returns local metadata database data file path. Creates directory if
    not existed."""
    working_dir = get_working_dir()
    path = os.path.abspath(os.path.join(working_dir, DATABASE_FILE_NAME))
    _ensure_dir_exists(path)
    return path


def get_latest_state_file_path():
    """Returns local recorded dbt-server lastest state id file path."""
    working_dir = get_working_dir()
    return os.path.join(working_dir, "latest-state-id.txt")


def get_latest_project_path_file_path():
    """Returns local recorded dbt-server lastest project path file path."""
    working_dir = get_working_dir()
    return os.path.join(working_dir, "latest-project-path.txt")


def get_path(*path_parts):
    return os.path.join(*path_parts)


@tracer.wrap
def get_size(path: str):
    """Returns file size specified by `path`."""
    return os.path.getsize(path)


@tracer.wrap
def _ensure_dir_exists(path: str):
    """Check directory of `path` exists, if not make new directory
    recursively."""
    dirname = os.path.dirname(path)
    if not os.path.exists(dirname):
        os.makedirs(dirname)


@tracer.wrap
def write_file(path: str, contents: str):
    """Writes `contents` encoded as utf-8 into `path`. The direcory of `path`
    will be created recursively if not existed."""
    _ensure_dir_exists(path)

    with open(path, "wb") as fh:
        if isinstance(contents, str):
            contents = contents.encode("utf-8")
        fh.write(contents)


@tracer.wrap
def copy_file(source_path: str, dest_path: str):
    """Copies file from `source_path` to `dest_path`. The directory of
    `dest_path` will be created recursively if it doesn't exist."""
    _ensure_dir_exists(dest_path)
    shutil.copyfile(source_path, dest_path)


@tracer.wrap
def read_serialized_manifest(path: str):
    """Returns serialized manifest file from `path`.

    Raises:
        StateNotFoundException: if file is not found.
    """
    try:
        with open(path, "rb") as fh:
            return fh.read()
    except FileNotFoundError as e:
        raise StateNotFoundException(e)


@tracer.wrap
def write_unparsed_manifest_to_disk(
    state_id: str, previous_state_id: str, filedict: dict
):
    """Writes files in `filedict` to root path specified by `state_id`, then
    copies previous partial parsed msgpack to current root path.

    Args:
        state_id: required to get root path.
        previous_state_id: if it's none, we'll skip copy previous partial parsed
            msgpack to current root path.
        filedict: key is file name and value is FileInfo."""
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
def get_latest_state_id(state_id: str):
    """Returns dbt-server latest processed state id.

    Args:
        state_id: if `state_id` is none, returns state id from local persisted
            storage, otherwise returns `state_id` directly.
    """
    if not state_id:
        path = os.path.abspath(get_latest_state_file_path())
        if not os.path.exists(path):
            return None
        with open(path, "r") as latest_path_file:
            state_id = latest_path_file.read().strip()
    return state_id


@tracer.wrap
def get_latest_project_path():
    """Returns dbt-server latest processed project path, read from local
    persisted storage. Returns None if not found."""
    path = os.path.abspath(get_latest_project_path_file_path())
    if not os.path.exists(path):
        return None
    with open(path, "r") as latest_path_file:
        project_path = latest_path_file.read().strip()
    return project_path


@tracer.wrap
def update_state_id(state_id: str):
    """Updates local persisted `state_id`."""
    path = os.path.abspath(get_latest_state_file_path())
    _ensure_dir_exists(path)
    with open(path, "w+") as latest_path_file:
        latest_path_file.write(state_id)


@tracer.wrap
def update_project_path(project_path: str):
    """Updates local persisted `project_path`."""
    path = os.path.abspath(get_latest_project_path_file_path())
    _ensure_dir_exists(path)
    with open(path, "w+") as latest_path_file:
        latest_path_file.write(project_path)
