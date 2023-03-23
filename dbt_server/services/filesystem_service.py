import os
import shutil
from dbt_server.logging import GLOBAL_LOGGER as logger
from dbt_server.exceptions import StateNotFoundException
from dbt_server import tracer


DEFAULT_WORKING_DIR = os.path.join(os.getcwd(), "working-dir")
PARTIAL_PARSE_FILE = "partial_parse.msgpack"
DEFAULT_TARGET_DIR = os.path.join(os.getcwd(), "target")


def get_working_dir():
    return os.environ.get(
        "__DBT_WORKING_DIR",
        DEFAULT_WORKING_DIR,
    )


def get_root_path(state_id):
    working_dir = get_working_dir()
    return os.path.join(working_dir, f"state-{state_id}")


def get_latest_state_file_path():
    working_dir = get_working_dir()
    return os.path.join(working_dir, "latest-state-id.txt")


def get_path(*path_parts):
    return os.path.abspath(os.path.join(*path_parts))


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
def read_serialized_manifest(path):
    try:
        with open(path, "rb") as fh:
            return fh.read()
    except FileNotFoundError as e:
        raise StateNotFoundException(e)


@tracer.wrap
def _ensure_dir_exists(path: str):
    """Check directory of `path` exists, if not make new directory
    recursively."""
    dirname = os.path.dirname(path)
    if not os.path.exists(dirname):
        os.makedirs(dirname)

@tracer.wrap
def copy_file(source_path: str, dest_path: str):
    """Copies file from `source_path` to `dest_path`. The directory of
    `dest_path` will be created recursively if it doesn't exist."""
    _ensure_dir_exists(dest_path)
    shutil.copyfile(source_path, dest_path)


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
        previous_partial_parse_path = get_path(
            get_root_path(previous_state_id), "target", PARTIAL_PARSE_FILE
        )
        new_partial_parse_path = get_path(root_path, "target", PARTIAL_PARSE_FILE)
        if not os.path.exists(previous_partial_parse_path):
            return
        copy_file(previous_partial_parse_path, new_partial_parse_path)


def get_target_path():
    """Returns dbt-core compiled target path."""
    # TODO: The --target-path flag should override this, but doesn't
    # appear to be working on invoke. When it does, need to revisit
    # how partial parsing is working
    return os.environ.get("DBT_TARGET_PATH", DEFAULT_TARGET_DIR)


def get_partial_parse_path():
    """Returns dbt-core compiled partial parse file."""
    target_path = get_target_path()
    return os.path.join(target_path, PARTIAL_PARSE_FILE)


@tracer.wrap
def get_latest_state_id(state_id):
    if not state_id:
        path = os.path.abspath(get_latest_state_file_path())
        if not os.path.exists(path):
            logger.error("No state id included in request, no previous state id found.")
            return None
        with open(path, "r") as latest_path_file:
            state_id = latest_path_file.read().strip()
    return state_id


@tracer.wrap
def update_state_id(state_id):
    path = os.path.abspath(get_latest_state_file_path())
    with open(path, "w+") as latest_path_file:
        latest_path_file.write(state_id)
