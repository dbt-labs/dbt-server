
import os, shutil
from dbt_server.logging import GLOBAL_LOGGER as logger

ROOT_PATH = "./working-dir"

def get_root_path(state_id):
    return os.path.join(ROOT_PATH, f"state-{state_id}")

def get_symlink_path():
    return os.path.join(ROOT_PATH, "latest-state-manifest")

def get_path(state_id, *path_parts):
    if not state_id:
        return get_symlink_path()
    return os.path.join(get_root_path(state_id), *path_parts)

def ensure_dir_exists(path):
    dirname = os.path.dirname(path)
    if not os.path.exists(dirname):
        os.makedirs(dirname)

def write_file(path, contents):
    ensure_dir_exists(path)

    with open(path, 'wb') as fh:
        if isinstance(contents, str):
            contents = contents.encode('utf-8')
        fh.write(contents)

def read_file(path):
    with open(path, 'rb') as fh:
        return fh.read()

def write_unparsed_manifest_to_disk(state_id, filedict):
    root_path = get_root_path(state_id)
    if os.path.exists(root_path):
        shutil.rmtree(root_path)

    for filename, body in filedict.items():
        path = get_path(state_id, filename)
        write_file(path, body['contents'])

def update_symlink(serialize_path):
    symlink_path = os.path.abspath(get_symlink_path())
    serialize_absolute_path = os.path.abspath(serialize_path)
    if os.path.exists(symlink_path):
        os.unlink(symlink_path)
    os.symlink(serialize_absolute_path, symlink_path)

    if os.path.realpath(symlink_path) != serialize_absolute_path:
        logger.error(f"Symlink was not successfully updated to {serialize_absolute_path}")
