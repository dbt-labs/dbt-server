import os
import shutil

ROOT_PATH = "./working-dir"


def get_root_path(state_id):
    return os.path.join(ROOT_PATH, f"state-{state_id}")


def get_latest_state_file_path():
    return os.path.join(ROOT_PATH, "latest-state-id.txt")


def get_path(state_id, *path_parts):
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


def get_latest_state_id(state_id):
    if not state_id:
        path = os.path.abspath(get_latest_state_file_path())
        with open(path, 'r') as latest_path_file:
            state_id = latest_path_file.read()
    return state_id


def update_state_id(state_id):
    path = os.path.abspath(get_latest_state_file_path())
    with open(path, 'w+') as latest_path_file:
        latest_path_file.write(state_id)
