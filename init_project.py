import re
import requests
import time
import sys
import hashlib
import os
import json

import yaml

# CALL THIS SCRIPT WITH A PROJECT_DIR, OR FROM THE ROOT OF A DBT PROJECT
# ex. python test_server.py ../jaffle_shop

if len(sys.argv) > 1:
    project_dir = sys.argv[1]
else:
    project_dir = None


SERVER_HOST_PUSH = "http://0.0.0.0:8585/push"
SERVER_HOST_PARSE = "http://0.0.0.0:8585/parse"
SERVER_HOST_COMPILE = "http://0.0.0.0:8585/compile"
SERVER_HOST_MEM = "http://0.0.0.0:8585/"

VALID_FILE_EXTENSIONS = re.compile(r"^.+\.(sql|yml|yaml|md|csv|py|dbtignore)$")

PROJECT_PATH_KEYS = {
    "model-paths": {
        "default": ["models"],
        "aliases": ["source-paths"],
    },
    "analysis-paths": {
        "default": ["analyses"],
        "aliases": [],
    },
    "test-paths": {
        "default": ["tests"],
        "aliases": [],
    },
    "seed-paths": {
        "default": ["seeds"],
        "aliases": ["data-paths"],
    },
    "macro-paths": {
        "default": ["macros"],
        "aliases": [],
    },
    "snapshot-paths": {
        "default": ["snapshots"],
        "aliases": [],
    },
}

PACKAGE_PATH_KEY = "packages-install-path"
PACKAGE_PATH_DEFAULT = "dbt_packages"


def resolve_project_dir(dirname):
    if dirname is None:
        return os.path.abspath(os.getcwd())
    return os.path.abspath(dirname)


def walk(root_dir):
    if os.path.isfile(root_dir):
        yield root_dir
        return

    for root, _, files in os.walk(root_dir):
        for name in files:
            path = os.path.join(root, name)
            if os.path.isdir(path):
                yield from walk(path)
            else:
                yield path


def load(filepath):
    with open(filepath, "rb") as fh:
        contents = fh.read()
        try:
            decoded_contents = contents.decode("utf-8")
        except UnicodeDecodeError:
            return None

        return {
            "contents": decoded_contents,
            "hash": hashlib.md5(contents).hexdigest(),
            "path": filepath,
        }


def read_project(project_dir, paths):
    manifest = {}
    for path in paths:
        for file in walk(path):
            relpath = os.path.relpath(file, project_dir)
            if is_syncable_file(relpath):
                file_contents = load(file)
                if file_contents is not None:
                    manifest[relpath] = file_contents
    return manifest


def get_hash(s):
    return hashlib.md5(json.dumps(s).encode()).hexdigest()


def get_state(project_dir):
    dirs = get_all_project_paths(project_dir)
    return read_project(project_dir, dirs)


def is_syncable_file(path):
    return re.match(VALID_FILE_EXTENSIONS, path) is not None


def get_hash(s):
    return hashlib.md5(json.dumps(s).encode()).hexdigest()


def get_packages(project_dir):
    project_dir = resolve_project_dir(project_dir)
    packages_path = get_packages_yml_path(project_dir)
    if packages_path is None:
        return None
    return read_packages_to_json(packages_path)


def get_packages_yml_path(project_dir):
    dbt_packages_yml_path = os.path.join(project_dir, "packages.yml")
    if not os.path.exists(dbt_packages_yml_path):
        return None
    return dbt_packages_yml_path


def get_project_paths(project_dir):
    """
    Returns all paths referenced from a dbt_project.yml file
    """
    dbt_project_yml_path = get_projects_yml_path(project_dir)
    dbtignore_path = os.path.join(project_dir, ".dbtignore")
    project_config = get_project_config(dbt_project_yml_path)

    all_paths = [dbt_project_yml_path, dbtignore_path]
    for key, key_info in PROJECT_PATH_KEYS.items():
        candidate_keys = [key] + key_info["aliases"]
        default = key_info.get("default", [])
        config_value = resolve_project_path_key(project_config, candidate_keys, default)
        all_paths += config_value

    # Relative to project dir
    return [os.path.join(project_dir, path) for path in all_paths]


def get_all_project_paths(project_dir):
    project_paths = []
    packages_yml_path = get_packages_yml_path(project_dir)
    selectors_yml_path = get_selectors_yml_path(project_dir)
    if packages_yml_path is not None:
        project_paths.append(packages_yml_path)
    if selectors_yml_path is not None:
        project_paths.append(selectors_yml_path)
    project_root_paths = [project_dir] + get_package_dirs(project_dir)

    for project in project_root_paths:
        project_paths += get_project_paths(project)
    return project_paths


def resolve_project_path_key(project_config, candidate_keys, default):
    NotFound = object()
    for key in candidate_keys:
        config_value = project_config.get(key, NotFound)
        if config_value is NotFound:
            continue
        elif isinstance(config_value, str):
            return [config_value]
        elif isinstance(config_value, (tuple, list)):
            return list(config_value)
        else:
            raise RuntimeError(
                f"Key {key} in dbt_project.yml file is not" "a list or string"
            )
    return default


def get_selectors_yml_path(project_dir):
    selectors_yml_path = os.path.join(project_dir, "selectors.yml")
    if not os.path.exists(selectors_yml_path):
        return None
    return selectors_yml_path


def get_package_dirs(project_dir):
    package_dir_paths = []
    package_path = get_package_install_root_path(project_dir)
    root_dir = os.path.join(project_dir, package_path)

    if not os.path.exists(root_dir):
        return package_dir_paths

    for name in os.listdir(root_dir):
        path = os.path.join(root_dir, name)
        if os.path.isdir(path):
            package_dir_paths.append(path)
    return package_dir_paths


def get_package_install_root_path(project_dir):
    dbt_project_yml_path = get_projects_yml_path(project_dir)
    project_config = get_project_config(dbt_project_yml_path)
    return project_config.get(PACKAGE_PATH_KEY, PACKAGE_PATH_DEFAULT)


def get_project_config(dbt_project_yml_path):
    with open(dbt_project_yml_path) as fh:
        project_code = fh.read()
    return yaml.safe_load(project_code)


def get_projects_yml_path(project_dir):
    dbt_project_yml_path = os.path.join(project_dir, "dbt_project.yml")
    if not os.path.exists(dbt_project_yml_path):
        raise Exception(
            f"dbt_project.yml file not found at {project_dir}."
            " Is this a valid dbt project?"
        )
    return dbt_project_yml_path


def read_packages_to_json(path):
    with open(path, "rb") as file:
        contents = file.read()
    package_config = yaml.safe_load(contents)
    return json.dumps(package_config)


project_dir = resolve_project_dir(project_dir)
state = get_state(project_dir)
state_id = get_hash(state)

# POST /push
req = requests.post(SERVER_HOST_PUSH, json={"state_id": state_id, "body": state})
print("PUSH", req.status_code)

# POST /parse
req = requests.post(SERVER_HOST_PARSE, json={"state_id": state_id})
print("PARSE", req.status_code)

print("state_id: ", state_id)
