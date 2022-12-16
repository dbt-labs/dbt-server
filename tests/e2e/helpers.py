import contextlib
import os
import dbt


@contextlib.contextmanager
def profiles_dir(profiles_yml_dir):
    original_value = dbt.flags.PROFILES_DIR
    original_env_var = os.environ.get("DBT_PROFILES_DIR", "")

    dbt.flags.PROFILES_DIR = profiles_yml_dir
    os.environ["DBT_PROFILES_DIR"] = profiles_yml_dir

    yield

    dbt.flags.PROFILES_DIR = original_value
    os.environ["DBT_PROFILES_DIR"] = original_env_var
