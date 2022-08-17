import os

this_file = os.path.realpath(__file__)
this_dir = os.path.dirname(this_file)

PROFILES_YML_POSTGRES = os.path.join(this_dir, "profiles", "postgres")


class Profiles:
    Postgres = PROFILES_YML_POSTGRES
