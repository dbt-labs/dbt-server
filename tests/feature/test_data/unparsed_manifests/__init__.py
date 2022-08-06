import json
from typing import Literal

from dbt_server.views import FileInfo
from fs import open_fs


def get_unparsed_manifest(name: Literal["jaffle_shop_metrics"]) -> FileInfo:
    with open_fs(".") as root:
        return json.loads(
            root.readtext(f"tests/feature/test_data/unparsed_manifests/{name}.json")
        )
