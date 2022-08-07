import json
from typing import Literal

from clients.filesystem import FilesystemClient
from dbt_server.views import FileInfo


def get_unparsed_manifest(name: Literal["jaffle_shop_metrics"]) -> FileInfo:
    root = FilesystemClient(".")
    path = f"tests/feature/test_data/unparsed_manifests/{name}.json"
    text = root.get(path)
    if text is None:
        raise Exception(f"Unable to find unparsed manifest: {path}")
    return json.loads(text)
