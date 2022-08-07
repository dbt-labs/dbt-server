from contextlib import contextmanager
from typing import Generator

import pytest
from clients.filesystem import FilesystemClient
from tests.context import temp_dir


@pytest.mark.parametrize(
    "path", ["some_file.txt", "abc/some_file.txt", "abc/def/123/some_file.txt"]
)
def test_put_read_delete_flow(path: str):
    with test_filesystem_client() as client:
        assert client.get(path) is None
        client.put(path, "contents")
        assert client.get(path) == "contents"
        client.delete(path)
        assert client.get(path) is None


@contextmanager
def test_filesystem_client() -> Generator[FilesystemClient, None, None]:
    with temp_dir() as tmp_dir_path:
        test_client = FilesystemClient(directory=tmp_dir_path)

        yield test_client
