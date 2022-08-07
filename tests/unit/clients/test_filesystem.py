import uuid
from contextlib import contextmanager
from typing import Generator

import pytest
from clients.filesystem import FilesystemClient
from fs.tempfs import TempFS


@pytest.mark.parametrize(
    "path", ["some_file.txt", "abc/some_file.txt", "abc/def/123/some_file.txt"]
)
def test_filesystem_put_read_delet(path):
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


@contextmanager
def temp_dir(identifier: str = "test") -> Generator[str, None, None]:
    id = uuid.uuid4()
    fs = TempFS(identifier=f"{identifier}-{id}")

    yield fs.getospath(".").decode()

    fs.close()
