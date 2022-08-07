import uuid
from contextlib import contextmanager
from typing import Generator

from fs.tempfs import TempFS


@contextmanager
def temp_dir(identifier: str = "test") -> Generator[str, None, None]:
    """
    Creates a temporary directory and yields its path.
    The temporary directory is destroyed when the context block is exited.

    Created as an alternative to the pytest tmpdir fixture, because Python type hints are lost when injecting pytest fixtures into tests.
    """
    id = uuid.uuid4()
    fs = TempFS(identifier=f"{identifier}-{id}")

    yield fs.getospath(".").decode()

    fs.close()
