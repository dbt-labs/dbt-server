import uuid
from contextlib import contextmanager
from typing import Generator

from fs.tempfs import TempFS


@contextmanager
def temp_dir(identifier: str = "test") -> Generator[str, None, None]:
    id = uuid.uuid4()
    fs = TempFS(identifier=f"{identifier}-{id}")

    yield fs.getospath(".").decode()

    fs.close()
