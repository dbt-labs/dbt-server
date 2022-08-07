import os
from dataclasses import dataclass

from fs import open_fs


@dataclass
class FilesystemClient:
    directory: str

    def get(self, path: str) -> str | None:
        with open_fs(self.directory) as root:
            if root.exists(path):
                return root.readtext(path)
            else:
                return None

    def put(self, path: str, contents: str) -> None:
        with open_fs(self.directory, writeable=True) as root:
            path_dirname = os.path.dirname(path)
            if path_dirname:
                root.makedirs(path_dirname)
            root.writetext(path, contents)

    def delete(self, path: str) -> None:
        with open_fs(self.directory, writeable=True) as root:
            if root.exists(path):
                root.remove(path)
