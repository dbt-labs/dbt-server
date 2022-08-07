import os
from dataclasses import dataclass
from xmlrpc.client import boolean

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
                root.makedirs(path_dirname, recreate=True)
            root.writetext(path, contents)

    def delete(self, path: str, force: boolean = False) -> None:
        """
        Deletes the node at a path.
        If a file, deletes the file.
        If a directory, deletes the directory and all of its contents, if force is true
        """
        with open_fs(self.directory, writeable=True) as root:
            if not root.exists(path):
                return None

            if root.isfile(path):
                root.remove(path)
                return None

            if root.isdir(path):
                if force:
                    root.removetree(path)
                    return None
                else:
                    raise Exception(
                        "Attempted to delete a directory, but force is set to false."
                    )

            return None
