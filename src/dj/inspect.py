import hashlib
from functools import cached_property
from pathlib import Path

import magic

from dj.schemes import FileMetadata


class FileInspector:
    def __init__(self, filepath: str):
        self.filepath: Path = Path(filepath)

    def calculate_sha256_hash(self) -> str:
        sha256 = hashlib.sha256()
        with open(self.filepath, "rb") as f:
            while chunk := f.read(8192):
                sha256.update(chunk)
        return sha256.hexdigest()

    def get_mime_type(self) -> str:
        if not self.filepath.is_file():
            raise FileNotFoundError(f"Path does not point to a file: {self.filepath}")

        try:
            mime = magic.Magic(mime=True)
            mime_type = mime.from_file(str(self.filepath))
            return mime_type or "application/octet-stream"
        except Exception:
            return "application/octet-stream"

    @cached_property
    def metadata(self) -> FileMetadata:
        if not self.filepath.is_file():
            raise FileNotFoundError(f"{self.filepath} is missing!")

        return FileMetadata(
            filepath=self.filepath.absolute(),
            size_bytes=self.filepath.stat().st_size,
            sha256=self.calculate_sha256_hash(),
            mime_type=self.get_mime_type(),
        )
