import os
import tempfile
from contextlib import contextmanager
from logging import Logger, getLogger
from typing import Any

from dj.registry.journalist import Journalist
from dj.registry.storage import Storage
from dj.schemes import RegistryConfig

logger: Logger = getLogger(__name__)


class RegistryActor:
    def __init__(self, cfg: RegistryConfig):
        self.cfg: RegistryConfig = cfg
        self.storage: Storage = Storage(cfg)
        self.journalist: Journalist = Journalist(cfg)

        if not cfg.s3bucket:
            raise ValueError("Please configure S3 bucket!")

    def __enter__(self):
        logger.debug("Entering DataAction context manager")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        logger.debug("Exiting DataAction context manager")
        self.journalist.close()
        return None

    @contextmanager
    def _get_local_file(self, datafile_src: str):
        if datafile_src.startswith("s3://"):
            tmpfile: str = os.path.join(
                tempfile.gettempdir(), os.path.basename(datafile_src)
            )
            self.storage.download_obj(datafile_src, tmpfile)
            try:
                yield tmpfile
            finally:
                if os.path.exists(tmpfile):
                    os.remove(tmpfile)
        else:
            yield datafile_src

    def _increment_ref_count(self, s3uri: str) -> None:
        tags: dict[str, Any] = self.storage.get_obj_tags(s3uri)
        new_ref_count: int = int(tags.get("ref_count", 0)) + 1
        tags["ref_count"] = new_ref_count

        logger.debug(f'updating "{s3uri}" ref count -> {new_ref_count}')
        self.storage.put_obj_tags(s3uri, tags)

    def _decrement_ref_count(self, s3uri: str) -> None:
        tags: dict[str, Any] = self.storage.get_obj_tags(s3uri)
        new_ref_count: int = int(tags.get("ref_count", 1)) - 1
        tags["ref_count"] = new_ref_count

        logger.debug(f'updating "{s3uri}" ref count -> {new_ref_count}')
        self.storage.put_obj_tags(s3uri, tags)
