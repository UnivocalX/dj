import os
import tempfile
from contextlib import contextmanager
from logging import Logger, getLogger

from dj.registry.journalist import Journalist
from dj.schemes import DJConfig
from dj.storage import Storage

logger: Logger = getLogger(__name__)


class DataAction:
    def __init__(self, cfg: DJConfig):
        self.cfg: DJConfig = cfg
        self.storage: Storage = Storage(cfg)
        self.journalist: Journalist = Journalist(cfg)

        if not cfg.s3bucket:
            raise ValueError("Please configure S3 bucket!")

    def __enter__(self):
        logger.debug("Entering DataAction context manager")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        logger.debug("Exiting DataAction context manager")
        if exc_type:
            logger.error(
                f"Exception in context manager: {exc_type.__name__}: {exc_val}"
            )
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