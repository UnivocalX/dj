import os
import tempfile
from contextlib import contextmanager
from logging import Logger, getLogger

from sqlalchemy.exc import IntegrityError

from dj.inspect import FileInspector
from dj.registry.journalist import Journalist
from dj.registry.models import DatasetRecord, FileRecord
from dj.schemes import FileMetadata, LoadDataConfig
from dj.storage import Storage
from dj.utils import (
    collect_files,
    merge_s3uri,
    pretty_bar,
)

logger: Logger = getLogger(__name__)


class DataLoader:
    def __init__(self, cfg: LoadDataConfig):
        self.cfg: LoadDataConfig = cfg
        self.storage: Storage = Storage(cfg)
        self.journalist: Journalist = Journalist(cfg)

        if not cfg.s3bucket:
            raise ValueError("Please configure S3 bucket!")

    def __enter__(self):
        logger.debug("Entering DataLoader context manager")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        logger.debug("Exiting DataLoader context manager")
        if exc_type:
            logger.error(
                f"Exception in context manager: {exc_type.__name__}: {exc_val}"
            )
        self.journalist.close()
        return None

    def _gather_datafiles(self) -> set[str]:
        datafiles: set[str] = set()

        logger.info(f"attempting to gather data, filters: {self.cfg.filters}")
        if self.cfg.data_src.startswith("s3://"):
            logger.info("gathering data from S3")

            s3objcets: list[str] = self.storage.list_objects(
                self.cfg.data_src,
                self.cfg.filters,
            )

            for s3obj in s3objcets:
                datafiles.add(merge_s3uri(self.cfg.data_src, s3obj))
        else:
            logger.info("gathering data from local storage")
            datafiles = collect_files(self.cfg.data_src, self.cfg.filters)

        logger.info(f'Gathered {len(datafiles)} file\\s from "{self.cfg.data_src}"')
        return datafiles

    def _load_datafile(self, dataset: DatasetRecord, datafile_src: str) -> FileRecord:
        with self._get_local_file(datafile_src) as local_path:
            # Inspect File Metadata
            metadata: FileMetadata = FileInspector(local_path).metadata

            # Create a data file record
            try:
                with self.journalist.transaction():
                    datafile_record: FileRecord = self.journalist.add_file_record(
                        dataset=dataset,
                        s3bucket=self.cfg.s3bucket,  # type: ignore[arg-type]
                        s3prefix=self.cfg.s3prefix,
                        filename=metadata.filename,
                        sha256=metadata.sha256,
                        mime_type=metadata.mime_type,
                        size_bytes=metadata.size_bytes,
                        stage=self.cfg.stage,
                        tags=self.cfg.tags,
                    )
            except IntegrityError as e:
                if "files.s3uri" in str(e.orig):
                    raise FileExistsError(
                        f"File {metadata.filename} already exists in {self.cfg.domain}\\{self.cfg.dataset_name}"
                    ) from e
                raise

            # self.storage.upload(metadata.filepath, datafile_record.s3uri)
            return datafile_record

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

    def load(self) -> None:
        # Gather all data files
        datafiles: set[str] = self._gather_datafiles()
        if not datafiles:
            raise ValueError(f"Failed to gather data files from {self.cfg.data_src}")

        # Create\Get a dataset record
        dataset_record: DatasetRecord = self.journalist.add_dataset(
            self.cfg.domain,
            self.cfg.dataset_name,
            self.cfg.description,
            self.cfg.exists_ok,
        )

        # Load files
        logger.info(f"Starting to process {len(datafiles)} file\\s")
        processed_datafiles: dict[str, FileRecord] = {}
        for datafile in pretty_bar(
            datafiles, disable=self.cfg.plain, desc="☁️ Loading", unit="file"
        ):
            try:
                datafile_record: FileRecord = self._load_datafile(
                    dataset_record, datafile
                )
            except Exception as e:
                logger.error(e)
                logger.error(f"Failed to load {datafile}.\n")
            else:
                processed_datafiles[datafile] = datafile_record

        if not processed_datafiles:
            raise ValueError(f"Failed to load datafiles ({self.cfg.data_src}).")

        logger.info(f"Successfully loaded: {len(processed_datafiles)} file\\s.")
