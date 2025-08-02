import os
import tempfile
from logging import Logger, getLogger

from dj.inspect import FileInspector
from dj.schemes import DJCFG, FileMetadata, LoadDataCFG, StorageCFG
from dj.storage import DataStorage
from dj.utils import (
    collect_files,
    merge_s3uri,
    pretty_bar,
)

logger: Logger = getLogger(__name__)


class DataLoader:
    def __init__(
        self, dj_cfg: DJCFG, cfg: LoadDataCFG, storage_cfg: StorageCFG | None = None
    ):
        self.cfg: LoadDataCFG = cfg
        self.dj_cfg: DJCFG = dj_cfg
        self.storage: DataStorage = DataStorage(storage_cfg)

    def _gather_datafiles(self) -> set[str]:
        datafiles: set[str] = {}

        logger.info(f"attempting to load data, filters: {self.cfg.filters}")
        if self.cfg.file_src.startswith("s3://"):
            logger.info("gathering data from S3")

            s3objcets: list[str] = self.storage.list_objects(
                self.cfg.file_src,
                self.cfg.filters,
            )

            for s3obj in s3objcets:
                datafiles.add(merge_s3uri(self.cfg.file_src, s3obj))
        else:
            logger.info("gathering data from local storage")
            datafiles = collect_files(self.cfg.file_src, self.cfg.filters)

        logger.info(f'Gathered {len(datafiles)} file\\s from "{self.cfg.file_src}"')
        return datafiles

    def _inspect_datafile(self, datafile: str) -> FileMetadata:
        if datafile.startswith("s3://"):
            tmpfile: str = os.path.join(
                tempfile.gettempdir(), os.path.basename(datafile)
            )
            try:
                self.storage.download_obj(datafile, tmpfile)
                inspector = FileInspector(tmpfile)
            finally:
                if os.path.exists(tmpfile):
                    os.remove(tmpfile)
        else:
            inspector = FileInspector(datafile)

        return inspector.metadata

    def _inspect_datafiles(self, datafiles: set[str]) -> dict[str, FileMetadata]:
        logger.info(f"Starting to inspect {len(datafiles)} file\\s")

        files_metadata: dict[str, FileMetadata] = {}
        for datafile in pretty_bar(
            datafiles,
            disable=not self.dj_cfg.colors,
            desc="🔍 Inspecting",
        ):
            try:
                metadata: FileMetadata = self._inspect_datafile(datafile)
            except Exception as e:
                logger.debug(e, exc_info=True)
                logger.error(f"failed to inspect {datafile}")
            else:
                files_metadata[datafile] = metadata

        logger.info(f"Gathered metadata from {len(files_metadata)} file\\s")
        return files_metadata

    def _s3load(self, datafiles_metadata: dict[str, FileMetadata]) -> dict[str, str]:
        logger.info(f"starting to upload {len(datafiles_metadata)} datafiles.")

        uploaded: dict[str, str] = {}
        for data_src in pretty_bar(
            datafiles_metadata.keys(),
            disable=not self.dj_cfg.colors,
            desc="🔍 Uploading",
        ):
            data_s3uri: str = self.storage.resolve_data_s3uri(
                self.dj_cfg.s3bucket,
                self.dj_cfg.s3prefix,
                self.cfg.domain,
                self.cfg.dataset_id,
                self.cfg.stage,
                datafiles_metadata[data_src].mime_type,
                datafiles_metadata[data_src].filename,
            )
            try:
                self.storage.upload(datafiles_metadata[data_src].filepath, data_s3uri)
            except Exception as e:
                logger.debug(e, exc_info=True)
                logger.error(
                    f"failed to upload {datafiles_metadata[data_src].filepath}"
                )
            else:
                uploaded[data_src] = datafiles_metadata[data_s3uri]

        return uploaded

    def load(self) -> None:
        datafiles: set[str] = self._gather_datafiles()
        if not datafiles:
            raise ValueError(f"Failed to aggregate data files from {self.cfg.file_src}")

        datafiles_metadata: dict[str, FileMetadata] = self._inspect_datafiles(datafiles)
        if not datafiles_metadata:
            raise ValueError(f"Failed to inspect data files ({self.cfg.file_src}).")

        loaded_s3data_mapping: dict[str, str] = self._s3load(datafiles_metadata)
        if not loaded_s3data_mapping:
            raise ValueError(f"Failed to load data ({self.cfg.file_src}) into s3.")

        logger.info(f"Successfully loaded: {len(loaded_s3data_mapping)} file\\s.")
