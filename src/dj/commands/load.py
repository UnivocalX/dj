from logging import Logger, getLogger

from sqlalchemy.exc import IntegrityError

from dj.commands.action import DataAction
from dj.inspect import FileInspector
from dj.registry.models import DatasetRecord, FileRecord
from dj.schemes import FileMetadata, LoadDataConfig
from dj.utils import (
    collect_files,
    merge_s3uri,
    pretty_bar,
)

logger: Logger = getLogger(__name__)


class DataLoader(DataAction):
    def _gather_datafiles(self, data_src: str, filters: list[str] | None) -> set[str]:
        datafiles: set[str] = set()

        logger.info(f"attempting to gather data, filters: {filters}")
        if data_src.startswith("s3://"):
            logger.info("gathering data from S3")

            s3objcets: list[str] = self.storage.list_objects(
                data_src,
                filters,
            )

            for s3obj in s3objcets:
                datafiles.add(merge_s3uri(data_src, s3obj))
        else:
            logger.info("gathering data from local storage")
            datafiles = collect_files(data_src, filters)

        logger.info(f'Gathered {len(datafiles)} file\\s')
        return datafiles

    def _load_datafile(
        self, load_cfg: LoadDataConfig, dataset: DatasetRecord, datafile_src: str
    ) -> FileRecord:
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
                        stage=load_cfg.stage,
                        tags=load_cfg.tags,
                    )
            except IntegrityError as e:
                if "files.s3uri" in str(e.orig):
                    raise FileExistsError(
                        f"File {metadata.filename} already exists in {load_cfg.domain}\\{load_cfg.dataset_name}"
                    ) from e
                raise

            # self.storage.upload(metadata.filepath, datafile_record.s3uri)
            return datafile_record

    def load(self, load_cfg: LoadDataConfig) -> None:
        logger.info(f'Starting to load data from "{load_cfg.data_src}"')
        
        datafiles: set[str] = self._gather_datafiles(load_cfg.data_src, load_cfg.filters)
        if not datafiles:
            raise ValueError(f"Failed to gather data files from {load_cfg.data_src}")

        # Create\Get a dataset record
        dataset_record: DatasetRecord = self.journalist.add_dataset(
            load_cfg.domain,
            load_cfg.dataset_name,
            load_cfg.description,
            load_cfg.exists_ok,
        )

        # Load files
        logger.info(f"Starting to process {len(datafiles)} file\\s")
        processed_datafiles: dict[str, FileRecord] = {}
        for datafile in pretty_bar(
            datafiles, disable=self.cfg.plain, desc="☁️   Loading", unit="file"
        ):
            try:
                datafile_record: FileRecord = self._load_datafile(
                    load_cfg, dataset_record, datafile
                )
            except Exception as e:
                logger.error(e)
                logger.error(f"Failed to load {datafile}.\n")
            else:
                processed_datafiles[datafile] = datafile_record

        if not processed_datafiles:
            raise ValueError(f"Failed to load datafiles ({load_cfg.data_src}).")
