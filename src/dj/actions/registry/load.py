from logging import Logger, getLogger

from dj.actions.inspect import FileInspector
from dj.actions.registry.base import BaseAction
from dj.actions.registry.models import DatasetRecord, FileRecord
from dj.exceptions import FailedToGatherFiles, FileRecordExist
from dj.schemes import FileMetadata, LoadDataConfig
from dj.utils import collect_files, delay, merge_s3uri, pretty_bar

logger: Logger = getLogger(__name__)


class DataLoader(BaseAction):
    def _gather_datafiles(
        self, paths: list[str], filters: list[str] | None
    ) -> set[str]:
        datafiles: set[str] = set()

        logger.info(f"Attempting to gather data, filters: {filters}")
        for path in paths:
            if path.startswith("s3://"):
                logger.info("gathering data from S3")
                s3objects: list[str] = self.storage.list_objects(
                    path,
                    filters,
                )

                for s3obj in s3objects:
                    datafiles.add(merge_s3uri(path, s3obj))
            else:
                logger.debug("gathering data from local storage")
                datafiles.update(collect_files(path, filters, recursive=True))

        logger.info(f"Gathered {len(datafiles)} file\\s")
        return datafiles

    def _load_datafile(
        self, load_cfg: LoadDataConfig, dataset: DatasetRecord, datafile_src: str
    ) -> FileRecord:
        with self._get_local_file(datafile_src) as local_path:
            metadata: FileMetadata = FileInspector(local_path).metadata

            with self.journalist.transaction():
                try:
                    datafile_record: FileRecord = self.journalist.create_file_record(
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
                except FileRecordExist as e:
                    datafile_record = self.journalist.get_file_record_by_sha256(
                        sha256=metadata.sha256,
                        stage=load_cfg.stage,
                        domain=dataset.domain,  # type: ignore[arg-type]
                        dataset_name=dataset.name,  # type: ignore[arg-type]
                        s3bucket=self.cfg.s3bucket, 
                        s3prefix=self.cfg.s3prefix
                    ).pop()  # type: ignore[arg-type]
                    logger.warning(e)
                else:
                    self._upload2storage(metadata.filepath, datafile_record.s3uri)

            return datafile_record
        
    def _upload2storage(self, filepath: str, s3uri: str) -> None:
        self.storage.upload(filepath, s3uri, False)  # type: ignore[arg-type]
        self._update_ref_count(s3uri)

    def load(self, load_cfg: LoadDataConfig) -> None:
        logger.info("Starting to load files.")
        datafiles: set[str] = self._gather_datafiles(load_cfg.paths, load_cfg.filters)
        if not datafiles:
            raise FailedToGatherFiles(
                f"Failed to gather data files from {load_cfg.paths}"
            )

        # Create\Get a dataset record
        dataset_record: DatasetRecord = self.journalist.create_dataset(
            load_cfg.domain,
            load_cfg.dataset_name,
            load_cfg.description,
            load_cfg.exists_ok,
        )

        # Load files
        logger.info(f'Starting to process "{len(datafiles)}" file\\s')
        delay()
        for datafile in pretty_bar(
            datafiles, disable=self.cfg.plain, desc="☁️   Loading", unit="file"
        ):
            self._load_datafile(load_cfg, dataset_record, datafile)
