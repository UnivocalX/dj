import json
from logging import Logger, getLogger

import yaml

from dj.constants import DataStage
from dj.exceptions import FailedToGatherFiles, FileRecordExist
from dj.inspect import FileInspector
from dj.registry.actions.actor import RegistryActor
from dj.registry.models import DatasetRecord, FileRecord
from dj.schemes import CreateDatasetConfig, FileMetadata, LoadDataConfig
from dj.utils import collect_files, delay, merge_s3uri, pretty_bar

logger: Logger = getLogger(__name__)


class DataLoader(RegistryActor):
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

            increment: bool = False
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
                    increment = True
                except FileRecordExist as e:
                    datafile_record = self.journalist.get_file_records_by_sha256(
                        sha256=metadata.sha256,
                        stage=load_cfg.stage,
                        domain=dataset.domain,  # type: ignore[arg-type]
                        dataset_name=dataset.name,  # type: ignore[arg-type]
                        s3bucket=self.cfg.s3bucket,
                        s3prefix=self.cfg.s3prefix,
                    ).pop()  # type: ignore[arg-type]
                    logger.warning(e)

            self._upload2storage(
                str(metadata.filepath),
                datafile_record.s3uri,
                increment,
            )  # type: ignore[arg-type]
            return datafile_record

    def _upload2storage(self, filepath: str, s3uri: str, increment: bool) -> None:
        exist: bool = self.storage.obj_exists(s3uri)
        if not exist:
            self.storage.upload(filepath, s3uri)  # type: ignore[arg-type]

        if not exist or increment:
            self._increment_ref_count(s3uri)

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

    @classmethod
    def read_config_file(self, filepath: str) -> list[dict]:
        logger.debug(f'Reading data relation config from file: "{str(filepath)}"')
        cfg_file_metadata: FileMetadata = FileInspector(filepath).metadata

        # Initialize as empty dict instead of list
        data_cfg: list[dict] = []

        if cfg_file_metadata.mime_type in [
            "application/x-yaml",
            "text/yaml",
            "text/x-yaml",
        ]:
            with open(filepath, "r") as f:
                data_cfg = yaml.safe_load(f) or []
        elif cfg_file_metadata.mime_type == "application/json":
            with open(filepath, "r") as f:
                data_cfg = json.load(f) or []
        else:
            raise ValueError(
                f"Unsupported config file type: {cfg_file_metadata.mime_type}"
            )

        return data_cfg

    def _relate_data(self, dataset: DatasetRecord, data_cfg: list[dict]) -> None:
        for cfg in data_cfg:
            logger.debug(f"Relating '{cfg['sha256']}' to dataset")
            tag_names: list[str] = [tag["name"] for tag in cfg.get("tags", [])]

            try:
                self.journalist.create_file_record(
                    dataset,
                    cfg["s3bucket"],
                    cfg["s3prefix"],
                    cfg["filename"],
                    cfg["sha256"],
                    cfg["mime_type"],
                    cfg["size_bytes"],
                    DataStage[cfg["stage"].upper()],
                    tag_names,
                )
            except FileRecordExist as e:
                logger.warning(f"Skipping duplicate file: {e}")
            else:
                s3uri: str = str(
                    self.journalist.get_file_records_by_sha256(
                        cfg["sha256"],
                        DataStage[cfg["stage"].upper()],
                        str(dataset.domain),
                        str(dataset.name),
                        cfg["s3bucket"],
                        cfg["s3prefix"],
                    )
                    .pop()
                    .s3uri
                )
                self._increment_ref_count(s3uri)

                filename: str = cfg["filename"]
                logger.info(f'Added file "{filename}" to dataset')

    def create_dataset(self, create_cfg: CreateDatasetConfig) -> None:
        formatted_dataset_name: str = f"{create_cfg.domain}/{create_cfg.name}"

        logger.info(f"Creating dataset '{formatted_dataset_name}'")
        dataset_record = self.journalist.create_dataset(
            domain=create_cfg.domain,
            name=create_cfg.name,
            description=create_cfg.description,
            exists_ok=create_cfg.exists_ok,
        )

        if create_cfg.config_filepaths:
            logger.debug(f"Relating data for dataset '{formatted_dataset_name}'")

        for config_filepath in create_cfg.config_filepaths or []:
            data_cfg: list[dict] = self.read_config_file(str(config_filepath))
            with self.journalist.transaction():
                self._relate_data(dataset_record, data_cfg)

        logger.info("Successfully created dataset.")
