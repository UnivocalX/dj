import json
from logging import Logger, getLogger

import yaml

from dj.actions.inspect import FileInspector
from dj.actions.registry.base import BaseAction
from dj.actions.registry.models import DatasetRecord
from dj.constants import DataStage
from dj.exceptions import FileRecordExist
from dj.schemes import CreateDatasetConfig, FileMetadata

logger: Logger = getLogger(__name__)


class DatasetCreator(BaseAction):
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

    def relate_data(self, dataset: DatasetRecord, data_cfg: list[dict]) -> None:
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
                logger.info(f"Added file '{cfg['filename']}' to dataset")

    def create(self, create_cfg: CreateDatasetConfig) -> None:
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
                self.relate_data(dataset_record, data_cfg)

        logger.info("Successfully created dataset.")
