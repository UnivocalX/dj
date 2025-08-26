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
    def read_config_file(self, filepath: str) -> dict[dict]:
        logger.info(f'Reading data relation config from file: "{str(filepath)}"')
        config_file_metadata: FileMetadata = FileInspector(filepath).metadata
        data_config: list[dict] = []

        if config_file_metadata.mime_type in [
            "application/x-yaml",
            "text/yaml",
            "text/x-yaml",
        ]:
            with open(filepath, "r") as f:
                data_config = yaml.safe_load(f) or []
        elif config_file_metadata.mime_type == "application/json":
            with open(filepath, "r") as f:
                data_config = json.load(f) or []
        else:
            raise ValueError(
                f"Unsupported config file type: {config_file_metadata.mime_type}"
            )

        return data_config

    def relate_data(self, dataset: DatasetRecord, data_config: dict[dict]) -> list[int]:
        related_file_ids: list[int] = []

        for data_cfg in data_config.values():
            logger.debug(f"Relating '{data_cfg['sha256']}' to dataset")
            tag_names = [tag["name"] for tag in data_cfg.get("tags", [])]

            try:
                file_id: int = self.journalist.create_file_record(
                    dataset,
                    data_cfg["s3bucket"],
                    data_cfg["s3prefix"],
                    data_cfg["filename"],
                    data_cfg["sha256"],
                    data_cfg["mime_type"],
                    data_cfg["size_bytes"],
                    DataStage[data_cfg["stage"].upper()],
                    tag_names,
                ).id

                related_file_ids.append(file_id)
            except FileRecordExist as e:
                logger.warning(e)
                related_file_ids.append(file_id)

        return related_file_ids

    def create(self, create_cfg: CreateDatasetConfig) -> list[int]:
        logger.info(f"Creating dataset '{create_cfg.domain}/{create_cfg.name}'")

        with self.journalist.transaction():
            dataset_record = self.journalist.create_dataset(
                domain=create_cfg.domain,
                name=create_cfg.name,
                description=create_cfg.description,
                exists_ok=create_cfg.exists_ok,
            )

            if create_cfg.config_filepaths:
                formatted_dataset_name: str = f"{create_cfg.name}/{create_cfg.domain}"
                logger.info(f"Relating data for dataset '{formatted_dataset_name}'")

            for config_filepath in create_cfg.config_filepaths or []:
                data_config: dict[dict] = self.read_config_file(config_filepath)
                related_file_ids: list[int] = self.relate_data(
                    dataset_record, data_config
                )

        logger.info(f"Successfully created '{formatted_dataset_name}'")
        return related_file_ids
