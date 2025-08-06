import os
from logging import Logger, getLogger

from dj.commands.action import DataAction
from dj.registry.models import DatasetRecord, FileRecord, TagRecord
from dj.schemes import FetchDataConfig
from dj.utils import export_data, pretty_bar, pretty_format

logger: Logger = getLogger(__name__)


class DataFetcher(DataAction):
    def _download_records(self, file_records: list[FileRecord], directory: str) -> None:
        logger.info("Downloading files")

        success: bool = False
        for file_record in pretty_bar(
            file_records, disable=self.cfg.plain, desc="⬇️   Downloading", unit="file"
        ):
            local_filepath: str = os.path.join(
                directory, os.path.basename(file_record.s3uri)
            )
            logger.info(f"{file_record.s3uri} -> {local_filepath}")
            try:
                self.storage.download_obj(
                    file_record.s3uri,
                    local_filepath,
                )
            except Exception as e:
                logger.error(e)
                logger.error(f"Failed to load {file_record.s3uri}.\n")
            else:
                success = True

        if not success:
            raise ValueError("Failed to download files (0 files were downloaded)")

    def _export_records(self, file_records: list[FileRecord], filepath: str) -> None:
        logger.info(f"exporting file records -> {filepath}")

        records_dict: dict = {}
        for record in file_records:
            record_dict: dict = self.journalist.file_record2dict(record)
            records_dict[record_dict["sha256"]] = record_dict

        export_data(filepath, records_dict)

    def fetch(self, fetch_cfg: FetchDataConfig, delay: int | None = None) -> None:
        logger.info("Starting to Fetch data.")
        logger.info(
            pretty_format(
                title="🔍 Filters",
                data=fetch_cfg.model_dump(
                    exclude=["export_format", "export", "dry", "fetch_export_filepath"]
                ),
            )
        )

        # Since domain is always present, we always need to join DatasetRecord
        query = self.journalist.session.query(FileRecord).join(DatasetRecord)

        # Apply domain filter (always present)
        logger.info(f"filtering by domain: {fetch_cfg.domain}")
        query = query.filter(DatasetRecord.domain == fetch_cfg.domain)

        # Apply stage filter (always present)
        logger.info(f"filtering by stage: {fetch_cfg.stage}")
        query = query.filter(FileRecord.stage == fetch_cfg.stage)

        # Apply optional filters
        if fetch_cfg.dataset_name:
            logger.info(f"filtering by dataset: {fetch_cfg.dataset_name}")
            query = query.filter(DatasetRecord.name == fetch_cfg.dataset_name)

        if fetch_cfg.mime:
            logger.info(f"filtering by mime: {fetch_cfg.mime}")
            query = query.filter(FileRecord.mime_type.like(f"%{fetch_cfg.mime}%"))

        if fetch_cfg.tags:
            logger.info(f"filtering by tags: {', '.join(fetch_cfg.tags)}")
            # Filter files that have ANY of the specified tags
            query = query.join(FileRecord.tags).filter(
                TagRecord.name.in_(fetch_cfg.tags)
            )

        # Apply limit and execute query
        file_records: list[FileRecord] = query.limit(fetch_cfg.limit).all()
        logger.info(f"Found {len(file_records)} files matching filters")

        if file_records:
            if fetch_cfg.export_format:
                os.makedirs(fetch_cfg.directory, exist_ok=True)
                self._export_records(file_records, fetch_cfg.fetch_export_filepath)
            if not fetch_cfg.dry:
                os.makedirs(fetch_cfg, exist_ok=True)
                self._download_records(file_records, fetch_cfg.directory)