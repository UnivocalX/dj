import os
from logging import Logger, getLogger

from dj.actions.registry.base import BaseAction
from dj.actions.registry.models import DatasetRecord, FileRecord, TagRecord
from dj.schemes import FetchDataConfig
from dj.utils import export_data, pretty_bar, pretty_format

logger: Logger = getLogger(__name__)


class DataFetcher(BaseAction):
    def _unique_records(self, file_records: list[FileRecord]) -> list[FileRecord]:
        unique_records: list[FileRecord] = []
        seen_sha256: set[str] = set()
        for record in file_records:
            if record.sha256 not in seen_sha256:
                seen_sha256.add(record.sha256)  # type: ignore[arg-type]
                unique_records.append(record)

        if len(unique_records) < len(file_records):
            duplicates_count = len(file_records) - len(unique_records)
            logger.warning(
                f"Filtered out {duplicates_count} duplicate files based on sha256"
            )

        return unique_records

    def _get_local_filepath(
        self, file_record: FileRecord, directory: str, flat: bool
    ) -> str:
        if flat:
            local_filepath: str = os.path.join(
                directory, os.path.basename(file_record.s3uri)
            )
        else:
            local_filepath = os.path.join(
                os.path.join(directory, file_record.mime_type),
                os.path.basename(file_record.s3uri),
            )

        return local_filepath

    def _download_records(
        self,
        file_records: list[FileRecord],
        directory: str,
        overwrite: bool,
        flat: bool = False,
    ) -> None:
        logger.info("Downloading files")

        # Ensure unique records before downloading
        unique_records: list[FileRecord] = self._unique_records(file_records)
        success: bool = False
        for file_record in pretty_bar(
            unique_records, disable=self.cfg.plain, desc="⬇️   Downloading", unit="file"
        ):
            local_filepath: str = self._get_local_filepath(file_record, directory, flat)
            try:
                self.storage.download_obj(
                    file_record.s3uri,  # type: ignore[arg-type]
                    local_filepath,
                    overwrite=overwrite,
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

    def _get_file_records(self, fetch_cfg: FetchDataConfig) -> list[FileRecord]:
        query = self.journalist.session.query(FileRecord).join(DatasetRecord)

        logger.debug(f"filtering by domain: {fetch_cfg.domain}")
        query = query.filter(DatasetRecord.domain == fetch_cfg.domain)

        logger.debug(f"filtering by stage: {fetch_cfg.stage}")
        query = query.filter(FileRecord.stage == fetch_cfg.stage)

        if fetch_cfg.dataset_name:
            logger.debug(f"filtering by dataset: {fetch_cfg.dataset_name}")
            query = query.filter(DatasetRecord.name == fetch_cfg.dataset_name)

        if fetch_cfg.mime:
            logger.debug(f"filtering by mime: {fetch_cfg.mime}")
            query = query.filter(FileRecord.mime_type.like(f"%{fetch_cfg.mime}%"))

        if fetch_cfg.sha256:
            logger.debug(f"filtering by sha256: {', '.join(fetch_cfg.sha256)}")
            query = query.filter(FileRecord.sha256.in_(fetch_cfg.sha256))

        if fetch_cfg.filenames:
            logger.debug(f"filtering by file names: {', '.join(fetch_cfg.filenames)}")
            query = query.filter(FileRecord.filename.in_(fetch_cfg.filenames))

        if fetch_cfg.tags:
            logger.debug(f"filtering by tags: {', '.join(fetch_cfg.tags)}")
            query = query.join(FileRecord.tags).filter(
                TagRecord.name.in_(fetch_cfg.tags)
            )

        return query.limit(fetch_cfg.limit).all()

    def fetch(self, fetch_cfg: FetchDataConfig) -> list[FileRecord]:
        logger.info("Starting to Fetch data.")
        logger.info(
            pretty_format(
                title="🔍 Filters",
                data=fetch_cfg.model_dump(
                    exclude=["export_format", "export", "dry", "fetch_export_filepath"]  # type: ignore[arg-type]
                ),
            )
        )

        file_records: list[FileRecord] = self._get_file_records(fetch_cfg)
        logger.info(f"Found {len(file_records)} files.")

        # Output results
        if file_records:
            if fetch_cfg.export:
                self._export_records(file_records, fetch_cfg.fetch_export_filepath)
            if not fetch_cfg.dry:
                self._download_records(
                    file_records,
                    fetch_cfg.directory,
                    fetch_cfg.overwrite,
                    fetch_cfg.flat,
                )

        return file_records
