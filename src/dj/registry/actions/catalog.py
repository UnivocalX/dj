import os
from logging import Logger, getLogger

from dj.registry.actions.actor import RegistryActor
from dj.registry.models import DatasetRecord, FileRecord, TagRecord
from dj.schemes import ExportDataConfig, FetchDataConfig, SearchDataConfig
from dj.utils import export_data, pretty_bar, pretty_format

logger: Logger = getLogger(__name__)


class DataCatalog(RegistryActor):
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
        self, directory: str, filename: str, mime: str | None = None
    ) -> str:
        if mime:
            directory = os.path.join(directory, mime)

        local_filepath: str = os.path.join(directory, filename)
        return local_filepath

    def _download_records(
        self,
        file_records: list[FileRecord],
        directory: str,
        overwrite: bool,
        flat: bool = False,
    ) -> None:
        logger.info("Downloading files")
        unique_records: list[FileRecord] = self._unique_records(file_records)

        for file_record in pretty_bar(
            unique_records, disable=self.cfg.plain, desc="⬇️   Downloading", unit="file"
        ):
            local_filepath: str = self._get_local_filepath(
                directory,
                os.path.basename(file_record.s3uri),
                file_record.mime_type if not flat else None,  # type: ignore[arg-type]
            )
            self.storage.download_obj(
                file_record.s3uri,  # type: ignore[arg-type]
                local_filepath,
                overwrite=overwrite,
            )

    def search(self, cfg: SearchDataConfig) -> list[FileRecord]:
        logger.info("Searching for files in catalog.")
        logger.info(
            pretty_format(
                title="🔍 Filters",
                data=cfg.model_dump(),
            )
        )

        query = self.journalist.session.query(FileRecord).join(DatasetRecord)

        logger.debug(f"filtering by domain: {cfg.domain}")
        query = query.filter(DatasetRecord.domain == cfg.domain)

        logger.debug(f"filtering by stage: {cfg.stage}")
        query = query.filter(FileRecord.stage == cfg.stage)

        if cfg.dataset_name:
            logger.debug(f"filtering by dataset: {cfg.dataset_name}")
            query = query.filter(DatasetRecord.name == cfg.dataset_name)

        if cfg.mime:
            logger.debug(f"filtering by mime: {cfg.mime}")
            query = query.filter(FileRecord.mime_type.like(f"%{cfg.mime}%"))

        if cfg.sha256:
            logger.debug(f"filtering by sha256: {', '.join(cfg.sha256)}")
            query = query.filter(FileRecord.sha256.in_(cfg.sha256))

        if cfg.filenames:
            logger.debug(f"filtering by file names: {', '.join(cfg.filenames)}")
            query = query.filter(FileRecord.filename.in_(cfg.filenames))

        if cfg.tags:
            logger.debug(f"filtering by tags: {', '.join(cfg.tags)}")
            query = query.join(FileRecord.tags).filter(TagRecord.name.in_(cfg.tags))

        file_records: list[FileRecord] = query.limit(cfg.limit).all()
        logger.info(f"Found {len(file_records)} files matching the criteria.")
        return file_records

    def fetch(self, cfg: FetchDataConfig) -> None:
        logger.info("Fetching files from catalog.")
        file_records: list[FileRecord] = self.search(
            SearchDataConfig(**cfg.model_dump())
        )

        if file_records and not cfg.dry:
            self._download_records(file_records, cfg.directory, cfg.overwrite, cfg.flat)

    def export(self, cfg: ExportDataConfig) -> list[dict]:
        logger.info(f"Exporting file records -> {cfg.filepath}")
        file_records: list[FileRecord] = self.search(
            SearchDataConfig(**cfg.model_dump())
        )

        records: list = []
        for record in file_records:
            record_dict: dict = self.journalist.file_record2dict(record)
            records.append(record_dict)

        export_data(cfg.filepath, records)

        return records
