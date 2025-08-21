from logging import Logger, getLogger

from dj.actions.registry.journalist import Journalist
from dj.actions.registry.models import DatasetRecord, FileRecord
from dj.exceptions import DatasetNotFound, FileRecordNotFound
from dj.schemes import DJConfig, TagsConfig
from dj.utils import pretty_bar, pretty_format

logger: Logger = getLogger(__name__)


class DataTagger:
    def __init__(self, cfg: DJConfig):
        self.cfg: DJConfig = cfg
        self.journalist: Journalist = Journalist(cfg)

    def __enter__(self):
        logger.debug("Entering DataAction context manager")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        logger.debug("Exiting DataAction context manager")
        self.journalist.close()
        return None

    def _get_file_records(
        self,
        domain: str,
        dataset_name: str,
        stage: str,
        sha256: list[str] | None = None,
        filenames: list[str] | None = None,
    ) -> list[FileRecord]:
        query = self.journalist.session.query(FileRecord).join(DatasetRecord)

        logger.debug(f"filtering by domain: {domain}")
        query = query.filter(DatasetRecord.domain == domain)

        logger.debug(f"filtering by dataset: {dataset_name}")
        query = query.filter(DatasetRecord.name == dataset_name)

        logger.debug(f"filtering by stage: {stage}")
        query = query.filter(FileRecord.stage == stage)

        if sha256:
            logger.debug(f"filtering by sha256: {', '.join(sha256)}")
            query = query.filter(FileRecord.sha256.in_(sha256))

        if filenames:
            logger.debug(f"filtering by file names: {', '.join(filenames)}")
            query = query.filter(FileRecord.filename.in_(filenames))

        return query.all()

    def add(self, add_cfg: TagsConfig) -> list[FileRecord]:
        logger.info(pretty_format(add_cfg.model_dump(), title="Add Tags Config"))

        if not self.journalist.get_dataset(add_cfg.domain, add_cfg.dataset_name):
            raise DatasetNotFound(f"Dataset {add_cfg.dataset_name} not found")

        file_records: list[FileRecord] = self._get_file_records(
            add_cfg.domain,
            add_cfg.dataset_name,
            add_cfg.stage,
            add_cfg.sha256,
            add_cfg.filenames,
        )
        logger.info(f"Found {len(file_records)} files to add tags to.")
        if not file_records:
            raise FileRecordNotFound(
                f"No file records found for dataset {add_cfg.dataset_name} with the given filters."
            )

        for file_record in pretty_bar(
            file_records, disable=self.cfg.plain, desc="Adding tags"
        ):
            logger.debug(f"Adding tags to file {file_record.sha256}")
            self.journalist.add_tags2file(file_record.id, add_cfg.tags)

        logger.info(f"Successfully added tags {add_cfg.tags}")
        return file_records

    def remove(self, remove_cfg: TagsConfig) -> list[FileRecord]:
        logger.info(pretty_format(remove_cfg.model_dump(), title="Remove Tags Config"))

        if not self.journalist.get_dataset(remove_cfg.domain, remove_cfg.dataset_name):
            raise DatasetNotFound(f"Dataset {remove_cfg.dataset_name} not found")

        file_records: list[FileRecord] = self._get_file_records(
            remove_cfg.domain,
            remove_cfg.dataset_name,
            remove_cfg.stage,
            remove_cfg.sha256,
            remove_cfg.filenames,
        )
        logger.info(f"Found {len(file_records)} files to remove tags from.")
        if not file_records:
            raise FileRecordNotFound(
                f"No file records found for dataset {remove_cfg.dataset_name} with the given filters."
            )

        for file_record in pretty_bar(
            file_records, disable=self.cfg.plain, desc="Removing tags"
        ):
            logger.debug(f"Removing tags from file {file_record.sha256}")
            self.journalist.remove_tags(file_record.id, remove_cfg.tags)

        logger.info(f"Successfully removed tags {remove_cfg.tags}")
        return file_records
