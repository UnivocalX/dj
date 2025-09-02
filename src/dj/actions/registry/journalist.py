import os
from contextlib import contextmanager
from logging import Logger, getLogger
from typing import TypeVar

from sqlalchemy import Engine, create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from dj.actions.registry.models import Base, DatasetRecord, FileRecord, TagRecord
from dj.constants import DataStage
from dj.exceptions import DatasetExist, FileRecordExist, FileRecordNotFound, TagNotFound
from dj.schemes import Dataset, RegistryConfig
from dj.utils import resolve_data_s3uri

T = TypeVar("T")
logger: Logger = getLogger(__name__)


class Journalist:
    def __init__(self, cfg: RegistryConfig):
        self.cfg: RegistryConfig = cfg
        logger.debug(
            f"Initializing Journalist with registry endpoint: {self.cfg.registry_endpoint}"
        )

        self.engine: Engine = self._create_engine()
        self.Session = sessionmaker(bind=self.engine)
        self.session: Session = self.Session()

        logger.debug("Creating database tables...")
        Base.metadata.create_all(self.engine)

        if str(self.cfg.registry_endpoint).startswith("sqlite"):
            db_path = str(self.cfg.registry_endpoint).replace("sqlite:///", "")
            if os.path.exists(db_path):
                logger.debug(
                    f"SQLite database file created successfully at: {os.path.abspath(db_path)}"
                )
            else:
                logger.warning(
                    f"SQLite database file not found at expected location: {os.path.abspath(db_path)}"
                )

        logger.debug("Journalist initialization completed")

    def __enter__(self):
        logger.debug("Entering Journalist context manager")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        logger.debug("Exiting Journalist context manager")
        if exc_type:
            logger.error(
                f"Exception in context manager: {exc_type.__name__}: {exc_val}"
            )
        self.close()

    def _create_engine(self) -> Engine:
        logger.debug(f"Creating database engine for: {self.cfg.registry_endpoint}")

        kwargs: dict = {
            "echo": self.cfg.echo,
            "pool_size": self.cfg.pool_size,
            "max_overflow": self.cfg.max_overflow,
        }

        # SQLite specific configuration
        if str(self.cfg.registry_endpoint).startswith("sqlite"):
            logger.debug("Configuring SQLite-specific engine settings")
            kwargs.update(
                {
                    "connect_args": {"check_same_thread": False},
                    "poolclass": None,  # SQLite doesn't need connection pooling
                }
            )

        # PostgreSQL specific configuration
        elif str(self.cfg.registry_endpoint).startswith(("postgresql", "postgres")):
            logger.debug("Configuring PostgreSQL-specific engine settings")
            kwargs.update(
                {
                    "pool_pre_ping": True,  # Test connections for liveness
                    "pool_recycle": 3600,  # Recycle connections after 1 hour
                }
            )

        logger.debug(f"Engine configuration: {kwargs}")
        engine = create_engine(str(self.cfg.registry_endpoint), **kwargs)
        logger.debug("Database engine created successfully")
        return engine

    def close(self) -> None:
        logger.debug("Closing database session")
        self.session.close()

    @contextmanager
    def transaction(self):
        logger.debug("Starting database transaction")
        try:
            yield self  # Provide access to the journalist instance
            self.session.commit()  # Only commit if no exceptions occurred
            logger.debug("Transaction committed successfully")
        except Exception:
            logger.debug("Transaction failed, rolling back")
            self.session.rollback()
            raise

    # Dataset methods
    def get_dataset(
        self,
        domain: str,
        name: str,
    ) -> DatasetRecord | None:
        return (
            self.session.query(DatasetRecord)
            .filter(DatasetRecord.name == name, DatasetRecord.domain == domain)
            .first()
        )

    def create_dataset(
        self,
        domain: str,
        name: str,
        description: str | None = None,
        exists_ok: bool = True,
    ) -> DatasetRecord:
        logger.debug(f"Creating new dataset: {name}")
        dataset: DatasetRecord = DatasetRecord(
            domain=domain, name=name, description=description
        )
        self.session.add(dataset)

        try:
            self.session.commit()
            logger.debug(f"Successfully created dataset '{name}' with ID: {dataset.id}")
        except IntegrityError as e:
            logger.debug(f"Dataset '{name}' already exists", exc_info=e)
            logger.debug(f"exists_ok={exists_ok}")
            logger.debug("rolling back")
            self.session.rollback()

            if (
                "unique_dataset"
                or "UNIQUE constraint failed: datasets.name, datasets.domain" in str(e)
            ):
                if not exists_ok:
                    raise DatasetExist(f"Dataset '{name}' already exists.")

                existing_dataset = self.get_dataset(domain, name)
                assert existing_dataset is not None, f"Dataset '{name}' should exist"
                dataset = existing_dataset
                logger.debug(f"Using existing dataset '{name}' with ID: {dataset.id}")
            else:
                raise

        return dataset

    def list_datasets(
        self,
        domain: str,
        name_pattern: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[Dataset]:
        query = self.session.query(DatasetRecord)

        # Apply filters
        query = query.filter(DatasetRecord.domain == domain)

        if name_pattern is not None:
            logger.debug(f"Filtering datasets by name pattern: {name_pattern}")
            query = query.filter(DatasetRecord.name.contains(name_pattern))

        # Apply pagination
        if offset is not None:
            logger.debug(f"Applying offset: {offset}")
            query = query.offset(offset)
        if limit is not None:
            logger.debug(f"Applying limit: {limit}")
            query = query.limit(limit)

        datasets: list[DatasetRecord] = query.all()

        logger.debug(f"Found {len(datasets)} dataset(s) matching filters")
        result: list[Dataset] = []
        for dataset in datasets:
            file_count = (
                self.session.query(FileRecord)
                .filter(FileRecord.dataset_id == dataset.id)
                .count()
            )

            logger.debug(f"Dataset {dataset.name} has {file_count} files")
            result.append(
                Dataset(
                    id=dataset.id,  # type: ignore[arg-type]
                    name=dataset.name,  # type: ignore[arg-type]
                    domain=dataset.domain,  # type: ignore[arg-type]
                    created_at=dataset.created_at,  # type: ignore[arg-type]
                    description=dataset.description,  # type: ignore[arg-type]
                    total_files=file_count,  # type: ignore[arg-type]
                )
            )

        return result

    # File methods
    def get_file_record_by_id(self, file_id: int) -> FileRecord:
        logger.debug(f"Getting file record by ID: {file_id}")
        file_record: FileRecord | None = (
            self.session.query(FileRecord).filter(FileRecord.id == file_id).first()
        )

        if not file_record:
            raise FileRecordNotFound(f"File record not found for file_id: {file_id}")

        return file_record

    def get_file_record_by_sha256(
        self,
        sha256: str,
        stage: DataStage,
        domain: str | None = None,
        dataset_name: str | None = None,
        s3bucket: str | None = None,
        s3prefix: str | None = None,
    ) -> list[FileRecord]:
        logger.debug(
            f"Searching by sha256: {sha256} in stage {stage}"
            + (f" ({domain}/{dataset_name})" if dataset_name and domain else "")
        )

        query = (
            self.session.query(FileRecord)
            .join(FileRecord.dataset)
            .filter(FileRecord.sha256 == sha256, FileRecord.stage == stage)
        )

        if domain:
            query = query.filter(DatasetRecord.domain == domain)

        if dataset_name:
            query = query.filter(DatasetRecord.name == dataset_name)

        if s3bucket:
            query = query.filter(FileRecord.s3bucket == s3bucket)

        if s3prefix:
            query = query.filter(FileRecord.s3prefix == s3prefix)

        return query.all()

    def create_file_record(
        self,
        dataset: DatasetRecord,
        s3bucket: str,
        s3prefix: str,
        filename: str,
        sha256: str,
        mime_type: str,
        size_bytes: int,
        stage: DataStage = DataStage.RAW,
        tags: list[str] | None = None,
    ) -> FileRecord:
        formatted_dataset_name: str = f"{dataset.name}/{dataset.domain}"
        logger.debug(f"Adding file record {filename} to {formatted_dataset_name}")

        tags_records: list[TagRecord] = []
        if tags:
            for tag_name in tags:
                tags_records.append(self.add_tag(tag_name.strip(), commit=False))

        s3uri: str = resolve_data_s3uri(
            s3bucket=s3bucket,
            s3prefix=s3prefix,
            stage=stage.value,
            mime_type=mime_type,
            sha256=sha256,
            ext=os.path.splitext(filename)[1],
        )

        datafile: FileRecord = FileRecord(
            dataset_id=dataset.id,
            s3uri=s3uri,
            s3bucket=s3bucket,
            s3prefix=s3prefix,
            stage=stage,
            filename=filename,
            sha256=sha256,
            mime_type=mime_type,
            size_bytes=size_bytes,
        )
        datafile.dataset = dataset

        if tags_records:
            datafile.tags = tags_records

        self.session.add(datafile)
        try:
            self.session.flush()  # Try to write to DB, but don't commit yet
        except IntegrityError as e:
            self.session.rollback()
            if (
                "unique_data_file"
                or "UNIQUE constraint failed: files.dataset_id, files.s3bucket, files.s3prefix, files.stage, files.sha256"
                in str(e)
            ):
                logger.debug(
                    f"File record already exists in {formatted_dataset_name}",
                    exc_info=e,
                )
                raise FileRecordExist(
                    f'File record "{filename}" ({sha256[:10]}...) already exists.'
                ) from e
            raise

        return datafile

    def _normalize_tag_name(self, tag_name: str) -> str:
        return tag_name.lower().strip()

    def get_tag(self, tag_name: str) -> TagRecord | None:
        normalized_name: str = self._normalize_tag_name(tag_name)
        logger.debug(f"Getting tag: {normalized_name}")
        return (
            self.session.query(TagRecord)
            .filter(TagRecord.name == normalized_name)
            .first()
        )

    def create_tag(self, tag_name: str, commit: bool = True) -> TagRecord:
        normalized_name: str = self._normalize_tag_name(tag_name)

        logger.debug(f"Creating new tag: {normalized_name}")
        tag: TagRecord = TagRecord(name=normalized_name)
        self.session.add(tag)
        if commit:
            logger.debug(f'Committing tag "{tag.name}"')
            self.session.commit()
        return tag

    def add_tag(self, tag_name: str, commit: bool = True) -> TagRecord:
        logger.debug(f"Adding tag: {tag_name}")
        tag: TagRecord | None = self.get_tag(tag_name)

        if not tag:
            tag = self.create_tag(tag_name, commit)
        else:
            logger.debug(f"Found existing tag: {tag.name}")

        return tag

    def add_tags2file(self, file_id: int, tag_names: list[str]) -> FileRecord:
        logger.debug(f"Adding {len(tag_names)} tag\\s to file ID {file_id}")
        file_record: FileRecord = self.get_file_record_by_id(file_id)

        for tag_name in tag_names:
            tag: TagRecord = self.add_tag(tag_name, commit=False)
            if tag not in file_record.tags:
                file_record.tags.append(tag)
                logger.debug(f"Added tag '{tag.name}' to file {file_record.filename}")
            else:
                logger.debug(
                    f"Tag '{tag.name}' already exists on file {file_record.filename}"
                )
        self.session.commit()
        return file_record

    def remove_tags(self, file_id: int, tag_names: list[str]) -> FileRecord:
        logger.debug(f"Removing {len(tag_names)} tag(s) from file ID {file_id}")
        file_record: FileRecord = self.get_file_record_by_id(file_id)

        initial_tag_count: int = len(file_record.tags)
        tags_to_remove = [self.get_tag(tag_name) for tag_name in tag_names]
        tags_to_remove = [tag for tag in tags_to_remove if tag is not None]

        if not tags_to_remove:
            raise TagNotFound(
                "No valid tags provided for removal. Please check the tag names."
            )

        logger.debug(
            f"Removing {len(tags_to_remove)} tag(s) from file {file_record.filename}"
        )
        for tag in tags_to_remove:
            if tag:
                if tag in file_record.tags:
                    file_record.tags.remove(tag)
                    logger.debug(f"Removed tag '{tag.name}'")
                else:
                    logger.debug(f"Tag '{tag.name}' not present on file")

        self.session.commit()
        logger.debug(f"Removed {initial_tag_count - len(file_record.tags)} tag(s)")
        return file_record

    def file_record2dict(
        self,
        file_record: FileRecord,
        exclude_fields: list[str] | None = None,
        datetime_format: str = "%Y-%m-%dT%H:%M:%SZ",
    ) -> dict:
        if exclude_fields is None:
            exclude_fields = []

        # Base fields conversion
        data: dict = {
            "id": file_record.id,
            "s3bucket": file_record.s3bucket,
            "s3prefix": file_record.s3prefix,
            "stage": file_record.stage.value,
            "filename": file_record.filename,
            "sha256": file_record.sha256,
            "mime_type": file_record.mime_type,
            "size_bytes": file_record.size_bytes,
            "created_at": file_record.created_at.strftime(datetime_format),
            "s3uri": file_record.s3uri,
        }
        if "dataset" not in exclude_fields and file_record.dataset:
            data["dataset"] = {
                "id": file_record.dataset.id,
                "name": file_record.dataset.name,
                "domain": file_record.dataset.domain,
            }
        if "tags" not in exclude_fields and file_record.tags:
            data["tags"] = [
                {"id": tag.id, "name": tag.name}  # Basic tag representation
                for tag in file_record.tags
            ]

        for field in exclude_fields:
            data.pop(field, None)

        return data
