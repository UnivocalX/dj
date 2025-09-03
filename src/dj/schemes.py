import os
from datetime import datetime
from functools import cached_property
from pathlib import Path
from urllib.parse import urlparse

from pydantic import (
    BaseModel,
    Field,
    computed_field,
    field_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict

from dj.constants import (
    DEFAULT_DOMAIN,
    EXPORT_FORMATS,
    FETCH_FILENAME,
    PROGRAM_NAME,
    DataStage,
)
from dj.utils import clean_string, format_file_size, resolve_internal_dir


class BaseSettingsConfig(BaseSettings):
    model_config = SettingsConfigDict(
        str_strip_whitespace=True,
        populate_by_name=True,
        extra="ignore",
        env_prefix=PROGRAM_NAME,
    )


class Dataset(BaseModel):
    id: int
    name: str
    domain: str
    created_at: datetime
    description: str | None
    total_files: int


class DJConfigCLI(BaseSettingsConfig):
    command: str = Field(
        default="config",
        description="Command to execute (config, load, etc.)",
    )
    subcommand: str | None = Field(
        default=None,
        description="Subcommand for the main command (e.g., add, remove for tags)",
    )
    log_dir: str | None = Field(default=None)
    verbose: bool = Field(default=False)
    plain: bool = Field(default=False, description="Disable colors and loading bar")


class StorageConfig(BaseSettingsConfig):
    s3endpoint: str | None = Field(default=None)


class DatabaseConfig(BaseSettingsConfig):
    database_endpoint: str | None = Field(
        default=None,
        description="Database connection URL. If not provided, SQLite will be used.",
    )
    echo: bool = Field(
        default=False, description="If True, the Engine will log all statements"
    )
    pool_size: int = Field(
        default=5,
        description="The number of connections to keep open in the connection pool",
    )
    max_overflow: int = Field(
        default=10,
        description="The number of connections to allow in connection pool overflow",
    )

    @field_validator("database_endpoint")
    @classmethod
    def set_default_database_url(cls, v: str | None) -> str:
        if v is None:
            db_path: Path = Path(resolve_internal_dir()) / f"{PROGRAM_NAME}.db"
            return f"sqlite:///{db_path.absolute()}"

        parsed = urlparse(v)
        if parsed.scheme not in ("postgresql", "postgres", "sqlite"):
            raise ValueError("Only PostgreSQL or SQLite databases are supported")

        # Ensure SQLite URLs have the correct format
        if parsed.scheme == "sqlite":
            if not v.startswith("sqlite:///"):
                return f"sqlite:///{Path(parsed.path).absolute()}"
        return v


class RegistryConfig(StorageConfig, DatabaseConfig):
    s3bucket: str | None = Field(default=None)
    s3prefix: str = Field(default=PROGRAM_NAME)
    plain: bool = Field(default=False, description="disable loading bar")


class ConfigureRegistryConfig(BaseSettingsConfig):
    set_s3endpoint: str | None = Field(default=None, description="Set S3 endpoint URL")
    set_s3bucket: str | None = Field(default=None, description="Set S3 bucket")
    set_s3prefix: str | None = Field(default=None, description="Set S3 prefix")

    set_database_endpoint: str | None = Field(
        default=None, description="Set database endpoint URL"
    )
    set_echo: bool = Field(default=False, description="Enable SQL command echoing")
    set_pool_size: int | None = Field(
        default=None, description="Set database connection pool size"
    )
    set_max_overflow: int | None = Field(
        default=None, description="Set max overflow for database connections"
    )


class LoadDataConfig(BaseSettingsConfig):
    paths: list[str]
    dataset_name: str
    domain: str = Field(default=DEFAULT_DOMAIN)
    description: str | None = Field(default=None)
    stage: DataStage = Field(default=DataStage.RAW)
    tags: list[str] | None = Field(default=None)
    filters: list[str] | None = Field(default=None)
    exists_ok: bool = Field(default=False)

    @field_validator("domain", "dataset_name")
    def clean_strings(cls, string: str) -> str:
        return clean_string(string)

    @field_validator("tags")
    def clean_tags(cls, tags: list[str] | None) -> list[str] | None:
        if tags:
            tags = [clean_string(tag) for tag in tags]

        return tags

    @field_validator("paths")
    def abs_path(cls, paths: list[str]) -> list[str]:
        abs_paths: list[str] = []
        for path in paths:
            if os.path.exists(path):
                abs_paths.append(os.path.abspath(path))
            else:
                abs_paths.append(path)
        return abs_paths


class SearchDataConfig(BaseSettingsConfig):
    domain: str = Field(default=DEFAULT_DOMAIN)
    dataset_name: str | None = Field(default=None)
    stage: DataStage = Field(default=DataStage.RAW)
    mime: str | None = Field(default=None)
    tags: list[str] | None = Field(default=None)
    filenames: list[str] | None = Field(default=None)
    sha256: list[str] | None = Field(default=None)
    limit: int = Field(default=100)

    @field_validator("domain", "dataset_name")
    def clean_strings(cls, string: str | None) -> str | None:
        if string:
            string = clean_string(string)
        return string

    @field_validator("tags")
    def clean_tags(cls, tags: list[str] | None) -> list[str] | None:
        if tags:
            tags = [clean_string(tag) for tag in tags]

        return tags


class FetchDataConfig(SearchDataConfig):
    directory: str
    limit: int
    dry: bool = Field(default=False)
    overwrite: bool = Field(default=False, description="Overwrite existing files")
    flat: bool = Field(default=False, description="Store files in a flat structure")


class ExportDataConfig(SearchDataConfig):
    filepath: str = Field(default=FETCH_FILENAME)
    export_format: str | None = Field(default="json")

    @field_validator("export_format")
    def is_supported_format(cls, format: str) -> str:
        cleaned_format: str = clean_string(format)
        if cleaned_format not in EXPORT_FORMATS:
            raise ValueError(f"supported export formats: {', '.join(EXPORT_FORMATS)}")
        return cleaned_format


class ListDatasetsConfig(BaseSettingsConfig):
    domain: str = Field(default=DEFAULT_DOMAIN)
    name_pattern: str | None = Field(default=None)
    limit: int | None = Field(default=None)
    offset: int | None = Field(default=None)

    @field_validator("domain")
    def clean_strings(cls, v: str) -> str:
        return clean_string(v)


class TagConfig(BaseSettingsConfig):
    dataset_name: str
    domain: str = Field(default=DEFAULT_DOMAIN)
    tags: list[str] = Field(..., description="Tags names")
    stage: DataStage = Field(default=DataStage.RAW)
    sha256: list[str] | None = Field(
        default=None, description="SHA256 hashes to filter files by"
    )
    filenames: list[str] | None = Field(
        default=None, description="File names to filter files by"
    )

    @field_validator("domain", "dataset_name")
    def clean_strings(cls, string: str) -> str:
        return clean_string(string)

    @field_validator("tags")
    def clean_tags(cls, tags: list[str]) -> list[str]:
        return [clean_string(tag) for tag in tags]


class CreateDatasetConfig(BaseSettingsConfig):
    name: str
    domain: str = Field(default=DEFAULT_DOMAIN)
    description: str | None = Field(default=None)
    config_filepaths: list[Path] | None = Field(
        default=None, description="YAML/JSON file(s) with data relation configuration"
    )
    exists_ok: bool = Field(default=False)

    @field_validator("domain", "name")
    def clean_strings(cls, string: str) -> str:
        return clean_string(string)


class DeleteDatasetConfig(BaseSettingsConfig):
    name: str
    domain: str = Field(default=DEFAULT_DOMAIN)

    @field_validator("domain", "name")
    def clean_strings(cls, string: str) -> str:
        return clean_string(string)


class FileMetadata(BaseModel):
    filepath: Path
    size_bytes: int = Field(..., description="size in bytes")
    sha256: str = Field(..., description="Cryptographic hash")
    mime_type: str

    @computed_field  # type: ignore[misc]
    @cached_property
    def size_human(self) -> str:
        return format_file_size(self.size_bytes)

    @computed_field  # type: ignore[misc]
    @cached_property
    def filename(self) -> str:
        return os.path.basename(self.filepath)
