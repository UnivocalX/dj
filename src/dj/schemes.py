import os
from functools import cached_property
from pathlib import Path
from urllib.parse import urlparse

from pydantic import (
    BaseModel,
    Field,
    SecretStr,
    computed_field,
    field_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict

from dj.constants import DEFAULT_DOMAIN, PROGRAM_NAME, DataStage
from dj.utils import clean_string, format_file_size, resolve_internal_dir


class BaseConfig(BaseSettings):
    """Base configuration with common settings."""

    model_config = SettingsConfigDict(
        str_strip_whitespace=True,
        populate_by_name=True,
        extra="ignore",
        env_prefix=PROGRAM_NAME,
    )


class DJConfigCLI(BaseConfig):
    command: str = Field(
        default="config",
        description="Command to execute (config, load, etc.)",
    )
    log_dir: str | None = Field(default=None)
    verbose: bool = Field(default=False)
    plain: bool = Field(default=False, description="Disable colors and loading bar")


class S3Credentials(BaseConfig):
    access_key_id: SecretStr | None = Field(
        default=None, description="AWS S3 Access Key ID"
    )
    secret_access_key: SecretStr | None = Field(
        default=None, description="AWS S3 Secret Access Key"
    )


class StorageConfig(S3Credentials):
    s3endpoint: str | None = Field(default=None)


class RegistryConfig(BaseConfig):
    registry_endpoint: str | None = Field(
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

    @field_validator("registry_endpoint")
    @classmethod
    def set_default_registry_url(cls, v: str | None) -> str:
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


class DJConfig(StorageConfig, RegistryConfig):
    s3bucket: str | None = Field(default=None)
    s3prefix: str = Field(default=PROGRAM_NAME)
    plain: bool = Field(default=False, description="disable loading bar")


class ConfigureDJConfig(BaseConfig):
    set_s3prefix: str | None = Field(default=None, description="Set S3 prefix")
    set_s3bucket: str | None = Field(default=None, description="Set S3 bucket")


class LoadDataConfig(DJConfig):
    data_src: str
    dataset_name: str
    description: str | None = Field(default=None)
    domain: str = Field(default=DEFAULT_DOMAIN)
    stage: DataStage = Field(default=DataStage.RAW)
    tags: list[str] | None = Field(default=None)
    filters: list[str] | None = Field(default=None)
    exists_ok: bool = Field(default=False)

    @field_validator("domain")
    def clean_strings(v: str) -> str:
        return clean_string(v)

    @field_validator("data_src")
    def abs_path(cls, v: str) -> str:
        if os.path.exists(v):
            return os.path.abspath(v)
        return v


class FileMetadata(BaseModel):
    filepath: Path
    size_bytes: int = Field(..., description="size in bytes")
    sha256: str = Field(..., description="Cryptographic hash")
    mime_type: str

    @computed_field  # type: ignore[prop-decorator]
    @cached_property
    def size_human(self) -> str:
        return format_file_size(self.size_bytes)

    @computed_field  # type: ignore[prop-decorator]
    @cached_property
    def filename(self) -> str:
        return os.path.basename(self.filepath)
