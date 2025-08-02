import os
from functools import cached_property
from pathlib import Path

from pydantic import BaseModel, Field, SecretStr, computed_field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from dj.constants import PROGRAM_NAME, DataStage
from dj.utils import clean_string, format_file_size


class CommonModelConfig(BaseSettings):
    model_config = SettingsConfigDict(
        str_strip_whitespace=True,
        populate_by_name=True,
        extra="ignore",
        env_prefix=PROGRAM_NAME,
    )


class DJCFG(CommonModelConfig):
    """DJ Config"""

    s3bucket: str | None = Field(default=None)
    s3prefix: str = Field(default=PROGRAM_NAME)
    log_dir: str | None = Field(default=None)
    verbose: bool = Field(default=False)
    colors: bool = Field(default=True)


class ConfigureDJCFG(CommonModelConfig):
    set_s3prefix: str | None = Field(default=None)
    set_s3bucket: str | None = Field(default=None)
    set_log_dir: str | None = Field(default=None)
    set_verbose: bool | None = Field(default=None)
    enable_colors: bool | None = Field(default=None)


class LoadDataCFG(CommonModelConfig):
    file_src: str
    dataset_id: str
    domain: str = Field(default="global")
    stage: DataStage = Field(default=DataStage.RAW)
    filters: list[str] | None = Field(default=None)
    overwrite: bool = Field(default=False)

    @field_validator("domain")
    def clean_strings(v: str) -> str:
        return clean_string(v)


class FileMetadata(BaseModel):
    filepath: Path
    size_bytes: int = Field(..., description="Exact size in bytes")
    sha256_hash: str = Field(..., description="Cryptographic hash")
    mime_type: str

    @computed_field
    @cached_property
    def size_human(self) -> str:
        return format_file_size(self.size_bytes)

    @computed_field
    @cached_property
    def filename(self) -> str:
        return os.path.basename(self.filepath)


class S3Credentials(BaseModel):
    access_key_id: SecretStr
    secret_access_key: SecretStr


class StorageCFG(BaseModel):
    access_keys: S3Credentials | None = None
    endpoint: str | None = Field(default=None)
