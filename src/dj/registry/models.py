# mypy: ignore-errors

import os
from datetime import datetime

from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Table,
    Text,
    UniqueConstraint,
    event,
)
from sqlalchemy import Enum as SQLEnum
from sqlalchemy.orm import declarative_base, relationship

from dj.constants import DEFAULT_DOMAIN, DataStage
from dj.utils import resolve_data_s3uri

Base = declarative_base()

# Association table for many-to-many relationship between files and tags
file_tags: Table = Table(
    "file_tags",
    Base.metadata,
    Column("file_id", Integer, ForeignKey("files.id"), primary_key=True),
    Column("tag_id", Integer, ForeignKey("tags.id"), primary_key=True),
)


class DatasetRecord(Base):
    __tablename__ = "datasets"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    description = Column(Text)
    domain = Column(String, default=DEFAULT_DOMAIN, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    files = relationship(
        "FileRecord", back_populates="dataset", cascade="all, delete-orphan"
    )

    __table_args__ = (UniqueConstraint("name", "domain", name="unique_dataset"),)


class TagRecord(Base):
    __tablename__ = "tags"

    id = Column(Integer, primary_key=True)
    name = Column(String(50), nullable=False, unique=True)
    description = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    # Relationship to files
    files = relationship("FileRecord", secondary=file_tags, back_populates="tags")


class FileRecord(Base):
    __tablename__ = "files"

    id = Column(Integer, primary_key=True)
    dataset_id = Column(Integer, ForeignKey("datasets.id"), nullable=False)
    s3bucket = Column(String(63), nullable=False)  # Max S3 bucket length
    s3prefix = Column(String(1024), nullable=False, default="")
    stage = Column(SQLEnum(DataStage), default=DataStage.RAW, nullable=False)
    filename = Column(String, nullable=False)
    sha256 = Column(String(64), nullable=False)
    mime_type = Column(String(100), nullable=False)
    size_bytes = Column(BigInteger, nullable=False)
    s3uri = Column(String(2048), nullable=False, unique=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    # Relationships
    dataset = relationship("DatasetRecord", back_populates="files")
    tags = relationship("TagRecord", secondary=file_tags, back_populates="files")

    __table_args__ = (
        UniqueConstraint(
            "dataset_id",
            "s3bucket",
            "s3prefix",
            "stage",
            "sha256",
            name="unique_data_file",
        ),
    )


# Event listener to validate required components and compute s3uri
@event.listens_for(FileRecord, "before_insert")
@event.listens_for(FileRecord, "before_update")
def _validate_and_compute_s3uri(mapper, connection, target: FileRecord):
    """Ensure required fields are present and compute s3uri before saving."""

    # Compute and set the s3uri
    target.s3uri = resolve_data_s3uri(
        s3bucket=target.s3bucket,
        s3prefix=target.s3prefix,
        stage=target.stage.value,
        mime_type=target.mime_type,
        sha256=target.sha256,
        ext=os.path.splitext(target.filename)[1],
    )
