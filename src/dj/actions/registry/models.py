# mypy: ignore-errors

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
)
from sqlalchemy import Enum as SQLEnum
from sqlalchemy.orm import declarative_base, relationship

from dj.constants import DEFAULT_DOMAIN, DataStage

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
    s3uri = Column(String(2048), nullable=False)
    s3bucket = Column(String(63), nullable=False)  # Max S3 bucket length
    s3prefix = Column(String(1024), nullable=False, default="")
    stage = Column(SQLEnum(DataStage), default=DataStage.RAW, nullable=False)
    filename = Column(String, nullable=False)
    sha256 = Column(String(64), nullable=False)
    mime_type = Column(String(100), nullable=False)
    size_bytes = Column(BigInteger, nullable=False)
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
