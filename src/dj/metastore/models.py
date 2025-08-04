from datetime import datetime

from sqlalchemy import BigInteger, Column, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

class Dataset(Base):
    __tablename__ = "datasets"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    description = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)

    files = relationship("File", back_populates="dataset", cascade="all, delete-orphan")

class File(Base):
    __tablename__ = "files"
    id = Column(Integer, primary_key=True)
    dataset_id = Column(Integer, ForeignKey("datasets.id"), nullable=False)
    s3uri = Column(String, nullable=False)
    sha256_hash = Column(String)
    mime_type = Column(String)
    size_bytes = Column(BigInteger)
    created_at = Column(DateTime, default=datetime.utcnow)

    dataset = relationship("Dataset", back_populates="files")
    labels = relationship("Label", back_populates="file", cascade="all, delete-orphan")

class LabelClass(Base):
    __tablename__ = "label_classes"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    description = Column(Text)

    labels = relationship("Label", back_populates="label_class", cascade="all, delete-orphan")

class Label(Base): 
    __tablename__ = "labels"
    id = Column(Integer, primary_key=True)
    file_id = Column(Integer, ForeignKey("files.id"), nullable=False)
    label_class_id = Column(Integer, ForeignKey("label_classes.id"), nullable=True)
    value = Column(String)  # for freeform or extra info
    created_at = Column(DateTime, default=datetime.utcnow)

    file = relationship("File", back_populates="labels")
    label_class = relationship("LabelClass", back_populates="labels")