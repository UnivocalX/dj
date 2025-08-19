import posixpath
from logging import Logger, getLogger
from typing import Iterable

from boto3 import client
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from dj.schemes import StorageConfig
from dj.utils import split_s3uri

logger: Logger = getLogger(__name__)


class Storage:
    def __init__(self, cfg: StorageConfig | None = None):
        self.cfg: StorageConfig = cfg or StorageConfig()

    @property
    def client(self) -> client:
        client_config: BotoConfig = BotoConfig(
            retries={"max_attempts": 3, "mode": "standard"}
        )

        client_params: dict = {
            "config": client_config,
        }

        if self.cfg.s3endpoint:
            client_params["endpoint_url"] = self.cfg.s3endpoint

        return client("s3", **client_params)

    def obj_exists(self, s3uri: str) -> bool:
        s3bucket, s3key = split_s3uri(s3uri)
        try:
            self.client.head_object(Bucket=s3bucket, Key=s3key)
            logger.debug(f"{s3key} Exist.")
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            else:
                raise

    def prefix_exists(self, s3uri: str) -> bool:
        s3bucket, s3prefix = split_s3uri(s3uri)

        try:
            self.client.head_bucket(Bucket=s3bucket)
            return bool(
                self.client.list_objects_v2(
                    Bucket=s3bucket, Prefix=s3prefix, MaxKeys=1
                ).get("Contents")
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            raise

    def list_objects(
        self,
        s3uri: str,
        extensions: Iterable[str] | None = None,
    ) -> list[str]:
        s3bucket, s3prefix = split_s3uri(s3uri)
        s3prefix = s3prefix if s3prefix.endswith("/") else s3prefix + "/"

        formatted_extensions: str = "All" if not extensions else ", ".join(extensions)
        logger.info(f'starting to search for files in: "{s3prefix}"')
        logger.info(f"allowed extensions: {formatted_extensions}")

        page_iterator = self.client.get_paginator("list_objects_v2").paginate(
            Bucket=s3bucket, Prefix=s3prefix
        )

        found_objects: list[str] = []
        for page in page_iterator:
            if "Contents" not in page:
                logger.debug(f"no contents found in page for prefix: {s3prefix}")
                continue

            for obj in page["Contents"]:
                object_name: str = posixpath.basename(obj["Key"])
                if not extensions or object_name.lower().endswith(tuple(extensions)):
                    found_objects.append(object_name)

        logger.info(f"found {len(found_objects)} file\\s")
        return found_objects

    def copy_object(self, src_s3uri: str, dst_s3uri: str) -> None:
        src_s3bucket, src_s3key = split_s3uri(src_s3uri)
        dst_s3bucket, dst_s3key = split_s3uri(dst_s3uri)

        self.client.copy_object(
            CopySource={"Bucket": src_s3bucket, "Key": src_s3key},
            Bucket=dst_s3bucket,
            Key=dst_s3key,
        )

        logger.debug(f"copy completed successful {src_s3uri} -> {dst_s3uri}")

    def upload(self, filepath: str, dst_s3uri: str, overwrite: bool = True) -> None:
        if not overwrite and self.obj_exists(dst_s3uri):
            logger.debug(f"File {dst_s3uri} already exists, skipping upload.")
            return

        dst_s3bucket, dst_s3key = split_s3uri(dst_s3uri)
        self.client.upload_file(filepath, dst_s3bucket, dst_s3key)
        logger.debug(f"uploaded {filepath} -> {dst_s3uri}")

    def delete_obj(self, s3uri: str) -> None:
        s3bucket, s3key = split_s3uri(s3uri)
        self.client.delete_object(
            Bucket=s3bucket,
            Key=s3key,
        )
        logger.debug(f"deleted {s3uri}")

    def download_obj(self, s3uri: str, dst_path: str) -> None:
        s3bucket, s3key = split_s3uri(s3uri)
        with open(dst_path, "wb") as f:
            self.client.download_fileobj(s3bucket, s3key, f)
        logger.debug(f"downloaded {s3uri} -> {dst_path}")
