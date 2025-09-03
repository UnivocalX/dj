import os
import posixpath
from functools import wraps
from logging import Logger, getLogger
from typing import Any, Iterable

from boto3 import client
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from dj.exceptions import UnsuffiecentPermissions
from dj.schemes import StorageConfig
from dj.utils import split_s3uri

logger: Logger = getLogger(__name__)


class CustomS3Client:
    def __init__(self, client):
        self._client = client

    def __getattr__(self, name):
        attr = getattr(self._client, name)
        if callable(attr):

            @wraps(attr)
            def wrapped(*args, **kwargs):
                try:
                    return attr(*args, **kwargs)
                except ClientError as e:
                    error_code = e.response.get("Error", {}).get("Code")
                    if error_code == "403":
                        raise UnsuffiecentPermissions(
                            "Verify that your access keys are valid and associated with an appropriate role."
                        )
                    raise

            return wrapped
        return attr


class Storage:
    def __init__(self, cfg: StorageConfig | None = None):
        self.cfg: StorageConfig = cfg or StorageConfig()
        logger.debug(f"Storage endpoint: {self.cfg.s3endpoint or 'default'}")

    @property
    def client(self) -> CustomS3Client:
        client_config: BotoConfig = BotoConfig(
            retries={"max_attempts": 3, "mode": "standard"}
        )

        client_params: dict = {
            "config": client_config,
        }

        if self.cfg.s3endpoint:
            client_params["endpoint_url"] = self.cfg.s3endpoint

        raw_client = client("s3", **client_params)
        return CustomS3Client(raw_client)

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
        logger.debug(f'starting to search for files in: "{s3prefix}"')
        logger.debug(f"allowed extensions: {formatted_extensions}")

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

        logger.debug(f"found {len(found_objects)} file\\s")
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

    def upload(
        self,
        filepath: str,
        dst_s3uri: str,
        overwrite: bool = True,
        tags: dict[str, Any] | None = None,
    ) -> None:
        if not overwrite and self.obj_exists(dst_s3uri):
            logger.debug(f"File {dst_s3uri} already exists, skipping upload.")
            return

        extra_args: dict = {}
        if tags:
            extra_args["Tagging"] = self.dict2tagset(tags)

        dst_s3bucket, dst_s3key = split_s3uri(dst_s3uri)
        self.client.upload_file(filepath, dst_s3bucket, dst_s3key, ExtraArgs=extra_args)
        logger.debug(f"Uploaded {filepath} -> {dst_s3uri}")
        logger.debug(f"{dst_s3uri} tags: {tags if tags else 'None'}")

    def delete_obj(self, s3uri: str) -> None:
        s3bucket, s3key = split_s3uri(s3uri)
        self.client.delete_object(
            Bucket=s3bucket,
            Key=s3key,
        )
        logger.debug(f"deleted {s3uri}")

    def download_obj(self, s3uri: str, dst_path: str, overwrite: bool = True) -> None:
        if not overwrite and os.path.exists(dst_path):
            logger.debug(f"File {dst_path} already exists, skipping download.")
            return

        os.makedirs(os.path.dirname(dst_path), exist_ok=True)
        s3bucket, s3key = split_s3uri(s3uri)
        with open(dst_path, "wb") as f:
            self.client.download_fileobj(s3bucket, s3key, f)
        logger.debug(f"downloaded {s3uri} -> {dst_path}")

    def get_obj_tags(self, s3uri: str) -> dict[str, Any]:
        s3bucket, obj_key = split_s3uri(s3uri)
        response = self.client.get_object_tagging(Bucket=s3bucket, Key=obj_key)
        tags: dict[str, Any] = {tag["Key"]: tag["Value"] for tag in response["TagSet"]}
        return tags

    def put_obj_tags(self, s3uri: str, tags: dict[str, Any]) -> None:
        s3bucket, obj_key = split_s3uri(s3uri)

        self.client.put_object_tagging(
            Bucket=s3bucket, Key=obj_key, Tagging={"TagSet": self.dict2tagset(tags)}
        )

    @classmethod
    def dict2tagset(self, tag_dict: dict[str, Any]) -> list[dict[str, str]]:
        return [{"Key": str(k), "Value": str(v)} for k, v in tag_dict.items()]
