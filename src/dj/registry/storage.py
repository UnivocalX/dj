import copy
import json
import os
import posixpath
from functools import wraps
from logging import Logger, getLogger
from typing import Any, Iterable

from boto3 import client
from boto3.exceptions import S3UploadFailedError
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from dj.exceptions import S3BucketNotFound, S3KeyNotFound, UnsuffiecentPermissions
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
                except (S3UploadFailedError, ClientError) as e:
                    # Handle S3UploadFailedError by extracting underlying ClientError
                    if isinstance(e, S3UploadFailedError):
                        # Try to extract the underlying exception
                        if hasattr(e, "last_exception") and hasattr(
                            e.last_exception, "response"
                        ):
                            client_error = e.last_exception
                            error_code = client_error.response.get("Error", {}).get(
                                "Code"
                            )
                        else:
                            # Fallback: check the string representation for specific errors
                            error_message = str(e)
                            if "NoSuchBucket" in error_message:
                                raise S3BucketNotFound("Failed to find s3 bucket")
                            elif (
                                "403" in error_message
                                or "AccessDenied" in error_message
                            ):
                                raise UnsuffiecentPermissions(
                                    "Verify that your access keys are valid and associated with an appropriate role."
                                )
                            elif "404" in error_message or "NoSuchKey" in error_message:
                                raise S3KeyNotFound("Failed to find s3 key")
                            raise
                    else:
                        error_code = e.response.get("Error", {}).get("Code")

                    # Process the error code (only if we successfully extracted it)
                    if "error_code" in locals():
                        if error_code == "NoSuchBucket":
                            raise S3BucketNotFound("Failed to find s3 bucket")
                        elif error_code == "403" or error_code == "AccessDenied":
                            raise UnsuffiecentPermissions(
                                "Verify that your access keys are valid and associated with an appropriate role."
                            )
                        elif error_code == "404" or error_code == "NoSuchKey":
                            raise S3KeyNotFound("Failed to find s3 key")

                    raise

            return wrapped
        return attr


class Storage:
    def __init__(self, cfg: StorageConfig | None = None):
        self.cfg: StorageConfig = cfg or StorageConfig()
        logger.debug(f"Storage endpoint: {self.cfg.s3endpoint or 'default'}")
        self._check_connection()

    def _check_connection(self):
        logger.debug("Checking connection by listing buckets.")
        try:
            self.client.list_buckets()
        except Exception:
            raise UnsuffiecentPermissions(
                "Failed to list buckets please make you have the required permissions"
            )

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
        except S3KeyNotFound:
            return False

    def prefix_exists(self, s3uri: str) -> bool:
        s3bucket, s3prefix = split_s3uri(s3uri)

        try:
            self.client.head_bucket(Bucket=s3bucket)
            return bool(
                self.client.list_objects_v2(
                    Bucket=s3bucket, Prefix=s3prefix, MaxKeys=1
                ).get("Contents")
            )
        except S3KeyNotFound:
            return False

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
        self.client.download_file(s3bucket, s3key, dst_path)
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

    def update_bucket_policy(self, s3bucket: str, s3bucket_policy: dict) -> dict:
        try:
            response = self.client.get_bucket_policy(Bucket=s3bucket)
            existing_policy: dict = json.loads(response["Policy"])
            merged_policy: dict = copy.deepcopy(s3bucket_policy)

            new_sids: set[Any] = {
                stmt.get("Sid") for stmt in merged_policy["Statement"]
            }

            for stmt in existing_policy["Statement"]:
                if stmt.get("Sid") not in new_sids:
                    merged_policy["Statement"].append(stmt)

            s3bucket_policy = merged_policy

        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchBucketPolicy":
                logger.debug(f"No existing policy found for bucket {s3bucket}")
            else:
                raise e

        try:
            response = self.client.put_bucket_policy(
                Bucket=s3bucket, Policy=json.dumps(s3bucket_policy)
            )
        except ClientError as e:
            raise e

        logger.debug(f"Successfully updated bucket policy for {s3bucket}")
        return s3bucket_policy

    def add_lifecycle_rule(self, s3bucket: str, lifecycle_rule: dict) -> list:
        new_rule_id: str | None = str(lifecycle_rule.get("ID"))

        if not new_rule_id:
            raise ValueError("Lifecycle rule must have an 'ID' field")

        try:
            # Get existing lifecycle configuration
            response = self.client.get_bucket_lifecycle_configuration(Bucket=s3bucket)
            existing_rules: list = response.get("Rules", [])

            # Check for existing rule with same ID and overwrite if found
            updated_rules: list = []
            rule_replaced: bool = False

            for existing_rule in existing_rules:
                if existing_rule.get("ID") == new_rule_id:
                    updated_rules.append(lifecycle_rule)
                    rule_replaced = True
                    logger.warning(f'Replaced existing rule with ID: "{new_rule_id}"')
                else:
                    updated_rules.append(existing_rule)

            if not rule_replaced:
                updated_rules.append(lifecycle_rule)
                logger.debug(f'Added new rule with ID: "{new_rule_id}"')

        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchLifecycleConfiguration":
                updated_rules = [lifecycle_rule]
                logger.debug(
                    f'Created new lifecycle configuration with rule ID: "{new_rule_id}"'
                )
            else:
                raise e

        response = self.client.put_bucket_lifecycle_configuration(
            Bucket=s3bucket, LifecycleConfiguration={"Rules": updated_rules}
        )

        return updated_rules
