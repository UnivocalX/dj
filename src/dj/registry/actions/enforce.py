import copy
from logging import Logger, getLogger

from dj.registry.actions.actor import RegistryActor

logger: Logger = getLogger(__name__)

PROTECT_DATA_S3BUCKET_POLICY_TPL: dict = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "DenyDeleteFromProtectedPrefix",
            "Effect": "Deny",
            "Principal": "*",
            "Action": [
                "s3:DeleteObject",
                "s3:DeleteObjectVersion",
            ],
            "Resource": "{s3bucket_arn}/{s3prefix}/*",
        }
    ],
}

DELETE_BY_TAG_LIFECYCLE_RULE_TPL: dict = {
    "ID": "DeleteObjectsWithZeroRefCount",
    "Status": "Enabled",
    "Filter": {
        "And": {
            "Prefix": "{s3prefix}/",
            "Tags": [{"Key": "ref_count", "Value": "0"}],
        }
    },
    "Expiration": {
        "Days": 1  # minimum supported by S3 lifecycle
    },
}


class PolicyEnforcer(RegistryActor):
    def enforce(self) -> None:
        assert self.cfg.s3bucket is not None, "S3 bucket must be configured."

        s3bucket: str = self.cfg.s3bucket
        s3prefix: str = self.cfg.s3prefix

        # Update bucket policy
        s3bucket_policy: dict = copy.deepcopy(PROTECT_DATA_S3BUCKET_POLICY_TPL)
        s3bucket_arn = f"arn:aws:s3:::{s3bucket}"

        s3bucket_policy["Statement"][0]["Resource"] = s3bucket_policy["Statement"][0][
            "Resource"
        ].format(s3bucket_arn=s3bucket_arn, s3prefix=s3prefix)

        logger.info(f"Applying bucket policy to {s3bucket}/{s3prefix}")
        self.storage.update_bucket_policy(s3bucket, s3bucket_policy)

        # Add lifecycle rule
        lifecycle_rule = copy.deepcopy(DELETE_BY_TAG_LIFECYCLE_RULE_TPL)
        lifecycle_rule["Filter"]["And"]["Prefix"] = lifecycle_rule["Filter"]["And"][
            "Prefix"
        ].format(s3prefix=s3prefix)

        logger.info(f"Applying lifecycle rule for {s3bucket}/{s3prefix}")
        self.storage.add_lifecycle_rule(s3bucket, lifecycle_rule)

        logger.info("Policy enforcement completed successfully")
