"""
setup_s3.py
===========
Creates the MTA Data Lake S3 bucket with the correct
folder (prefix) structure:

  mta-data-lake-rudri/
  ├── raw/subway/
  ├── processed/subway/
  └── logs/

Run once before the first pipeline execution.
Usage:
    python src/setup_s3.py
"""

import os
import logging
import boto3
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s")
log = logging.getLogger(__name__)

BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "mta-data-lake-rudri")
AWS_REGION  = os.getenv("AWS_REGION", "us-east-1")

# Folder structure — S3 uses empty placeholder objects to represent folders
PREFIXES = [
    "raw/subway/",
    "processed/subway/",
    "logs/",
]


def create_bucket(s3_client) -> bool:
    """Create the S3 bucket if it does not already exist."""
    try:
        if AWS_REGION == "us-east-1":
            s3_client.create_bucket(Bucket=BUCKET_NAME)
        else:
            s3_client.create_bucket(
                Bucket=BUCKET_NAME,
                CreateBucketConfiguration={"LocationConstraint": AWS_REGION},
            )
        log.info("Created bucket: s3://%s", BUCKET_NAME)
        return True
    except ClientError as exc:
        error_code = exc.response["Error"]["Code"]
        if error_code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            log.info("Bucket already exists: s3://%s", BUCKET_NAME)
            return True
        log.error("Failed to create bucket: %s", exc)
        return False


def create_folder_structure(s3_client) -> None:
    """Create placeholder objects to establish the folder structure."""
    for prefix in PREFIXES:
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=prefix,
            Body=b"",
        )
        log.info("Created folder: s3://%s/%s", BUCKET_NAME, prefix)


def enable_versioning(s3_client) -> None:
    """Enable versioning on the bucket for data lineage tracking."""
    s3_client.put_bucket_versioning(
        Bucket=BUCKET_NAME,
        VersioningConfiguration={"Status": "Enabled"},
    )
    log.info("Versioning enabled on s3://%s", BUCKET_NAME)


def apply_lifecycle_policy(s3_client) -> None:
    """
    Apply a lifecycle policy:
    - Raw data: transition to Glacier after 90 days
    - Logs: expire after 30 days
    """
    policy = {
        "Rules": [
            {
                "ID": "archive-raw-data",
                "Status": "Enabled",
                "Filter": {"Prefix": "raw/"},
                "Transitions": [
                    {"Days": 90, "StorageClass": "GLACIER"}
                ],
            },
            {
                "ID": "expire-logs",
                "Status": "Enabled",
                "Filter": {"Prefix": "logs/"},
                "Expiration": {"Days": 30},
            },
        ]
    }
    s3_client.put_bucket_lifecycle_configuration(
        Bucket=BUCKET_NAME,
        LifecycleConfiguration=policy,
    )
    log.info("Lifecycle policy applied to s3://%s", BUCKET_NAME)


def setup():
    s3 = boto3.client("s3", region_name=AWS_REGION)

    log.info("Setting up MTA Data Lake bucket: %s", BUCKET_NAME)
    if not create_bucket(s3):
        return

    create_folder_structure(s3)
    enable_versioning(s3)
    apply_lifecycle_policy(s3)

    log.info("\n✅ Data Lake setup complete!")
    log.info("Structure:")
    for p in PREFIXES:
        log.info("  s3://%s/%s", BUCKET_NAME, p)


if __name__ == "__main__":
    setup()
