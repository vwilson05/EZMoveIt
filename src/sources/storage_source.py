import logging
import json
import os
import boto3

CONFIG_DIR = os.path.join(os.path.dirname(__file__), "../../config")


# ✅ Load Storage Config
def load_storage_config(pipeline_name):
    config_path = os.path.join(
        CONFIG_DIR, f"{pipeline_name.replace(' ', '_').lower()}_config.json"
    )
    if os.path.exists(config_path):
        with open(config_path, "r") as f:
            return json.load(f)
    return {}


# ✅ Fetch Data from AWS S3
def fetch_data_from_s3(pipeline_name):
    config = load_storage_config(pipeline_name)
    bucket = config.get("bucket_name")
    key = config.get("file_path")

    if not bucket or not key:
        logging.error("❌ Missing S3 bucket or file path in config!")
        return None

    s3 = boto3.client(
        "s3",
        aws_access_key_id=config.get("access_key"),
        aws_secret_access_key=config.get("secret_key"),
    )

    obj = s3.get_object(Bucket=bucket, Key=key)
    data = obj["Body"].read().decode("utf-8")
    return data
