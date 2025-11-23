import pytest
import json
import os
from pyspark.sql import SparkSession


# Load Env Config
def load_env_config():
    env = os.environ.get("ENV", "local")  # Default to local
    with open("/app/config/env_config.json", "r") as f:
        return json.load(f)[env]


@pytest.fixture(scope="session")
def schema_registry():
    with open("/app/config/schema_registry.json", "r") as f:
        return json.load(f)


@pytest.fixture(scope="session")
def spark():
    config = load_env_config()

    return SparkSession.builder \
        .appName("DQ_Runner") \
        .config("spark.jars.packages",
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", f"s3a://{config['bucket_name']}/") \
        .config("spark.hadoop.fs.s3a.endpoint", config['s3_endpoint']) \
        .config("spark.hadoop.fs.s3a.access.key", config['s3_access_key']) \
        .config("spark.hadoop.fs.s3a.secret.key", config['s3_secret_key']) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()