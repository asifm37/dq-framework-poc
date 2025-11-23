import pytest
import json
import os
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def schema_registry():
    # Path inside the container
    with open("/app/config/schema_registry.json", "r") as f:
        return json.load(f)

@pytest.fixture(scope="session")
def spark():
    # Use 'minio' dns inside K8s
    s3_endpoint = os.environ.get("S3_ENDPOINT", "http://minio:9000")
    
    return SparkSession.builder \
        .appName("DQ_Runner") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "s3a://warehouse/") \
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
