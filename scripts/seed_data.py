from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import datetime
import os
import s3fs
import json

def load_env_config():
    # When running seeder locally on Mac (outside docker), we might need localhost
    # But if running in docker, we need host.docker.internal
    # For simplicity, let's assume we run seeder from Docker too.
    env = os.environ.get("ENV", "local")
    with open("config/env_config.json", "r") as f:
        return json.load(f)[env]

def ensure_bucket_exists(bucket_name):
    """
    Explicitly creates the bucket in MinIO using s3fs.
    Spark/Hadoop S3A often fails if the root bucket is missing.
    """
    print(f"Checking for bucket: {bucket_name}...")
    
    # Connect to MinIO using the internal K8s DNS
    endpoint = os.environ.get("S3_ENDPOINT", config['s3_endpoint'])
    
    fs = s3fs.S3FileSystem(
        client_kwargs={'endpoint_url': endpoint},
        key='admin', 
        secret='password'
    )
    
    try:
        if not fs.exists(bucket_name):
            fs.mkdir(bucket_name)
            print(f"✅ Created bucket: {bucket_name}")
        else:
            print(f"ℹ️ Bucket {bucket_name} already exists.")
    except Exception as e:
        print(f"⚠️ Warning: Could not check/create bucket: {e}")

def seed():
    # 1. Ensure Storage Exists
    ensure_bucket_exists(config['bucket_name'])

    # 2. Initialize Spark
    print("Initializing Spark Session...")
    spark = SparkSession.builder \
        .appName("Seeder") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.hadoop:hadoop-aws:3.3.4") \
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

    # 3. Generate Data
    print("Generating Data...")
    data = [
        ("TXN_1", 100.0, 500.0, datetime(2023, 1, 1, 10, 30)),
        ("TXN_2", 600.0, 500.0, datetime(2023, 1, 1, 10, 45)) 
    ]
    schema = StructType([
        StructField("txn_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("total_limit", DoubleType(), True),
        StructField("txn_date", TimestampType(), True)
    ])

    df = spark.createDataFrame(data, schema)
    
    # 4. Write to Iceberg
    print("Writing to Iceberg table: local.db.customer_transactions")
    df.write.format("iceberg").mode("overwrite").save("local.db.customer_transactions")
    print("✅ Seed Complete.")

if __name__ == "__main__":
    config = load_env_config()
    seed()
