from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import datetime
import os

def seed():
    # Standalone Spark Session for Seeding
    spark = SparkSession.builder \
        .appName("Seeder") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "s3a://warehouse/") \
        .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("S3_ENDPOINT", "http://minio:9000")) \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()

    # Data: 1 Valid, 1 Invalid (Amount 600 > Limit 500)
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
    
    print("Writing to local.db.customer_transactions...")
    df.write.format("iceberg").mode("overwrite").save("local.db.customer_transactions")
    print("Seed Complete.")

if __name__ == "__main__":
    seed()

