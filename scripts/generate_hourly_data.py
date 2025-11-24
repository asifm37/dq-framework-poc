"""
Hourly Data Generator for DQ Framework POC
Generates synthetic data for 10 tables with configurable data quality issues.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, TimestampType
)
from datetime import datetime
from typing import Tuple, Dict, Any, Callable, List
import random
import json
import os
import sys
import uuid

# Constants
DEFAULT_CONFIG_PATH = "/Users/amohiuddeen/Github/dq-framework-poc/config"
DOCKER_CONFIG_PATH = "/app/config"
DEFAULT_ENV = "local"
CATALOG_NAME = "local"
DATABASE_NAME = "db"


def load_config() -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Load environment and schema configuration files.
    
    Returns:
        Tuple containing (env_config, schema_config)
    """
    config_path = DOCKER_CONFIG_PATH if os.path.exists(DOCKER_CONFIG_PATH) else DEFAULT_CONFIG_PATH
    env = os.environ.get("ENV", DEFAULT_ENV)
    
    with open(f"{config_path}/env_config.json", "r") as f:
        env_config = json.load(f)[env]
    
    with open(f"{config_path}/schema_registry.json", "r") as f:
        schema_config = json.load(f)
    
    return env_config, schema_config


def init_spark(env_config: Dict[str, Any]) -> SparkSession:
    """
    Initialize Spark session with Iceberg and S3 configurations.
    
    Args:
        env_config: Environment configuration containing S3/MinIO settings
        
    Returns:
        Configured SparkSession instance
    """
    spark_configs = {
        "spark.jars.packages": "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.hadoop:hadoop-aws:3.3.4",
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        f"spark.sql.catalog.{CATALOG_NAME}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{CATALOG_NAME}.type": "hadoop",
        f"spark.sql.catalog.{CATALOG_NAME}.warehouse": f"s3a://{env_config['bucket_name']}/",
        "spark.hadoop.fs.s3a.endpoint": env_config['s3_endpoint'],
        "spark.hadoop.fs.s3a.access.key": env_config['s3_access_key'],
        "spark.hadoop.fs.s3a.secret.key": env_config['s3_secret_key'],
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false"
    }
    
    builder = SparkSession.builder.appName("HourlyDataGenerator")
    for key, value in spark_configs.items():
        builder = builder.config(key, value)
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("INFO")
    
    return spark


def _generate_uuid(prefix: str) -> str:
    """Generate a UUID with a given prefix."""
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def generate_customer_transactions(
    num_rows: int, 
    batch_time: datetime, 
    inject_bad_data: bool = False
) -> Tuple[List[tuple], StructType]:
    """
    Generate customer transaction data with optional quality issues.
    Bad data: ~5% of transactions exceed their credit limit.
    """
    data = []
    for _ in range(num_rows):
        amount = random.uniform(10, 50000)
        limit = random.uniform(500, 100000)
        
        # Inject bad data: amount exceeds limit
        if inject_bad_data and random.random() < 0.05:
            amount = limit + random.uniform(100, 1000)
        
        data.append((
            _generate_uuid("TXN"),
            f"CUST_{random.randint(1000, 9999)}",
            amount,
            limit,
            random.choice(["USD", "EUR", "GBP"]),
            batch_time
        ))
    
    schema = StructType([
        StructField("txn_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("total_limit", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("txn_timestamp", TimestampType(), True)
    ])
    return data, schema


def generate_user_profiles(
    num_rows: int, 
    batch_time: datetime, 
    inject_bad_data: bool = False
) -> Tuple[List[tuple], StructType]:
    """Generate user profile data."""
    data = [
        (
            _generate_uuid("USER"),
            f"user{random.randint(1000, 99999)}@example.com",
            random.randint(18, 80),
            random.choice(["US", "UK", "CA", "AU", "DE"]),
            batch_time
        )
        for _ in range(num_rows)
    ]
    
    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("email", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("country", StringType(), True),
        StructField("signup_timestamp", TimestampType(), True)
    ])
    return data, schema


def generate_product_inventory(
    num_rows: int, 
    batch_time: datetime, 
    inject_bad_data: bool = True
) -> Tuple[List[tuple], StructType]:
    """
    Generate product inventory data with optional quality issues.
    Bad data: ~15% of products have quantity below reorder level.
    """
    data = []
    for _ in range(num_rows):
        quantity = random.randint(0, 1000)
        reorder_level = random.randint(10, 100)
        
        # Inject bad data: quantity below reorder level
        if inject_bad_data and random.random() < 0.15:
            quantity = random.randint(0, reorder_level - 1)
        
        data.append((
            _generate_uuid("PROD"),
            f"SKU-{random.randint(10000, 99999)}",
            quantity,
            reorder_level,
            random.uniform(1.0, 999.99),
            batch_time
        ))
    
    schema = StructType([
        StructField("product_id", StringType(), True),
        StructField("sku", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("reorder_level", IntegerType(), True),
        StructField("price", DoubleType(), True),
        StructField("updated_timestamp", TimestampType(), True)
    ])
    return data, schema


def generate_order_details(
    num_rows: int, 
    batch_time: datetime, 
    inject_bad_data: bool = False
) -> Tuple[List[tuple], StructType]:
    """Generate order details data."""
    data = []
    for _ in range(num_rows):
        quantity = random.randint(1, 10)
        unit_price = random.uniform(5.0, 500.0)
        total = quantity * unit_price
        
        data.append((
            _generate_uuid("ORD"),
            f"PROD_{random.randint(1000, 9999)}",
            quantity,
            unit_price,
            total,
            batch_time
        ))
    
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("order_timestamp", TimestampType(), True)
    ])
    return data, schema


def generate_web_clickstream(
    num_rows: int, 
    batch_time: datetime, 
    inject_bad_data: bool = False
) -> Tuple[List[tuple], StructType]:
    """Generate web clickstream data."""
    pages = ["/home", "/products", "/cart", "/checkout", "/profile"]
    events = ["click", "view", "scroll", "submit"]
    
    data = [
        (
            _generate_uuid("SESS"),
            f"USER_{random.randint(1000, 9999)}",
            random.choice(pages),
            random.choice(events),
            random.randint(1, 600),
            batch_time
        )
        for _ in range(num_rows)
    ]
    
    schema = StructType([
        StructField("session_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("page_url", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("duration_seconds", IntegerType(), True),
        StructField("event_timestamp", TimestampType(), True)
    ])
    return data, schema


def generate_sensor_readings(
    num_rows: int, 
    batch_time: datetime, 
    inject_bad_data: bool = True
) -> Tuple[List[tuple], StructType]:
    """
    Generate sensor reading data with optional quality issues.
    Bad data: ~15% of readings have out-of-range temperature and humidity values.
    """
    data = []
    for _ in range(num_rows):
        temp = random.uniform(-10, 45)
        humidity = random.uniform(20, 80)
        pressure = random.uniform(950, 1050)
        
        # Inject bad data: out-of-range sensor values
        if inject_bad_data and random.random() < 0.15:
            temp = random.uniform(-100, 200)
            humidity = random.uniform(-10, 150)
        
        data.append((
            f"SENSOR_{random.randint(1000, 9999)}",
            temp,
            humidity,
            pressure,
            batch_time
        ))
    
    schema = StructType([
        StructField("sensor_id", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("pressure", DoubleType(), True),
        StructField("reading_timestamp", TimestampType(), True)
    ])
    return data, schema


def generate_payment_events(
    num_rows: int, 
    batch_time: datetime, 
    inject_bad_data: bool = False
) -> Tuple[List[tuple], StructType]:
    """Generate payment event data."""
    statuses = ["completed", "pending", "failed", "refunded"]
    
    data = []
    for _ in range(num_rows):
        amount = random.uniform(10, 5000)
        fee = amount * random.uniform(0.01, 0.05)
        net = amount - fee
        
        data.append((
            _generate_uuid("PAY"),
            _generate_uuid("ORD"),
            amount,
            fee,
            net,
            random.choice(statuses),
            batch_time
        ))
    
    schema = StructType([
        StructField("payment_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("fee", DoubleType(), True),
        StructField("net_amount", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("payment_timestamp", TimestampType(), True)
    ])
    return data, schema


def generate_email_campaigns(
    num_rows: int, 
    batch_time: datetime, 
    inject_bad_data: bool = False
) -> Tuple[List[tuple], StructType]:
    """Generate email campaign data."""
    email_types = ["promotional", "newsletter", "transactional", "welcome"]
    
    data = []
    for _ in range(num_rows):
        sent = random.randint(100, 10000)
        opened = int(sent * random.uniform(0.1, 0.4))
        clicked = int(opened * random.uniform(0.05, 0.3))
        
        data.append((
            _generate_uuid("CAMP"),
            f"USER_{random.randint(1000, 9999)}",
            random.choice(email_types),
            sent,
            opened,
            clicked,
            batch_time
        ))
    
    schema = StructType([
        StructField("campaign_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("email_type", StringType(), True),
        StructField("sent_count", IntegerType(), True),
        StructField("open_count", IntegerType(), True),
        StructField("click_count", IntegerType(), True),
        StructField("sent_timestamp", TimestampType(), True)
    ])
    return data, schema


def generate_support_tickets(
    num_rows: int, 
    batch_time: datetime, 
    inject_bad_data: bool = True
) -> Tuple[List[tuple], StructType]:
    """
    Generate support ticket data with optional quality issues.
    Bad data: ~15% of tickets have response time exceeding SLA.
    """
    categories = ["technical", "billing", "general", "complaint"]
    
    data = []
    for _ in range(num_rows):
        priority = random.randint(1, 5)
        sla = priority * random.randint(60, 240)
        response_time = random.randint(10, sla - 10)
        
        # Inject bad data: response time exceeds SLA
        if inject_bad_data and random.random() < 0.15:
            response_time = sla + random.randint(10, 500)
        
        data.append((
            _generate_uuid("TKT"),
            f"USER_{random.randint(1000, 9999)}",
            priority,
            random.choice(categories),
            response_time,
            sla,
            batch_time
        ))
    
    schema = StructType([
        StructField("ticket_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("priority", IntegerType(), True),
        StructField("category", StringType(), True),
        StructField("response_time_minutes", IntegerType(), True),
        StructField("sla_minutes", IntegerType(), True),
        StructField("created_timestamp", TimestampType(), True)
    ])
    return data, schema


def generate_api_request_logs(
    num_rows: int, 
    batch_time: datetime, 
    inject_bad_data: bool = False
) -> Tuple[List[tuple], StructType]:
    """Generate API request log data."""
    endpoints = ["/api/users", "/api/products", "/api/orders", "/api/auth"]
    methods = ["GET", "POST", "PUT", "DELETE"]
    status_codes = [200, 201, 400, 401, 404, 500]
    
    data = [
        (
            _generate_uuid("REQ"),
            random.choice(endpoints),
            random.choice(methods),
            random.choice(status_codes),
            random.randint(50, 5000),
            batch_time
        )
        for _ in range(num_rows)
    ]
    
    schema = StructType([
        StructField("request_id", StringType(), True),
        StructField("endpoint", StringType(), True),
        StructField("method", StringType(), True),
        StructField("status_code", IntegerType(), True),
        StructField("response_time_ms", IntegerType(), True),
        StructField("request_timestamp", TimestampType(), True)
    ])
    return data, schema


# Generator function type
GeneratorFunc = Callable[[int, datetime, bool], Tuple[List[tuple], StructType]]

# Registry of all data generators
GENERATORS: Dict[str, GeneratorFunc] = {
    "customer_transactions": generate_customer_transactions,
    "user_profiles": generate_user_profiles,
    "product_inventory": generate_product_inventory,
    "order_details": generate_order_details,
    "web_clickstream": generate_web_clickstream,
    "sensor_readings": generate_sensor_readings,
    "payment_events": generate_payment_events,
    "email_campaigns": generate_email_campaigns,
    "support_tickets": generate_support_tickets,
    "api_request_logs": generate_api_request_logs
}

# Tables that should have bad data injected
TABLES_WITH_BAD_DATA = {"product_inventory", "sensor_readings", "support_tickets"}


def _table_exists(spark: SparkSession, table_name: str) -> bool:
    """
    Check if an Iceberg table exists in the catalog.
    
    Args:
        spark: Active SparkSession
        table_name: Name of the table to check
        
    Returns:
        True if table exists, False otherwise
    """
    try:
        tables = spark.sql(f"SHOW TABLES IN {CATALOG_NAME}.{DATABASE_NAME}")
        existing_tables = {row.tableName for row in tables.collect()}
        return table_name in existing_tables
    except Exception:
        return False


def _get_snapshot_id(spark: SparkSession, table_path: str) -> str | None:
    """
    Retrieve the latest snapshot ID for a table.
    
    Args:
        spark: Active SparkSession
        table_path: Full path to the table
        
    Returns:
        Snapshot ID if available, None otherwise
    """
    try:
        history = spark.sql(
            f"SELECT * FROM {table_path}.history "
            f"ORDER BY made_current_at DESC LIMIT 1"
        )
        if history.count() > 0:
            return history.collect()[0]['snapshot_id']
    except Exception:
        pass
    return None


def generate_and_write_data(
    spark: SparkSession,
    table_name: str,
    generator_func: GeneratorFunc,
    num_rows: int,
    batch_time: datetime,
    inject_bad_data: bool
) -> str | None:
    """
    Generate synthetic data and write it to an Iceberg table.
    
    Args:
        spark: Active SparkSession
        table_name: Name of the target table
        generator_func: Function to generate the data
        num_rows: Number of rows to generate
        batch_time: Timestamp for the batch
        inject_bad_data: Whether to inject data quality issues
        
    Returns:
        Snapshot ID if available, None otherwise
    """
    print(f"\n📝 Generating {num_rows} rows for {table_name}...")
    
    # Generate data
    data, schema = generator_func(num_rows, batch_time, inject_bad_data)
    df = spark.createDataFrame(data, schema)
    
    table_path = f"{CATALOG_NAME}.{DATABASE_NAME}.{table_name}"
    table_exists = _table_exists(spark, table_name)
    
    # Write data to Iceberg table
    try:
        if table_exists:
            print(f"   Appending to existing table...")
            df.writeTo(table_path).append()
        else:
            print(f"   Creating new table...")
            df.writeTo(table_path).create()
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"   Table exists, appending data...")
            df.writeTo(table_path).append()
        else:
            raise
    
    # Retrieve snapshot information
    snapshot_id = _get_snapshot_id(spark, table_path)
    if snapshot_id:
        print(f"   ✅ Written {num_rows} rows | Snapshot ID: {snapshot_id}")
    else:
        print(f"   ✅ Written {num_rows} rows")
    
    return snapshot_id


def main() -> None:
    """Main entry point for the hourly data generator."""
    print("=" * 70)
    print("🚀 HOURLY DATA GENERATOR")
    print("=" * 70)
    
    # Parse batch time from command line or use current hour
    if len(sys.argv) > 1:
        batch_time = datetime.fromisoformat(sys.argv[1])
    else:
        now = datetime.now()
        batch_time = now.replace(minute=0, second=0, microsecond=0)
    
    print(f"📅 Batch Time: {batch_time.isoformat()}")
    
    # Load configurations
    env_config, schema_config = load_config()
    num_rows = schema_config["config"]["rows_per_batch"]
    
    print(f"🔧 MinIO Endpoint: {env_config['s3_endpoint']}")
    print(f"📊 Rows per table: {num_rows}")
    
    # Initialize Spark
    print("\n⚙️  Initializing Spark...")
    spark = init_spark(env_config)
    
    # Generate data for all tables
    success_count = 0
    error_count = 0
    
    for table_name in schema_config["tables"].keys():
        inject_bad = table_name in TABLES_WITH_BAD_DATA
        generator_func = GENERATORS.get(table_name)
        
        if not generator_func:
            print(f"\n⚠️  No generator found for {table_name}, skipping...")
            continue
        
        try:
            generate_and_write_data(
                spark, table_name, generator_func, num_rows, batch_time, inject_bad
            )
            success_count += 1
        except Exception as e:
            error_count += 1
            print(f"   ❌ Error: {str(e)[:200]}")
            import traceback
            traceback.print_exc()
    
    # Cleanup
    spark.stop()
    
    # Summary
    print("\n" + "=" * 70)
    print(f"✅ DATA GENERATION COMPLETE - {success_count} succeeded, {error_count} failed")
    print("=" * 70)


if __name__ == "__main__":
    main()
