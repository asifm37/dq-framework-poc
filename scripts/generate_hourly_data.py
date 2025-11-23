"""
Hourly Data Generator - Creates synthetic data for all tables
Runs independently (not part of Airflow DAG)
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import lit
from datetime import datetime, timedelta
import random
import json
import os
import sys
import uuid


def load_config():
    """Load environment and schema configuration"""
    config_path = "/Users/amohiuddeen/Github/dq-framework-poc/config"
    
    # Use mac_native for direct MinIO access from Mac
    env = os.environ.get("ENV", "mac_native")
    with open(f"{config_path}/env_config.json", "r") as f:
        env_config = json.load(f)[env]
    
    with open(f"{config_path}/schema_registry.json", "r") as f:
        schema_config = json.load(f)
    
    return env_config, schema_config


def init_spark(env_config):
    """Initialize Spark session with Iceberg"""
    return SparkSession.builder \
        .appName("HourlyDataGenerator") \
        .config("spark.jars.packages",
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", f"s3a://{env_config['bucket_name']}/") \
        .config("spark.hadoop.fs.s3a.endpoint", env_config['s3_endpoint']) \
        .config("spark.hadoop.fs.s3a.access.key", env_config['s3_access_key']) \
        .config("spark.hadoop.fs.s3a.secret.key", env_config['s3_secret_key']) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()


def generate_customer_transactions(num_rows, batch_time, inject_bad_data=False):
    """Generate transaction data"""
    data = []
    for i in range(num_rows):
        amount = random.uniform(10, 50000)
        limit = random.uniform(500, 100000)
        
        # Good data: amount < limit (most of the time)
        if random.random() < 0.05:  # 5% violations
            amount = limit + random.uniform(100, 1000)
        
        data.append((
            f"TXN_{uuid.uuid4().hex[:8]}",
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


def generate_user_profiles(num_rows, batch_time, inject_bad_data=False):
    """Generate user profile data"""
    data = []
    for i in range(num_rows):
        data.append((
            f"USER_{uuid.uuid4().hex[:8]}",
            f"user{random.randint(1000, 99999)}@example.com",
            random.randint(18, 80),
            random.choice(["US", "UK", "CA", "AU", "DE"]),
            batch_time
        ))
    
    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("email", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("country", StringType(), True),
        StructField("signup_timestamp", TimestampType(), True)
    ])
    return data, schema


def generate_product_inventory(num_rows, batch_time, inject_bad_data=True):
    """Generate inventory data - INTENTIONAL BAD DATA"""
    data = []
    for i in range(num_rows):
        quantity = random.randint(0, 1000)
        reorder_level = random.randint(10, 100)
        
        # Inject 15% bad data: quantity < reorder_level
        if inject_bad_data and random.random() < 0.15:
            quantity = random.randint(0, reorder_level - 1)
        
        data.append((
            f"PROD_{uuid.uuid4().hex[:8]}",
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


def generate_order_details(num_rows, batch_time, inject_bad_data=False):
    """Generate order line items"""
    data = []
    for i in range(num_rows):
        quantity = random.randint(1, 10)
        unit_price = random.uniform(5.0, 500.0)
        total = quantity * unit_price
        
        data.append((
            f"ORD_{uuid.uuid4().hex[:8]}",
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


def generate_web_clickstream(num_rows, batch_time, inject_bad_data=False):
    """Generate clickstream events"""
    data = []
    pages = ["/home", "/products", "/cart", "/checkout", "/profile"]
    events = ["click", "view", "scroll", "submit"]
    
    for i in range(num_rows):
        data.append((
            f"SESS_{uuid.uuid4().hex[:8]}",
            f"USER_{random.randint(1000, 9999)}",
            random.choice(pages),
            random.choice(events),
            random.randint(1, 600),
            batch_time
        ))
    
    schema = StructType([
        StructField("session_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("page_url", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("duration_seconds", IntegerType(), True),
        StructField("event_timestamp", TimestampType(), True)
    ])
    return data, schema


def generate_sensor_readings(num_rows, batch_time, inject_bad_data=True):
    """Generate IoT sensor data - INTENTIONAL BAD DATA"""
    data = []
    for i in range(num_rows):
        temp = random.uniform(-10, 45)
        humidity = random.uniform(20, 80)
        pressure = random.uniform(950, 1050)
        
        # Inject 15% bad data: out of range values
        if inject_bad_data and random.random() < 0.15:
            temp = random.uniform(-100, 200)  # Way out of range
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


def generate_payment_events(num_rows, batch_time, inject_bad_data=False):
    """Generate payment events"""
    data = []
    statuses = ["completed", "pending", "failed", "refunded"]
    
    for i in range(num_rows):
        amount = random.uniform(10, 5000)
        fee = amount * random.uniform(0.01, 0.05)
        net = amount - fee
        
        data.append((
            f"PAY_{uuid.uuid4().hex[:8]}",
            f"ORD_{uuid.uuid4().hex[:8]}",
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


def generate_email_campaigns(num_rows, batch_time, inject_bad_data=False):
    """Generate email campaign events"""
    data = []
    email_types = ["promotional", "newsletter", "transactional", "welcome"]
    
    for i in range(num_rows):
        sent = random.randint(100, 10000)
        opened = int(sent * random.uniform(0.1, 0.4))
        clicked = int(opened * random.uniform(0.05, 0.3))
        
        data.append((
            f"CAMP_{uuid.uuid4().hex[:8]}",
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


def generate_support_tickets(num_rows, batch_time, inject_bad_data=True):
    """Generate support ticket events - INTENTIONAL BAD DATA"""
    data = []
    categories = ["technical", "billing", "general", "complaint"]
    
    for i in range(num_rows):
        priority = random.randint(1, 5)
        sla = priority * random.randint(60, 240)  # SLA based on priority
        response_time = random.randint(10, sla - 10)
        
        # Inject 15% bad data: response_time > sla
        if inject_bad_data and random.random() < 0.15:
            response_time = sla + random.randint(10, 500)
        
        data.append((
            f"TKT_{uuid.uuid4().hex[:8]}",
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


def generate_api_request_logs(num_rows, batch_time, inject_bad_data=False):
    """Generate API request logs"""
    data = []
    endpoints = ["/api/users", "/api/products", "/api/orders", "/api/auth"]
    methods = ["GET", "POST", "PUT", "DELETE"]
    
    for i in range(num_rows):
        status = random.choice([200, 201, 400, 401, 404, 500])
        response_time = random.randint(50, 5000)
        
        data.append((
            f"REQ_{uuid.uuid4().hex[:8]}",
            random.choice(endpoints),
            random.choice(methods),
            status,
            response_time,
            batch_time
        ))
    
    schema = StructType([
        StructField("request_id", StringType(), True),
        StructField("endpoint", StringType(), True),
        StructField("method", StringType(), True),
        StructField("status_code", IntegerType(), True),
        StructField("response_time_ms", IntegerType(), True),
        StructField("request_timestamp", TimestampType(), True)
    ])
    return data, schema


# Map table names to generator functions
GENERATORS = {
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


def generate_and_write_data(spark, table_name, generator_func, num_rows, batch_time, inject_bad_data):
    """Generate and write data to Iceberg table"""
    print(f"\n📝 Generating {num_rows} rows for {table_name}...")
    
    data, schema = generator_func(num_rows, batch_time, inject_bad_data)
    df = spark.createDataFrame(data, schema)
    
    # Check if table exists
    try:
        spark.sql(f"DESCRIBE TABLE local.db.{table_name}")
        mode = "append"
        print(f"   Appending to existing table...")
    except:
        mode = "overwrite"
        print(f"   Creating new table...")
    
    # Write to Iceberg (creates new snapshot)
    df.write.format("iceberg").mode(mode).save(f"local.db.{table_name}")
    
    # Get snapshot ID
    snapshot_df = spark.sql(f"SELECT snapshot_id FROM local.db.{table_name}.snapshots ORDER BY committed_at DESC LIMIT 1")
    snapshot_id = snapshot_df.collect()[0][0]
    
    print(f"   ✅ Written {num_rows} rows | Snapshot ID: {snapshot_id}")
    return snapshot_id


def main():
    """Main execution"""
    print("=" * 70)
    print("🚀 HOURLY DATA GENERATOR")
    print("=" * 70)
    
    # Parse arguments
    if len(sys.argv) > 1:
        batch_time = datetime.fromisoformat(sys.argv[1])
    else:
        # Default to current hour start
        now = datetime.now()
        batch_time = now.replace(minute=0, second=0, microsecond=0)
    
    print(f"📅 Batch Time: {batch_time.isoformat()}")
    
    # Load configs
    env_config, schema_config = load_config()
    num_rows = schema_config["config"]["rows_per_batch"]
    
    print(f"🔧 MinIO Endpoint: {env_config['s3_endpoint']}")
    print(f"📊 Rows per table: {num_rows}")
    
    # Initialize Spark
    print("\n⚙️  Initializing Spark...")
    spark = init_spark(env_config)
    
    # Generate data for all tables
    tables_to_bad_data = ["product_inventory", "sensor_readings", "support_tickets"]
    
    for table_name, table_config in schema_config["tables"].items():
        inject_bad = table_name in tables_to_bad_data
        generator_func = GENERATORS[table_name]
        
        try:
            snapshot_id = generate_and_write_data(
                spark, table_name, generator_func, num_rows, batch_time, inject_bad
            )
        except Exception as e:
            print(f"   ❌ Error: {e}")
    
    spark.stop()
    print("\n" + "=" * 70)
    print("✅ DATA GENERATION COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()

