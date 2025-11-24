# Data Quality Framework POC

> **Incremental Snapshot-Based Data Quality Validation for Iceberg Data Lakes**

A production-ready framework that validates data quality in MinIO/Iceberg using snapshot-based incremental validation. Validates only newly ingested data without requiring watermark columns.

---

## 📋 Table of Contents

- [Overview](#-overview)
- [Architecture](#-architecture)
- [Features](#-features)
- [Quick Start](#-quick-start)
- [Project Structure](#-project-structure)
- [Configuration](#-configuration)
- [Running the Framework](#-running-the-framework)
- [Monitoring & Reports](#-monitoring--reports)

---

## 🎯 Overview

This framework provides automated data quality validation for data lakes using:
- **Apache Iceberg** for table format and snapshot management
- **Apache Spark** for distributed data processing
- **MinIO** as S3-compatible object storage
- **Airflow** for orchestration (optional)

### Key Innovation: Snapshot-Based Incremental Validation

Instead of using watermark columns, the framework tracks Iceberg snapshot IDs to validate only new data:

```python
# Read only data added since last validation
df = spark.read.format("iceberg")
    .option("start-snapshot-id", last_validated_snapshot)
    .option("end-snapshot-id", current_snapshot)
    .load("table_name")
```

**Benefits:**
- ✅ Works with any table structure (no watermark column required)
- ✅ Efficient - reads only new Parquet files
- ✅ Accurate - validates exactly what was added
- ✅ Scalable - handles 100+ tables

---

## 🏗️ Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                     Data Injection                            │
│  (Hourly Cron Job or Manual) → MinIO/Iceberg Tables          │
└────────────────────────┬─────────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────────────┐
│                  Validation Pipeline                          │
│                                                               │
│  ┌─────────────────┐      ┌──────────────────┐              │
│  │ Metadata Tests  │─────→│ Data Validation  │              │
│  │ (Schema Check)  │ PASS │ (Quality Checks) │              │
│  └─────────────────┘      └──────────────────┘              │
│         │                           │                         │
│         │ FAIL                      │                         │
│         ▼                           ▼                         │
│  ┌──────────────────────────────────────────┐               │
│  │      Results Tracker (SQLite)            │               │
│  │  - Validation results                    │               │
│  │  - Snapshot IDs                          │               │
│  │  - Pass/Fail metrics                     │               │
│  └──────────────────────────────────────────┘               │
└────────────────────────┬─────────────────────────────────────┘
                         │
         ┌───────────────┼───────────────┐
         ▼               ▼               ▼
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│  Dashboard  │  │   Allure    │  │   Alerts    │
│   (HTML)    │  │   Report    │  │  (Logs)     │
└─────────────┘  └─────────────┘  └─────────────┘
```

### Components

1. **Data Generator** - Creates synthetic data with intentional quality issues
2. **Metadata Tests** - Validates table schema and captures snapshot IDs
3. **Data Validation** - Runs quality checks on incremental data
4. **Results Tracker** - Stores validation history in SQLite
5. **Dashboard** - HTML dashboard with time-series charts
6. **Alert Handler** - Logs warnings for tables below 90% pass rate

---

## ✨ Features

| Feature | Description |
|---------|-------------|
| **Incremental Validation** | Only validates new data using Iceberg snapshots |
| **Schema-Agnostic** | Works with any table structure, no column dependencies |
| **Circuit Breaker Pattern** | Skips data validation if metadata tests fail |
| **Configurable Rules** | Define validation rules per table in JSON |
| **Real-time Dashboard** | Interactive HTML dashboard with Chart.js |
| **Automated Alerting** | File-based alerts for quality issues |
| **Docker Execution** | Isolated Spark environment with all dependencies |
| **Airflow Integration** | Hourly scheduled validation (optional) |
| **Scalable** | Currently 10 tables, designed for 100+ |

---

## 🚀 Quick Start

### Prerequisites

- **Docker** installed and running
- **MinIO** accessible at `http://192.168.1.2:9000` (or update `config/env_config.json`)
- **Python 3.9+** for local scripts
- **Docker image** built: `dq-runner:latest`

### 1. Generate Sample Data

```bash
./run_data_injection.sh
```

**Output:** Creates 10 tables with 100 rows each in MinIO

### 2. Run Validation

```bash
./run_quick_test.sh
```

**Output:** 
- Runs metadata and data validation tests
- Generates Allure report
- Updates dashboard
- Checks for alerts

### 3. View Results

```bash
# Interactive Dashboard
open reports/dashboard.html

# Detailed Test Report
open allure-report/index.html

# Alert Logs
cat alerts/alerts.log
```

---

## 📁 Project Structure

```
dq-framework-poc/
├── config/
│   ├── env_config.json              # MinIO/Spark configuration
│   └── schema_registry.json         # Table schemas + validation rules
│
├── dags/
│   └── dq_hourly_pipeline_bash.py   # Airflow DAG (hourly schedule)
│
├── scripts/
│   ├── generate_hourly_data.py      # Data generator with bad data
│   ├── results_tracker.py           # SQLite tracking + snapshots
│   ├── dashboard_generator.py       # HTML dashboard generator
│   └── alert_handler.py             # Alert checker (<90% threshold)
│
├── tests/
│   ├── conftest.py                  # Pytest fixtures (Spark session)
│   ├── test_metadata.py             # Schema validation + snapshot capture
│   └── test_validation.py           # Data quality checks (incremental)
│
├── infrastructure/
│   └── Dockerfile                   # Spark/Iceberg/Pytest image
│
├── results/
│   └── dq_results.db                # SQLite database (validation history)
│
├── reports/
│   └── dashboard.html               # Auto-generated dashboard
│
├── alerts/
│   └── alerts.log                   # Alert history
│
├── run_data_injection.sh            # Generate data (manual/cron)
├── run_quick_test.sh                # Run all tests (bypasses Airflow)
├── setup_cron.sh                    # Configure hourly cron job
├── requirements.txt                 # Python dependencies
└── README.md                        # This file
```

---

## ⚙️ Configuration

### MinIO Connection

Edit `config/env_config.json`:

```json
{
  "local": {
    "s3_endpoint": "http://192.168.1.2:9000",
    "s3_access_key": "minioadmin",
    "s3_secret_key": "minioadmin",
    "bucket_name": "warehouse",
    "warehouse_path": "s3a://warehouse/"
  }
}
```

### Table Configuration

Edit `config/schema_registry.json` to add/modify tables:

```json
{
  "tables": [
    {
      "table_name": "your_table",
      "columns": [
        {"name": "id", "type": "bigint"},
        {"name": "amount", "type": "double"}
      ],
      "expectations": [
        {
          "column": "id",
          "check": "not_null"
        },
        {
          "column": "amount",
          "check": "range",
          "min": 0,
          "max": 10000
        }
      ]
    }
  ]
}
```

### Validation Rules

Available check types in `expectations`:

| Check | Parameters | Description |
|-------|-----------|-------------|
| `not_null` | - | Column must not contain null values |
| `range` | `min`, `max` | Value must be between min and max |
| `compare` | `operator`, `compare_to` | Compare two columns (e.g., `end_time > start_time`) |

---

## 🔧 Running the Framework

### Option 1: Manual Execution (Recommended for Demo)

**Step 1: Generate Data**
```bash
# Generate current hour's data
./run_data_injection.sh

# Generate specific time
./run_data_injection.sh "2025-11-24 10:00:00"
```

**Step 2: Run Validation**
```bash
./run_quick_test.sh
```

**Step 3: View Results**
```bash
open reports/dashboard.html
open allure-report/index.html
cat alerts/alerts.log
```

### Option 2: Automated with Cron

**Setup:**
```bash
./setup_cron.sh
```

**Cron Schedule:**
```
15 * * * * cd /path/to/project && ./run_data_injection.sh
```

Runs hourly at minute 15. Validation can be triggered manually or via Airflow.

### Option 3: Airflow Integration

**Copy DAG to Airflow:**
```bash
cp dags/dq_hourly_pipeline_bash.py ~/airflow/dags/
```

**Trigger DAG:**
```bash
airflow dags trigger dq_hourly_pipeline_bash
```

**DAG Tasks:**
1. `metadata_check` - Validates schema + captures snapshot
2. `validation_check` - Runs data quality checks (if metadata passes)
3. `generate_report` - Creates Allure report
4. `update_dashboard` - Updates HTML dashboard
5. `alert_check` - Checks for quality issues

---

## 📊 Monitoring & Reports

### Dashboard

**Location:** `reports/dashboard.html`

**Features:**
- Summary cards (total tables, avg pass rate, failed checks)
- Time-series chart showing pass rates per table
- Table-level statistics (last run, rows validated, pass rate)
- Auto-refreshes after each validation run

### Allure Report

**Location:** `allure-report/index.html`

**Features:**
- Detailed test execution logs
- Pass/fail per table
- Validation rule breakdown
- Historical trends
- Error stack traces

### Alerts

**Location:** `alerts/alerts.log`

**Format:**
```
2025-11-24 10:15:23 - WARNING - product_inventory: Pass rate 85.0% below threshold (90%)
2025-11-24 10:15:23 - WARNING - sensor_readings: Pass rate 87.5% below threshold (90%)
```

**Threshold:** 90% (configurable in `scripts/alert_handler.py`)

### Database Queries

**Query validation history:**
```bash
sqlite3 results/dq_results.db "
SELECT 
    table_name,
    run_timestamp,
    pass_rate,
    total_rows,
    passed,
    failed
FROM validation_runs 
WHERE test_type='data_validation'
ORDER BY run_timestamp DESC 
LIMIT 10;
"
```

**Check snapshot tracking:**
```bash
sqlite3 results/dq_results.db "
SELECT 
    table_name,
    snapshot_id,
    captured_at
FROM snapshot_tracking
ORDER BY captured_at DESC;
"
```

---

## 🎯 Demo Tables

10 pre-configured tables with realistic schemas:

| Table | Schema | Bad Data | Expected Pass Rate |
|-------|--------|----------|-------------------|
| customer_transactions | id, customer_id, amount, timestamp | No | ~100% |
| user_profiles | user_id, name, email, signup_date | No | ~100% |
| **product_inventory** | product_id, quantity, price | **Yes** | **~85%** |
| order_details | order_id, product_id, quantity, total | No | ~100% |
| web_clickstream | session_id, user_id, event, timestamp | No | ~100% |
| **sensor_readings** | sensor_id, temperature, humidity, timestamp | **Yes** | **~85%** |
| payment_events | payment_id, amount, status, timestamp | No | ~100% |
| email_campaigns | campaign_id, sent, opened, clicked | No | ~100% |
| **support_tickets** | ticket_id, priority, created, resolved | **Yes** | **~85%** |
| api_request_logs | request_id, endpoint, status, duration | No | ~100% |

**Intentional Issues:**
- `product_inventory` - Some quantities exceed max threshold
- `sensor_readings` - Some temperature values out of range
- `support_tickets` - Some resolved_time < created_time

---

## 🐳 Docker Image

**Image:** `dq-runner:latest` (1.58GB)

**Includes:**
- Apache Spark 3.5.0
- Apache Iceberg 1.4.2
- Python 3.10
- Pytest + Allure
- AWS/S3 connectors

**Build:** (if needed)
```bash
docker build -t dq-runner:latest -f infrastructure/Dockerfile .
```

---

## 📦 Dependencies

See `requirements.txt` for complete list.

**Key packages:**
- `pyspark==3.5.0`
- `pytest==7.4.0`
- `allure-pytest==2.13.2`
- `pandas==2.0.3`
- `boto3==1.28.25`

---

## 🔍 Troubleshooting

**Issue: MinIO connection failed**
```bash
# Verify MinIO is accessible
curl http://192.168.1.2:9000/minio/health/live

# Update config/env_config.json with correct endpoint
```

**Issue: Docker image not found**
```bash
# Check if image exists
docker images | grep dq-runner

# Rebuild if missing
docker build -t dq-runner:latest -f infrastructure/Dockerfile .
```

**Issue: No data in tables**
```bash
# Generate initial data
./run_data_injection.sh
```

**Issue: Tests failing**
```bash
# Check logs
cat logs/data_injection.log

# Verify database
sqlite3 results/dq_results.db "SELECT COUNT(*) FROM validation_runs;"
```

---

## 📈 Extending the Framework

### Add New Table

1. Add table definition to `config/schema_registry.json`
2. Run data generation: `./run_data_injection.sh`
3. Run validation: `./run_quick_test.sh`

### Add Custom Validation Check

Edit `tests/test_validation.py`:

```python
def validate_custom_rule(df, table_config):
    # Your custom validation logic
    return df.filter(your_condition)
```

### Change Alert Threshold

Edit `scripts/alert_handler.py`:

```python
ALERT_THRESHOLD = 95.0  # Change from 90% to 95%
```

---

## 📄 Additional Documentation

See `docs/` folder for:
- Implementation details
- Setup guides
- Cleanup history

---

## 📝 Version

**v1.0.0** - Production Ready (November 24, 2025)

---

## 📧 Support

For issues or questions, refer to the documentation in `docs/` or check the generated reports for detailed error logs.

---

**Built with ❤️ for scalable data quality validation**
