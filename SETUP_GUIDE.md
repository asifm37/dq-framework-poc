# 🛡️ Data Quality Framework - Setup Guide

## 📋 Overview

This framework implements an **Iceberg snapshot-based** data quality validation system with:
- ✅ 10 append-only tables with synthetic data
- ✅ Hourly automated testing via Airflow
- ✅ Circuit breaker pattern (metadata → validation)
- ✅ **Incremental validation** (only new data per batch)
- ✅ Dynamic reporting dashboard
- ✅ File-based alerting (<90% pass rate)

---

## 🏗️ Architecture

```
Data Generation (Independent)
    ↓ Writes to MinIO/Iceberg
    ↓ Creates new snapshot per write
    ↓
Airflow DAG (@hourly)
    ├─ Metadata Tests → Check schema
    ├─ Data Validation → Incremental (snapshot-based)
    ├─ Allure Report → Detailed test results
    ├─ Dashboard → Time-series charts
    └─ Alerting → File-based logs
```

### Key Innovation: **Iceberg Snapshot Tracking**

- No watermark columns needed
- Schema-agnostic validation
- Reads only NEW data since last validation
- Tracks via SQLite: `table_name → last_validated_snapshot_id`

---

## 🚀 Quick Start

### 1. Prerequisites

```bash
# Verify MinIO is running
curl http://192.168.1.2:9000/minio/health/live

# Verify Airflow is accessible
export AIRFLOW_HOME=/Users/amohiuddeen/airflow
```

### 2. Initial Setup

```bash
cd /Users/amohiuddeen/Github/dq-framework-poc

# Run setup (creates dirs, builds Docker, copies DAG)
./setup_local.sh
```

### 3. Generate Initial Data

```bash
# Generate data for all 10 tables (current hour)
./run_data_injection.sh

# Or specify a batch time
./run_data_injection.sh "2024-11-23T10:00:00"
```

### 4. Run Validation

**Option A: Via Airflow (Production)**
```bash
# Ensure Airflow is running
airflow_start.sh restart

# Trigger DAG manually
airflow dags trigger dq_hourly_pipeline

# Or wait for @hourly schedule
```

**Option B: Quick Test (Bypass Airflow)**
```bash
./run_quick_test.sh
```

### 5. View Results

```bash
# Dashboard (time-series charts)
open reports/dashboard.html

# Allure Report (detailed test results)
open allure-report/index.html

# Alerts Log
cat alerts/alerts.log
```

---

## 📊 10 Tables Configuration

| Table | Description | Bad Data? | Pass Rate |
|-------|-------------|-----------|-----------|
| `customer_transactions` | Financial transactions | 5% violations | ~95% |
| `user_profiles` | User registrations | No | 100% |
| `product_inventory` | Inventory changes | **15% violations** | **~85%** ⚠️ |
| `order_details` | Order line items | No | 100% |
| `web_clickstream` | Click events | No | 100% |
| `sensor_readings` | IoT sensor data | **15% violations** | **~85%** ⚠️ |
| `payment_events` | Payment processing | No | 100% |
| `email_campaigns` | Email campaign stats | No | 100% |
| `support_tickets` | Support tickets | **15% violations** | **~85%** ⚠️ |
| `api_request_logs` | API request logs | No | 100% |

**Note:** 3 tables intentionally have bad data to test alerting (<90% threshold).

---

## 📁 Project Structure

```
dq-framework-poc/
├── config/
│   ├── env_config.json          # MinIO endpoints
│   └── schema_registry.json     # 10 table schemas + rules
├── dags/
│   └── dq_hourly_pipeline.py    # Main Airflow DAG
├── scripts/
│   ├── generate_hourly_data.py  # Data generator
│   ├── results_tracker.py       # SQLite tracking
│   ├── dashboard_generator.py   # HTML dashboard
│   └── alert_handler.py         # Alert checker
├── tests/
│   ├── test_metadata.py         # Schema validation
│   └── test_validation.py       # Data quality checks
├── results/
│   └── dq_results.db            # SQLite database
├── alerts/
│   └── alerts.log               # Alert log file
├── reports/
│   └── dashboard.html           # Time-series dashboard
├── allure-report/               # Detailed test reports
├── setup_local.sh               # Setup script
├── run_data_injection.sh        # Data generator script
└── run_quick_test.sh            # Quick test runner
```

---

## 🔄 Hourly Workflow

### Data Injection (Manual/Cron)

```bash
# Run every hour (via cron or manual)
./run_data_injection.sh
```

**What it does:**
1. Generates 100 rows per table
2. Writes to Iceberg (creates new snapshot)
3. 3 tables have intentional bad data (~15%)

### Airflow DAG (@hourly)

**Task Flow:**
```
1. Metadata Check
   ↓ (if pass)
2. Data Validation (incremental snapshot-based)
   ↓ (always)
3. Allure Report
   ↓ (always)
4. Dashboard Update
   ↓ (always)
5. Alert Checker
```

**Circuit Breaker Logic:**
- If metadata fails → Skip validation → Report failure → Alert
- If validation fails → Still generate reports → Alert

---

## 🎯 Iceberg Snapshot Validation

### How It Works

```python
# First run (no previous snapshot)
df = spark.read.format("iceberg").load("local.db.customer_transactions")
# Validates ALL existing data

# Subsequent runs (incremental)
last_snapshot = get_last_validated_snapshot("customer_transactions")  # e.g., 5432199123
current_snapshot = 5432199456  # Latest from Iceberg metadata

df = spark.read.format("iceberg") \
    .option("start-snapshot-id", last_snapshot) \
    .option("end-snapshot-id", current_snapshot) \
    .load("local.db.customer_transactions")
# Validates ONLY new data since last validation
```

### Benefits

✅ **No watermark columns** required  
✅ **Schema-agnostic** (works with any table structure)  
✅ **Efficient** (reads only new Parquet files)  
✅ **Reliable** (Iceberg ACID guarantees)  
✅ **Replayable** (can revalidate any historical snapshot)

---

## 📈 Dashboard Features

- **Stats Cards:** Total tables, avg pass rate, health status
- **Time-Series Chart:** Hourly pass rates per table (Chart.js)
- **Table Summary:** Avg/min/max pass rates, success counts
- **Recent Runs:** Last 50 validation runs

**Auto-refreshes** after each Airflow run.

---

## 🚨 Alerting

### Thresholds

- **Critical:** Metadata failure → Table unreachable
- **Warning:** Pass rate < 90% → Data quality issue

### Alert Channels

Currently: **File-based** (`alerts/alerts.log`)

**Easy to extend to:**
- Slack webhook
- Email (SMTP)
- PagerDuty
- Custom webhooks

Example alert:
```
[2024-11-23 10:30:15] [WARNING] DATA QUALITY ALERT - Table: sensor_readings | 
Pass Rate: 85.00% (Threshold: 90.00%) | Rows: 100 | Failed: 15
```

---

## 🔧 Configuration

### Adjust Number of Tables

Edit `config/schema_registry.json`:
```json
{
  "config": {
    "num_tables": 10,        // Change to 20, 50, etc.
    "rows_per_batch": 100,   // Rows per table per hour
    "table_type": "append_only"
  }
}
```

### Adjust Alert Threshold

Edit `scripts/alert_handler.py`:
```python
ALERT_THRESHOLD = 90.0  # Change to 80, 95, etc.
```

### Adjust DAG Schedule

Edit `dags/dq_hourly_pipeline.py`:
```python
schedule_interval='@hourly'  # Change to '@daily', '*/15 * * * *', etc.
```

---

## 🛠️ Troubleshooting

### Docker Image Not Found

```bash
cd /Users/amohiuddeen/Github/dq-framework-poc
docker build -t dq-runner:latest -f infrastructure/Dockerfile .
```

### MinIO Connection Failed

```bash
# Test MinIO connectivity
curl http://192.168.1.2:9000/minio/health/live

# Verify credentials in config/env_config.json
```

### Airflow DAG Not Showing

```bash
# Copy DAG to Airflow
cp dags/dq_hourly_pipeline.py /Users/amohiuddeen/airflow/dags/

# Restart Airflow
airflow_start.sh restart
```

### Tests Failing on First Run

**Expected:** First run validates all existing data, not just new data.  
**Solution:** Generate data first, then run tests.

---

## 📚 Next Steps

### Production Enhancements

1. **Automated Data Injection:** Add cron job for hourly data generation
2. **Slack Alerts:** Integrate webhook in `alert_handler.py`
3. **Scale Tables:** Add more tables to schema registry
4. **Kubernetes:** Use `dq_k8s_dag.py` for isolated compute
5. **Real Data:** Replace synthetic generators with real ETL

### Monitoring

```bash
# Watch Airflow logs
tail -f /Users/amohiuddeen/airflow/logs/scheduler/latest/*.log

# Watch alerts in real-time
tail -f /Users/amohiuddeen/Github/dq-framework-poc/alerts/alerts.log
```

---

## 🎓 Key Concepts

### Circuit Breaker Pattern

Metadata test failure → Skip expensive data validation → Report immediately

### Incremental Validation

Only validate new data since last successful validation (via Iceberg snapshots)

### Append-Only Tables

Each hour adds new rows, historical data immutable → Perfect for Iceberg

---

## 📞 Support

For issues or questions, check:
- Airflow logs: `/Users/amohiuddeen/airflow/logs/`
- Alert logs: `alerts/alerts.log`
- Test results: `allure-report/index.html`

---

**Happy Data Quality Testing! 🚀**

