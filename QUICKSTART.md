# 🚀 Quick Start Commands

## Initial Setup (One Time)
```bash
cd /Users/amohiuddeen/Github/dq-framework-poc
./setup_local.sh
```

## Daily Operations

### 1. Generate Data (Run hourly or via cron)
```bash
# Current hour
./run_data_injection.sh

# Specific time
./run_data_injection.sh "2024-11-23T10:00:00"
```

### 2. Run Tests via Airflow
```bash
# Start Airflow
airflow_start.sh restart

# Trigger DAG
airflow dags trigger dq_hourly_pipeline

# Check status
airflow dags list-runs -d dq_hourly_pipeline
```

### 3. Quick Test (Bypass Airflow)
```bash
./run_quick_test.sh
```

### 4. View Results
```bash
# Dashboard
open reports/dashboard.html

# Allure Report
open allure-report/index.html

# Alerts
cat alerts/alerts.log
tail -f alerts/alerts.log  # watch in real-time
```

## Monitoring
```bash
# Airflow UI
open http://localhost:8080

# MinIO UI
open http://192.168.1.2:9001/browser/warehouse

# Database stats
sqlite3 results/dq_results.db "SELECT * FROM validation_runs ORDER BY run_timestamp DESC LIMIT 10;"
```

## Troubleshooting
```bash
# Rebuild Docker
docker build -t dq-runner:latest -f infrastructure/Dockerfile .

# Reset database
rm results/dq_results.db
python3 scripts/results_tracker.py

# Clear alerts
> alerts/alerts.log

# Airflow logs
tail -f /Users/amohiuddeen/airflow/logs/scheduler/latest/*.log
```

## Configuration Files

- Tables & validation rules: `config/schema_registry.json`
- MinIO connection: `config/env_config.json`
- Alert threshold: `scripts/alert_handler.py` (line 10)
- DAG schedule: `dags/dq_hourly_pipeline.py` (line 17)

## Key Metrics

- **10 tables** configured
- **100 rows/table/hour**
- **3 tables** with intentional bad data (85% pass rate)
- **Alert threshold:** 90%
- **Validation:** Incremental (Iceberg snapshots)

---

**For detailed documentation, see:** `SETUP_GUIDE.md`

