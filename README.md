# DQ Framework POC

Simple data quality framework with automated data injection and validation.

## Quick Start

### 1. Prerequisites
- MinIO running at `192.168.1.2:9000`
- Airflow running (optional)
- Python 3.10+

### 2. Data Injection

```bash
# Check if MinIO is ready
./data_injection.sh check

# Start hourly data injection (cron)
./data_injection.sh start

# Run data injection once
./data_injection.sh run

# Stop data injection
./data_injection.sh stop
```

### 3. Run Tests

```bash
# Build test container (first time only)
docker build -t dq-runner:latest -f infrastructure/Dockerfile .

# Run DQ tests
docker run --rm \
  -v $(pwd):/app \
  --add-host host.docker.internal:host-gateway \
  -e ENV=local -e PYTHONPATH=/app \
  dq-runner:latest \
  pytest /app/tests/ --alluredir=/app/allure-results --clean-alluredir -v

# Generate reports
source venv/bin/activate
python3 scripts/dashboard_generator.py
python3 scripts/alert_handler.py
```

### 4. View Results

```bash
# Dashboard
open reports/dashboard.html

# Alerts
cat alerts/alerts.log

# Data injection log
tail -f logs/data_injection.log
```

## Configuration

Edit `config/env_config.json`:

```json
{
  "mac_native": {
    "s3_endpoint": "http://192.168.1.2:9000",
    "s3_access_key": "admin",
    "s3_secret_key": "password",
    "bucket_name": "warehouse"
  }
}
```

## Project Structure

```
dq-framework-poc/
├── data_injection.sh          # Main script
├── config/                    # Configurations
├── scripts/                   # Python scripts
├── tests/                     # DQ tests
└── infrastructure/            # Docker setup
```

## Troubleshooting

### Check MinIO
```bash
curl http://192.168.1.2:9000/minio/health/live
```

### Check Cron
```bash
crontab -l | grep generate_hourly_data
```

### Clean Up
```bash
rm -rf logs/*.log allure-results/ allure-report/ results/*.db reports/*.html alerts/*.log
```
