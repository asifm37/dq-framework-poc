# DQ Framework POC

## Quick Start

```bash
# Start everything (MinIO, Airflow, Allure, cron job)
./deploy_and_run.sh start

# Run data injection manually
./deploy_and_run.sh inject

# Run DQ tests
./deploy_and_run.sh test

# Stop data injection cron
./deploy_and_run.sh stop
```

## Commands

| Command | Description |
|---------|-------------|
| `./deploy_and_run.sh start` | Start MinIO, Airflow, Allure, setup hourly cron |
| `./deploy_and_run.sh inject` | Run data injection once |
| `./deploy_and_run.sh test` | Run DQ tests, generate reports |
| `./deploy_and_run.sh stop` | Stop data injection cron job |

## Access Points

- **MinIO Console**: http://localhost:9001
- **Allure Report**: http://localhost:8080
- **Dashboard**: `open reports/dashboard.html`
- **Alerts**: `cat alerts/alerts.log`

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
├── deploy_and_run.sh          # Main script
├── config/                    # Configurations
├── scripts/                   # Data generation & reports
├── tests/                     # DQ tests
├── infrastructure/Dockerfile  # Test container
└── venv/                      # Python environment (auto-created)
```

## Data Quality Rules

Configured in `config/schema_registry.json`:
- Metadata validation (existence, schema, freshness)
- Data quality rules per table
- Automatic alerts on failures

## Troubleshooting

### Check Services
```bash
# MinIO
curl http://192.168.1.2:9000/minio/health/live

# Cron jobs
crontab -l
```

### Clean Up
```bash
rm -rf logs/*.log allure-results/ allure-report/ results/*.db reports/*.html alerts/*.log
```
