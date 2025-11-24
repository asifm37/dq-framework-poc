# DQ Framework POC - Clean Slate

## Quick Start

### Prerequisites
- Python 3.10+
- Docker
- MinIO running at `192.168.1.2:9000` (or update `config/env_config.json`)

### Single Command Deployment

Run everything with one command:

```bash
./deploy_and_run.sh
```

This script will:
1. ✅ Set up Python virtual environment
2. ✅ Install all dependencies (isolated from your system)
3. ✅ Check MinIO availability
4. ✅ Build Docker image for testing
5. ✅ Inject data into Iceberg tables
6. ✅ Run data quality tests
7. ✅ Generate Allure reports
8. ✅ Generate dashboard
9. ✅ Check for data quality alerts

### Optional: Specify Batch Time

```bash
./deploy_and_run.sh "2025-11-24T10:00:00"
```

### View Results

After running the script:

- **Dashboard**: `open reports/dashboard.html`
- **Allure Report**: `open allure-report/index.html`
- **Alerts**: `cat alerts/alerts.log`

## Project Structure

```
dq-framework-poc/
├── deploy_and_run.sh          # ⭐ Main deployment script
├── config/
│   ├── env_config.json        # Environment configurations
│   └── schema_registry.json   # Table schemas and rules
├── scripts/
│   ├── generate_hourly_data.py    # Data generation
│   ├── dashboard_generator.py     # Dashboard creation
│   ├── alert_handler.py           # Alert processing
│   └── results_tracker.py         # Results tracking
├── tests/
│   ├── conftest.py            # Pytest configuration
│   ├── test_metadata.py       # Metadata validation tests
│   └── test_validation.py     # Data quality tests
├── infrastructure/
│   └── Dockerfile             # Docker image for tests
├── dags/
│   └── dq_hourly_pipeline_bash.py  # Airflow DAG (optional)
└── venv/                      # Virtual environment (auto-created)
```

## Environment Configuration

Edit `config/env_config.json` to configure your environment:

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

## Data Quality Rules

Configured in `config/schema_registry.json`:

- **Metadata Tests**: Verify table existence, schema, freshness
- **Data Validation Tests**: Check data quality rules per table
- **Alerts**: Automatically triggered on failures

## Clean Up

To start fresh again:

```bash
# Remove all generated files
rm -rf logs/*.log allure-results/ allure-report/ results/dq_results.db reports/dashboard.html alerts/*.log

# Stop services (if running in Docker)
docker ps -a | grep dq-runner | awk '{print $1}' | xargs docker stop
```

## Troubleshooting

### MinIO Connection Issues

Ensure MinIO is running and accessible:

```bash
curl http://192.168.1.2:9000/minio/health/live
```

### Virtual Environment Issues

If venv has issues, recreate it:

```bash
rm -rf venv
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Docker Issues

Rebuild the image:

```bash
docker build -t dq-runner:latest -f infrastructure/Dockerfile .
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     deploy_and_run.sh                       │
│                  (Single Entry Point)                       │
└──────────────┬──────────────────────────────────────────────┘
               │
       ┌───────┴────────┐
       │                │
       ▼                ▼
┌─────────────┐  ┌─────────────────┐
│   Python    │  │  Docker Build   │
│    venv     │  │  dq-runner      │
└──────┬──────┘  └────────┬────────┘
       │                  │
       ▼                  ▼
┌──────────────┐   ┌──────────────┐
│ Data         │   │ DQ Tests     │
│ Injection    │──▶│ (Pytest)     │
│ (PySpark)    │   │              │
└──────┬───────┘   └──────┬───────┘
       │                  │
       ▼                  ▼
┌──────────────┐   ┌──────────────┐
│   MinIO      │   │  Results     │
│  (Iceberg)   │   │  Dashboard   │
└──────────────┘   └──────────────┘
```
