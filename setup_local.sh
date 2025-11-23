#!/bin/bash
# Setup script for local DQ framework

set -e

echo "======================================================================"
echo "🚀 Data Quality Framework - Local Setup"
echo "======================================================================"

PROJECT_ROOT="/Users/amohiuddeen/Github/dq-framework-poc"
cd "$PROJECT_ROOT"

# 1. Create necessary directories
echo ""
echo "📁 Creating directories..."
mkdir -p results alerts reports allure-results allure-report

# 2. Initialize SQLite database
echo ""
echo "🗄️  Initializing results database..."
python3 scripts/results_tracker.py

# 3. Build Docker image
echo ""
echo "🐳 Building Docker image..."
docker build -t dq-runner:latest -f infrastructure/Dockerfile .

# 4. Copy DAG to Airflow
echo ""
echo "📋 Copying DAG to Airflow..."
AIRFLOW_HOME="/Users/amohiuddeen/airflow"
mkdir -p "$AIRFLOW_HOME/dags"
cp dags/dq_hourly_pipeline.py "$AIRFLOW_HOME/dags/"

echo ""
echo "======================================================================"
echo "✅ Setup Complete!"
echo "======================================================================"
echo ""
echo "Next steps:"
echo "  1. Generate initial data:"
echo "     ./run_data_injection.sh"
echo ""
echo "  2. Start Airflow (if not running):"
echo "     airflow_start.sh restart"
echo ""
echo "  3. Trigger DAG manually or wait for hourly schedule:"
echo "     airflow dags trigger dq_hourly_pipeline"
echo ""
echo "  4. View dashboard:"
echo "     open reports/dashboard.html"
echo ""
echo "  5. View Allure report:"
echo "     open allure-report/index.html"
echo ""
echo "  6. View alerts:"
echo "     cat alerts/alerts.log"
echo "======================================================================"

