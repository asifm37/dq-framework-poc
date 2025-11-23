#!/bin/bash
set -e

export JAVA_HOME

# 1. Install Docker Provider for Airflow
echo "Installing Docker Provider..."
source venv/bin/activate
pip install apache-airflow-providers-docker s3fs pyspark==3.5.0

# 2. Build the Runner Image
echo "Building Docker Image..."
docker build -t dq-runner:latest -f infrastructure/Dockerfile .

# 3. Seed Data (Run Locally)
echo "Seeding Data..."
python scripts/seed_data.py

# 4. Link DAG
echo "Linking DAG..."
ln -sf $(pwd)/dags/dq_production_dag.py ~/airflow/dags/dq_production_dag.py

echo "✅ Deployment Complete. Go to Airflow UI!"

