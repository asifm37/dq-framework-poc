#!/bin/bash
set -e # Exit immediately if any command fails
unset KUBECONFIG

# --- Configuration ---
PROJECT_DIR=$(pwd)
AIRFLOW_DAGS_DIR=~/airflow/dags
K8S_CONTEXT="docker-desktop"
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}[1/6] Pre-flight Checks...${NC}"

# Check if we are in the right folder
if [ ! -f "infrastructure/k8s-minio.yaml" ]; then
    echo -e "${RED}Error: Please run this script from the project root directory.${NC}"
    exit 1
fi

# Switch/Check Kubernetes Context
CURRENT_CTX=$(kubectl config current-context)
if [ "$CURRENT_CTX" != "$K8S_CONTEXT" ]; then
    echo "Current context is '$CURRENT_CTX'. Switching to '$K8S_CONTEXT'..."
    kubectl config use-context $K8S_CONTEXT || {
        echo -e "${RED}Error: Could not switch to context '$K8S_CONTEXT'. Is Docker Desktop K8s enabled?${NC}"
        exit 1
    }
fi

echo -e "${GREEN}[2/6] Deploying MinIO (Storage)...${NC}"
kubectl apply -f infrastructure/k8s-minio.yaml

# Wait for MinIO to actually be ready
echo "Waiting for MinIO pod to be ready..."
kubectl rollout status deployment/minio --timeout=60s
# Give it a tiny buffer for the API to start
sleep 5

echo -e "${GREEN}[3/6] Building Runner Image (Spark + Pytest)...${NC}"
# Use the local host architecture path we fixed earlier
docker build -t dq-runner:latest -f infrastructure/Dockerfile .

echo -e "${GREEN}[4/6] Seeding Initial Data...${NC}"
# Check if seeder already exists to avoid error
kubectl delete pod seeder --ignore-not-found=true

# Run the seeder pod (Added imagePullPolicy: Never)
kubectl run seeder --image=dq-runner:latest --restart=Never \
  --overrides='{"spec": {"containers": [{"name": "seeder", "image": "dq-runner:latest", "imagePullPolicy": "Never", "volumeMounts": [{"mountPath": "/app", "name": "vol"}], "env": [{"name": "S3_ENDPOINT", "value": "http://minio:9000"}, {"name": "PYTHONPATH", "value": "/app"}]}], "volumes": [{"name": "vol", "hostPath": {"path": "'$PROJECT_DIR'"}}]}}' \
  --command -- python /app/scripts/seed_data.py


# Wait for seeding to finish
echo "Waiting for data generation to complete..."
kubectl wait --for=condition=ready pod/seeder --timeout=30s || true
kubectl logs seeder
# Cleanup seeder
kubectl delete pod seeder

echo -e "${GREEN}[5/6] Linking DAG to Airflow...${NC}"
# Ensure target dir exists
mkdir -p $AIRFLOW_DAGS_DIR

# Remove old link if exists to avoid 'file exists' error
rm -f $AIRFLOW_DAGS_DIR/dq_k8s_dag.py
ln -s $PROJECT_DIR/dags/dq_k8s_dag.py $AIRFLOW_DAGS_DIR/dq_k8s_dag.py

echo -e "${GREEN}[6/6] Deployment Complete!${NC}"
echo "--------------------------------------------------------"
echo "1. Ensure Airflow is running:  airflow standalone"
echo "2. Open Airflow UI:            http://localhost:8080"
echo "3. Trigger DAG:                dq_framework_v1"
echo "4. View Reports (after run):   cd allure-report && python3 -m http.server 9999"
echo "--------------------------------------------------------"

