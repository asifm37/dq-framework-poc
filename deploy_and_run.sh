#!/bin/bash

set -e  # Exit on error

PROJECT_ROOT="/Users/amohiuddeen/Github/dq-framework-poc"
cd "$PROJECT_ROOT"

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "======================================================================"
echo -e "${BLUE}🚀 DQ Framework - Unified Deployment & Data Injection${NC}"
echo "======================================================================"

# ============================================================================
# 1. Environment Setup
# ============================================================================
echo -e "\n${BLUE}📦 Step 1: Setting up Python environment...${NC}"

if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
    echo -e "${GREEN}✅ Virtual environment created${NC}"
fi

source venv/bin/activate
echo "Installing dependencies..."
pip install --upgrade pip -q
pip install -r requirements.txt -q
echo -e "${GREEN}✅ Dependencies installed${NC}"

# ============================================================================
# 2. MinIO Check
# ============================================================================
echo -e "\n${BLUE}🗄️  Step 2: Checking MinIO availability...${NC}"

MINIO_ENDPOINT="192.168.1.2:9000"
if curl -s --connect-timeout 5 "http://${MINIO_ENDPOINT}/minio/health/live" > /dev/null 2>&1; then
    echo -e "${GREEN}✅ MinIO is running at ${MINIO_ENDPOINT}${NC}"
else
    echo -e "${RED}❌ MinIO is not accessible at ${MINIO_ENDPOINT}${NC}"
    echo -e "${YELLOW}Please ensure MinIO is running before proceeding${NC}"
    exit 1
fi

# ============================================================================
# 3. Docker Image Build
# ============================================================================
echo -e "\n${BLUE}🐳 Step 3: Building Docker image...${NC}"

if docker images | grep -q "dq-runner.*latest"; then
    echo "Docker image 'dq-runner:latest' already exists"
    read -p "Rebuild? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        docker build -t dq-runner:latest -f infrastructure/Dockerfile .
        echo -e "${GREEN}✅ Docker image rebuilt${NC}"
    else
        echo -e "${GREEN}✅ Using existing Docker image${NC}"
    fi
else
    docker build -t dq-runner:latest -f infrastructure/Dockerfile .
    echo -e "${GREEN}✅ Docker image built${NC}"
fi

# ============================================================================
# 4. Data Injection
# ============================================================================
echo -e "\n${BLUE}💉 Step 4: Running data injection...${NC}"

BATCH_TIME=""
if [ "$1" != "" ]; then
    BATCH_TIME="$1"
    echo "Using provided batch time: $BATCH_TIME"
fi

echo "Starting data generation..."
if [ "$BATCH_TIME" != "" ]; then
    ENV=mac_native python3 scripts/generate_hourly_data.py "$BATCH_TIME"
else
    ENV=mac_native python3 scripts/generate_hourly_data.py
fi

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ Data injection successful!${NC}"
else
    echo -e "${RED}❌ Data injection failed!${NC}"
    exit 1
fi

# ============================================================================
# 5. Run Data Quality Tests
# ============================================================================
echo -e "\n${BLUE}🧪 Step 5: Running data quality tests...${NC}"

docker run --rm \
  -v "$PROJECT_ROOT:/app" \
  --add-host host.docker.internal:host-gateway \
  -e ENV=local \
  -e PYTHONPATH=/app \
  dq-runner:latest \
  bash -c "
    echo '📊 Running Metadata Tests...'
    pytest /app/tests/test_metadata.py --alluredir=/app/allure-results --clean-alluredir -v || true
    
    echo ''
    echo '📊 Running Data Validation Tests...'
    pytest /app/tests/test_validation.py --alluredir=/app/allure-results -v || true
    
    echo ''
    echo '📈 Generating Allure Report...'
    allure generate /app/allure-results -o /app/allure-report --clean || echo 'Allure generation skipped'
  "

if [ $? -ne 0 ]; then
    echo -e "${YELLOW}⚠️  Some tests may have failed, but continuing...${NC}"
fi

# ============================================================================
# 6. Generate Dashboard & Alerts
# ============================================================================
echo -e "\n${BLUE}📊 Step 6: Generating dashboard and checking alerts...${NC}"

python3 scripts/dashboard_generator.py
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ Dashboard generated${NC}"
else
    echo -e "${RED}❌ Dashboard generation failed${NC}"
fi

python3 scripts/alert_handler.py
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ Alerts checked${NC}"
else
    echo -e "${RED}❌ Alert checking failed${NC}"
fi

# ============================================================================
# Summary
# ============================================================================
echo ""
echo "======================================================================"
echo -e "${GREEN}✅ Deployment & Data Quality Pipeline Complete!${NC}"
echo "======================================================================"
echo ""
echo "📋 View Results:"
echo "  Dashboard:  open reports/dashboard.html"
echo "  Allure:     open allure-report/index.html"
echo "  Alerts:     cat alerts/alerts.log"
echo ""
echo "📝 Logs:"
echo "  Test logs:  Check docker output above"
echo ""
echo "======================================================================"

