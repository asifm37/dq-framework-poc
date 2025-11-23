#!/bin/bash
# Quick test runner for local testing (bypasses Airflow)

PROJECT_ROOT="/Users/amohiuddeen/Github/dq-framework-poc"
cd "$PROJECT_ROOT"

echo "======================================================================"
echo "🧪 Quick Test Runner (Local)"
echo "======================================================================"

# Run in Docker
docker run --rm \
  -v "$PROJECT_ROOT:/app" \
  --add-host host.docker.internal:host-gateway \
  -e ENV=local \
  -e PYTHONPATH=/app \
  dq-runner:latest \
  bash -c "
    echo '1️⃣  Running Metadata Tests...'
    pytest /app/tests/test_metadata.py --alluredir=/app/allure-results --clean-alluredir -v
    
    echo ''
    echo '2️⃣  Running Data Validation Tests...'
    pytest /app/tests/test_validation.py --alluredir=/app/allure-results -v
    
    echo ''
    echo '3️⃣  Generating Allure Report...'
    allure generate /app/allure-results -o /app/allure-report --clean
  "

echo ""
echo "4️⃣  Generating Dashboard..."
python3 scripts/dashboard_generator.py

echo ""
echo "5️⃣  Checking Alerts..."
python3 scripts/alert_handler.py

echo ""
echo "======================================================================"
echo "✅ Tests Complete!"
echo "======================================================================"
echo "View results:"
echo "  Dashboard: open reports/dashboard.html"
echo "  Allure:    open allure-report/index.html"
echo "  Alerts:    cat alerts/alerts.log"
echo "======================================================================"

