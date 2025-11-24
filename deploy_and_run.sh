#!/bin/bash

PROJECT_ROOT="/Users/amohiuddeen/Github/dq-framework-poc"
cd "$PROJECT_ROOT"

# Function to start MinIO
start_minio() {
    if pgrep -x "minio" > /dev/null; then
        echo "✓ MinIO already running"
    else
        echo "Starting MinIO..."
        nohup minio server ~/minio-data --console-address ":9001" > logs/minio.log 2>&1 &
        sleep 2
        echo "✓ MinIO started"
    fi
}

# Function to start Airflow
start_airflow() {
    if [ -f "airflow_start.sh" ]; then
        echo "Starting Airflow..."
        ./airflow_start.sh restart > /dev/null 2>&1
        echo "✓ Airflow started"
    else
        echo "⚠ airflow_start.sh not found, skipping"
    fi
}

# Function to start Allure
start_allure() {
    if pgrep -f "allure.*open" > /dev/null; then
        echo "✓ Allure already running"
    elif [ -d "allure-report" ]; then
        echo "Starting Allure..."
        nohup allure open allure-report -p 8080 > logs/allure.log 2>&1 &
        echo "✓ Allure started on http://localhost:8080"
    else
        echo "⚠ No allure-report found, run tests first"
    fi
}

# Function to setup cron for data injection
setup_cron() {
    CRON_JOB="0 * * * * cd $PROJECT_ROOT && ENV=mac_native $PROJECT_ROOT/venv/bin/python3 $PROJECT_ROOT/scripts/generate_hourly_data.py >> $PROJECT_ROOT/logs/data_injection.log 2>&1"
    
    if crontab -l 2>/dev/null | grep -q "generate_hourly_data.py"; then
        echo "✓ Data injection cron already configured"
    else
        echo "Setting up hourly data injection..."
        (crontab -l 2>/dev/null; echo "$CRON_JOB") | crontab -
        echo "✓ Cron job added (runs every hour)"
    fi
}

# Function to stop data injection cron
stop_cron() {
    if crontab -l 2>/dev/null | grep -q "generate_hourly_data.py"; then
        crontab -l 2>/dev/null | grep -v "generate_hourly_data.py" | crontab -
        echo "✓ Data injection cron removed"
    else
        echo "✓ No data injection cron found"
    fi
}

# Setup virtual environment if needed
if [ ! -d "venv" ]; then
    echo "Setting up Python environment..."
    python3 -m venv venv
    source venv/bin/activate
    pip install -q --upgrade pip
    pip install -q -r requirements.txt
    echo "✓ Environment ready"
else
    source venv/bin/activate
fi

# Main menu
case "$1" in
    start)
        echo "=== Starting DQ Framework ==="
        start_minio
        start_airflow
        start_allure
        setup_cron
        echo ""
        echo "Dashboard: open reports/dashboard.html"
        echo "Allure: http://localhost:8080"
        echo "MinIO Console: http://localhost:9001"
        ;;
    stop)
        echo "=== Stopping Data Injection ==="
        stop_cron
        ;;
    inject)
        echo "Running data injection..."
        ENV=mac_native python3 scripts/generate_hourly_data.py
        ;;
    test)
        echo "Running DQ tests..."
        docker run --rm \
          -v "$PROJECT_ROOT:/app" \
          --add-host host.docker.internal:host-gateway \
          -e ENV=local -e PYTHONPATH=/app \
          dq-runner:latest \
          bash -c "pytest /app/tests/ --alluredir=/app/allure-results --clean-alluredir -v && \
                   allure generate /app/allure-results -o /app/allure-report --clean"
        python3 scripts/dashboard_generator.py
        python3 scripts/alert_handler.py
        echo "✓ Tests complete"
        ;;
    *)
        echo "Usage: $0 {start|stop|inject|test}"
        echo ""
        echo "  start  - Start MinIO, Airflow, Allure, and cron job"
        echo "  stop   - Stop data injection cron job"
        echo "  inject - Run data injection once"
        echo "  test   - Run DQ tests and generate reports"
        exit 1
        ;;
esac
