#!/bin/bash

PROJECT_ROOT="/Users/amohiuddeen/Github/dq-framework-poc"
cd "$PROJECT_ROOT"

# Setup venv if needed
if [ ! -d "venv" ]; then
    echo "Creating Python environment..."
    python3 -m venv venv
    source venv/bin/activate
    pip install -q --upgrade pip
    pip install -q -r requirements.txt
    echo "✓ Environment ready"
else
    source venv/bin/activate
fi

# Check deployments
check_deployments() {
    echo "Checking deployments..."
    
    # Check MinIO
    if curl -s --connect-timeout 3 "http://localhost:9000/minio/health/live" > /dev/null 2>&1; then
        echo "✓ MinIO is running"
    else
        echo "✗ MinIO is NOT running at localhost:9000"
        return 1
    fi
    
    echo "✓ All deployments ready"
    return 0
}

# Start data injection cron
start_injection() {
    if ! check_deployments; then
        echo ""
        echo "Please start MinIO first, then run this command again."
        exit 1
    fi
    
    CRON_JOB="0 * * * * cd $PROJECT_ROOT && ENV=mac_native $PROJECT_ROOT/venv/bin/python3 $PROJECT_ROOT/scripts/generate_hourly_data.py >> $PROJECT_ROOT/logs/data_injection.log 2>&1"
    
    if crontab -l 2>/dev/null | grep -q "generate_hourly_data.py"; then
        echo "✓ Data injection already running (hourly)"
    else
        (crontab -l 2>/dev/null; echo "$CRON_JOB") | crontab -
        echo "✓ Data injection started (runs every hour)"
        echo "  Log: tail -f logs/data_injection.log"
    fi
}

# Stop data injection cron
stop_injection() {
    if crontab -l 2>/dev/null | grep -q "generate_hourly_data.py"; then
        crontab -l 2>/dev/null | grep -v "generate_hourly_data.py" | crontab -
        echo "✓ Data injection stopped"
    else
        echo "✓ Data injection not running"
    fi
}

# Run injection once
run_once() {
    if ! check_deployments; then
        echo ""
        echo "Please start MinIO first, then run this command again."
        exit 1
    fi
    
    echo "Running data injection once..."
    ENV=mac_native python3 scripts/generate_hourly_data.py
}

# Main
case "$1" in
    start)
        start_injection
        ;;
    stop)
        stop_injection
        ;;
    run)
        run_once
        ;;
    check)
        check_deployments
        ;;
    *)
        echo "Usage: $0 {start|stop|run|check}"
        echo ""
        echo "  check  - Check if MinIO is running"
        echo "  start  - Start hourly data injection (cron)"
        echo "  stop   - Stop data injection"
        echo "  run    - Run data injection once now"
        exit 1
        ;;
esac

