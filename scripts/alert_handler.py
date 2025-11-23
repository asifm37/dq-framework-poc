"""
Alert Handler - Monitors validation results and sends alerts
"""
import sys
sys.path.insert(0, '/Users/amohiuddeen/Github/dq-framework-poc')

from scripts.results_tracker import get_recent_results, get_table_summary, init_database
from datetime import datetime
import os

ALERT_LOG_PATH = "/Users/amohiuddeen/Github/dq-framework-poc/alerts/alerts.log"
ALERT_THRESHOLD = 90.0  # Alert if pass rate < 90%


def ensure_alert_dir():
    """Ensure alerts directory exists"""
    os.makedirs(os.path.dirname(ALERT_LOG_PATH), exist_ok=True)


def write_alert(message, level="WARNING"):
    """Write alert to log file"""
    ensure_alert_dir()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] [{level}] {message}\n"
    
    with open(ALERT_LOG_PATH, 'a') as f:
        f.write(log_entry)
    
    print(log_entry.strip())


def check_validation_results():
    """Check recent validation results and alert if needed"""
    init_database()
    
    print("=" * 70)
    print("🚨 ALERT CHECKER")
    print("=" * 70)
    
    # Get recent results from last run (last hour)
    recent = get_recent_results(hours=2)
    
    if not recent:
        write_alert("No recent validation results found", level="INFO")
        return
    
    # Get latest run timestamp
    latest_run = recent[0]['run_timestamp']
    latest_run_id = recent[0].get('airflow_run_id', 'unknown')
    
    # Filter results from latest run only
    latest_results = [r for r in recent if r['run_timestamp'] == latest_run]
    
    print(f"\n📊 Analyzing {len(latest_results)} results from run: {latest_run}")
    print(f"   Run ID: {latest_run_id}")
    
    # Check for metadata failures
    metadata_failures = [r for r in latest_results if r['test_type'] == 'metadata' and r['status'] == 'fail']
    
    if metadata_failures:
        for failure in metadata_failures:
            alert_msg = (
                f"METADATA FAILURE - Table: {failure['table_name']} | "
                f"Details: {failure['details']}"
            )
            write_alert(alert_msg, level="CRITICAL")
    
    # Check for data validation failures (pass rate < 90%)
    validation_results = [r for r in latest_results if r['test_type'] == 'data_validation']
    
    alerts_triggered = 0
    
    for result in validation_results:
        pass_rate = result['pass_rate'] or 0
        
        if pass_rate < ALERT_THRESHOLD:
            alert_msg = (
                f"DATA QUALITY ALERT - Table: {result['table_name']} | "
                f"Pass Rate: {pass_rate:.2f}% (Threshold: {ALERT_THRESHOLD}%) | "
                f"Rows: {result['row_count']} | "
                f"Failed: {result['fail_count']}"
            )
            write_alert(alert_msg, level="WARNING")
            alerts_triggered += 1
    
    # Summary
    if alerts_triggered > 0:
        summary = f"ALERT SUMMARY: {alerts_triggered} table(s) below quality threshold"
        write_alert(summary, level="WARNING")
        print(f"\n⚠️  {alerts_triggered} alert(s) triggered")
    else:
        success_msg = f"All validations passed quality threshold (>= {ALERT_THRESHOLD}%)"
        write_alert(success_msg, level="INFO")
        print(f"\n✅ All tables healthy")
    
    print("=" * 70)


def get_alert_summary(hours=24):
    """Get summary of alerts in the last N hours"""
    ensure_alert_dir()
    
    if not os.path.exists(ALERT_LOG_PATH):
        return "No alerts logged"
    
    with open(ALERT_LOG_PATH, 'r') as f:
        lines = f.readlines()
    
    # Parse last N hours of alerts
    cutoff_time = datetime.now().timestamp() - (hours * 3600)
    recent_alerts = []
    
    for line in lines:
        try:
            # Extract timestamp from log entry
            timestamp_str = line.split(']')[0].replace('[', '')
            timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
            
            if timestamp.timestamp() >= cutoff_time:
                recent_alerts.append(line.strip())
        except:
            continue
    
    if not recent_alerts:
        return f"No alerts in the last {hours} hours"
    
    return "\n".join(recent_alerts)


if __name__ == "__main__":
    check_validation_results()

