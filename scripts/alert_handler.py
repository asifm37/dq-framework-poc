import sys
sys.path.insert(0, '/Users/amohiuddeen/Github/dq-framework-poc')

from scripts.results_tracker import get_recent_results, init_database
from datetime import datetime
import os

ALERT_LOG_PATH = "/Users/amohiuddeen/Github/dq-framework-poc/alerts/alerts.log"
ALERT_THRESHOLD = 90.0


def write_alert(message, level="WARNING"):
    os.makedirs(os.path.dirname(ALERT_LOG_PATH), exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] [{level}] {message}\n"
    
    with open(ALERT_LOG_PATH, 'a') as f:
        f.write(log_entry)
    
    print(log_entry.strip())


def check_validation_results():
    init_database()
    
    print("=" * 70)
    print("🚨 ALERT CHECKER")
    print("=" * 70)
    
    recent = get_recent_results(hours=2)
    
    if not recent:
        write_alert("No recent validation results found", level="INFO")
        return
    
    latest_run = recent[0]['run_timestamp']
    latest_run_id = recent[0].get('airflow_run_id', 'unknown')
    
    latest_results = [r for r in recent if r['run_timestamp'] == latest_run]
    
    print(f"\n📊 Analyzing {len(latest_results)} results from run: {latest_run}")
    print(f"   Run ID: {latest_run_id}")
    
    metadata_failures = [r for r in latest_results if r['test_type'] == 'metadata' and r['status'] == 'fail']
    
    if metadata_failures:
        for failure in metadata_failures:
            alert_msg = (
                f"METADATA FAILURE - Table: {failure['table_name']} | "
                f"Details: {failure['details']}"
            )
            write_alert(alert_msg, level="CRITICAL")
    
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
    
    if alerts_triggered > 0:
        summary = f"ALERT SUMMARY: {alerts_triggered} table(s) below quality threshold"
        write_alert(summary, level="WARNING")
        print(f"\n⚠️  {alerts_triggered} alert(s) triggered")
    else:
        success_msg = f"All validations passed quality threshold (>= {ALERT_THRESHOLD}%)"
        write_alert(success_msg, level="INFO")
        print(f"\n✅ All tables healthy")
    
    print("=" * 70)


if __name__ == "__main__":
    check_validation_results()
