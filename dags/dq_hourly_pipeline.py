"""
Data Quality Hourly Pipeline - Main Production DAG
Runs metadata checks -> data validation -> reporting -> alerting
"""
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
import os

# Project path
HOST_PATH = "/Users/amohiuddeen/Github/dq-framework-poc"

default_args = {
    'owner': 'data_eng',
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
    'catchup': False
}

with DAG(
    'dq_hourly_pipeline',
    default_args=default_args,
    schedule_interval='@hourly',
    description='Hourly data quality validation pipeline',
    catchup=False,
    tags=['data-quality', 'production']
) as dag:

    # Common Docker configuration
    docker_config = {
        "image": "dq-runner:latest",
        "api_version": "auto",
        "auto_remove": True,
        "mounts": [
            {
                "source": HOST_PATH,
                "target": "/app",
                "type": "bind"
            }
        ],
        "docker_url": "unix://var/run/docker.sock",
        "network_mode": "bridge",
        "extra_hosts": {"host.docker.internal": "host-gateway"},
        "environment": {
            "ENV": "local",
            "PYTHONPATH": "/app",
            "AIRFLOW_RUN_ID": "{{ run_id }}"
        }
    }

    # =================================================================
    # TASK 1: Metadata Check
    # =================================================================
    metadata_check = DockerOperator(
        task_id="metadata_check",
        command="pytest /app/tests/test_metadata.py --alluredir=/app/allure-results --clean-alluredir -v",
        **docker_config
    )

    # =================================================================
    # TASK 2: Data Validation (runs ONLY if metadata passes)
    # =================================================================
    data_validation = DockerOperator(
        task_id="data_validation",
        command="pytest /app/tests/test_validation.py --alluredir=/app/allure-results -v",
        trigger_rule=TriggerRule.ALL_SUCCESS,  # Only run if metadata passed
        **docker_config
    )

    # =================================================================
    # TASK 3: Generate Allure Report (always runs)
    # =================================================================
    generate_allure_report = BashOperator(
        task_id="generate_allure_report",
        trigger_rule=TriggerRule.ALL_DONE,  # Run regardless of test results
        bash_command=f"""
            cd {HOST_PATH}
            if [ -d "allure-results" ] && [ "$(ls -A allure-results)" ]; then
                allure generate allure-results -o allure-report --clean || echo "Allure generation failed"
            else
                echo "No allure-results directory or empty results"
            fi
        """
    )

    # =================================================================
    # TASK 4: Generate Dashboard (always runs)
    # =================================================================
    def generate_dashboard_task():
        import sys
        sys.path.insert(0, HOST_PATH)
        from scripts.dashboard_generator import generate_dashboard
        generate_dashboard()

    generate_dashboard = PythonOperator(
        task_id="generate_dashboard",
        python_callable=generate_dashboard_task,
        trigger_rule=TriggerRule.ALL_DONE
    )

    # =================================================================
    # TASK 5: Check & Alert (always runs)
    # =================================================================
    def check_alerts_task():
        import sys
        sys.path.insert(0, HOST_PATH)
        from scripts.alert_handler import check_validation_results
        check_validation_results()

    check_alerts = PythonOperator(
        task_id="check_alerts",
        python_callable=check_alerts_task,
        trigger_rule=TriggerRule.ALL_DONE
    )

    # =================================================================
    # Task Dependencies (Circuit Breaker Pattern)
    # =================================================================
    # Metadata → Validation (only if success)
    metadata_check >> data_validation
    
    # Both tests → Reporting (always)
    metadata_check >> generate_allure_report
    data_validation >> generate_allure_report
    
    # Reporting → Dashboard & Alerts (always)
    generate_allure_report >> generate_dashboard
    generate_allure_report >> check_alerts

