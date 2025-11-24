from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime

HOST_PATH = "/Users/amohiuddeen/Github/dq-framework-poc"

default_args = {
    'owner': 'data_eng',
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
    'catchup': False
}

with DAG(
    'dq_hourly_pipeline_bash',
    default_args=default_args,
    schedule_interval='@hourly',
    description='Hourly data quality validation pipeline',
    catchup=False,
    tags=['data-quality', 'production']
) as dag:

    metadata_check = BashOperator(
        task_id="metadata_check",
        bash_command=f"""
        cd {HOST_PATH}
        docker run --rm \
          -v {HOST_PATH}:/app \
          --add-host host.docker.internal:host-gateway \
          -e ENV=local \
          -e PYTHONPATH=/app \
          -e AIRFLOW_RUN_ID={{{{ run_id }}}} \
          dq-runner:latest \
          pytest /app/tests/test_metadata.py --alluredir=/app/allure-results --clean-alluredir -v
        """
    )

    data_validation = BashOperator(
        task_id="data_validation",
        bash_command=f"""
        cd {HOST_PATH}
        docker run --rm \
          -v {HOST_PATH}:/app \
          --add-host host.docker.internal:host-gateway \
          -e ENV=local \
          -e PYTHONPATH=/app \
          -e AIRFLOW_RUN_ID={{{{ run_id }}}} \
          dq-runner:latest \
          pytest /app/tests/test_validation.py --alluredir=/app/allure-results -v
        """,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    generate_allure_report = BashOperator(
        task_id="generate_allure_report",
        trigger_rule=TriggerRule.ALL_DONE,
        bash_command=f"""
            cd {HOST_PATH}
            if [ -d "allure-results" ] && [ "$(ls -A allure-results)" ]; then
                allure generate allure-results -o allure-report --clean || echo "Allure generation failed"
            else
                echo "No allure-results directory or empty results"
            fi
        """
    )

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

    metadata_check >> data_validation >> generate_allure_report
    metadata_check >> generate_allure_report
    generate_allure_report >> [generate_dashboard, check_alerts]
