from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
import os

# CHANGE THIS to your project path
HOST_PATH = "/Users/amohiuddeen/Github/dq-framework-poc"

default_args = {
    'owner': 'data_eng',
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
    'catchup': False
}

with DAG('dq_prod_v1', default_args=default_args, schedule_interval='@hourly') as dag:

    docker_config = {
        "image": "dq-runner:latest",
        "api_version": "auto",
        "auto_remove": True,
        "mounts": [f"{HOST_PATH}:/app"],
        "docker_url": "unix://var/run/docker.sock",
        "network_mode": "bridge",
        # This allows the container to see your Mac's MinIO
        "extra_hosts": {"host.docker.internal": "host-gateway"},
        "environment": {
            "ENV": "local", 
            "PYTHONPATH": "/app"
        }
    }

    # 1. Metadata Check
    meta = DockerOperator(
        task_id="metadata_check",
        command="pytest /app/tests/test_metadata.py --alluredir=/app/allure-results --clean-alluredir",
        **docker_config
    )

    # 2. Validation Check
    valid = DockerOperator(
        task_id="validation_check",
        command="pytest /app/tests/test_validation.py --alluredir=/app/allure-results",
        **docker_config
    )
    valid.environment["BATCH_START_TS"] = "{{ data_interval_start }}"
    valid.environment["BATCH_END_TS"] = "{{ data_interval_end }}"

    # 3. Reporting (Run locally via Bash, since Allure is on Mac)
    # We use BashOperator because the results are already in the mounted folder
    from airflow.operators.bash import BashOperator
    report = BashOperator(
        task_id="generate_report",
        trigger_rule=TriggerRule.ALL_DONE,
        bash_command=f"allure generate {HOST_PATH}/allure-results -o {HOST_PATH}/allure-report --clean"
    )

    meta >> valid >> report
    meta >> report
