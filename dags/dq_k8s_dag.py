from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.trigger_rule import TriggerRule
from kubernetes.client import models as k8s
from datetime import datetime
import os

HOST_PATH = "/Users/amohiuddeen/Github/dq-framework-poc"

# Map Volume
volume = k8s.V1Volume(name="proj-vol", host_path=k8s.V1HostPathVolumeSource(path=HOST_PATH))
mount = k8s.V1VolumeMount(name="proj-vol", mount_path="/app")

default_args = {'owner': 'data_eng', 'start_date': datetime(2023, 1, 1), 'retries': 0, 'catchup': False}

with DAG('dq_k8s_v1', default_args=default_args, schedule_interval='@hourly') as dag:
    
    # Common Pod Config
    base_config = {
        "namespace": "default",
        "image": "dq-runner:latest",
        "image_pull_policy": "Never", # Use local Docker image
        "volumes": [volume],
        "volume_mounts": [mount],
        "env_vars": {"S3_ENDPOINT": "http://minio:9000", "PYTHONPATH": "/app"},
        "is_delete_operator_pod": True,
	    "config_file": "/Users/amohiuddeen/.kube/config",
        "cluster_context": "docker-desktop",
        "in_cluster": False
    }

    # 1. Metadata Check
    meta = KubernetesPodOperator(
        task_id="metadata", name="dq-meta",
        cmds=["pytest"], arguments=["/app/tests/test_metadata.py", "--alluredir=/app/allure-results", "--clean-alluredir"],
        **base_config
    )

    # 2. Validation Check (Inject Timestamps)
    val_env_vars = base_config["env_vars"].copy()
    val_env_vars["BATCH_START_TS"] = "{{ data_interval_start }}"
    val_env_vars["BATCH_END_TS"] = "{{ data_interval_end }}"

    val_config = base_config.copy()
    val_config["env_vars"] = val_env_vars

    # 3. Create the operator with the COMPLETE config
    valid = KubernetesPodOperator(
        task_id="validation", 
        name="dq-valid",
        cmds=["pytest"], 
        arguments=["/app/tests/test_validation.py", "--alluredir=/app/allure-results"],
        **val_config  # <--- Use the custom config here
    )

    # 3. Reporting (Always Runs)
    report = KubernetesPodOperator(
        task_id="report", name="dq-rep", trigger_rule=TriggerRule.ALL_DONE,
        cmds=["bash", "-c"], arguments=["allure generate /app/allure-results -o /app/allure-report --clean"],
        **base_config
    )

    meta >> valid >> report
    meta >> report

