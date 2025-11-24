import pytest
import allure
import sys
import os

sys.path.insert(0, '/app')
from scripts.results_tracker import save_validation_result, init_database


def normalize_type(type_str):
    """Normalize Spark data types for comparison."""
    type_mapping = {
        'string': 'string', 'stringtype()': 'string',
        'int': 'int', 'integer': 'int', 'integertype()': 'int',
        'double': 'double', 'doubletype()': 'double',
        'timestamp': 'timestamp', 'timestamptype()': 'timestamp',
        'long': 'bigint', 'bigint': 'bigint',
    }
    return type_mapping.get(type_str.lower().strip(), type_str.lower().strip())


def get_latest_snapshot(spark, table):
    """Get the latest snapshot ID for a table."""
    try:
        snapshot_df = spark.sql(f"""
            SELECT snapshot_id 
            FROM local.db.{table}.snapshots 
            ORDER BY committed_at DESC 
            LIMIT 1
        """)
        return snapshot_df.collect()[0][0] if snapshot_df.count() > 0 else None
    except:
        return None


def validate_schema(spark, table, expected_schema):
    """Validate table schema against expectations."""
    describe_df = spark.sql(f"DESCRIBE TABLE local.db.{table}")
    actual_schema = {row['col_name']: row['data_type'] for row in describe_df.collect()}
    
    expected_cols = set(expected_schema.keys())
    actual_cols = set(actual_schema.keys())
    errors = []
    
    missing = expected_cols - actual_cols
    if missing:
        errors.append(f"Missing columns: {missing}")
    
    extra = actual_cols - expected_cols
    if extra:
        errors.append(f"Extra columns: {extra}")
    
    for col in expected_cols & actual_cols:
        expected_type = normalize_type(expected_schema[col])
        actual_type = normalize_type(actual_schema[col])
        if expected_type != actual_type:
            errors.append(f"Type mismatch in '{col}': expected {expected_schema[col]}, got {actual_schema[col]}")
    
    return errors


@allure.feature("Metadata Check")
def test_metadata(spark, schema_registry):
    init_database()
    
    for table, config in schema_registry["tables"].items():
        with allure.step(f"Check {table}"):
            try:
                errors = validate_schema(spark, table, config["expectations"]["schema"])
                snapshot_id = get_latest_snapshot(spark, table)
                airflow_run_id = os.environ.get("AIRFLOW_RUN_ID", "manual")
                
                if errors:
                    save_validation_result(
                        table_name=table, test_type="metadata", snapshot_id=snapshot_id,
                        row_count=0, pass_count=0, fail_count=1, status="fail",
                        details="; ".join(errors), airflow_run_id=airflow_run_id
                    )
                    allure.attach("\n".join(errors), name="Schema Errors", attachment_type=allure.attachment_type.TEXT)
                    pytest.fail(f"Schema validation failed: {'; '.join(errors)}")
                else:
                    save_validation_result(
                        table_name=table, test_type="metadata", snapshot_id=snapshot_id,
                        row_count=0, pass_count=1, fail_count=0, status="pass",
                        details=f"Schema valid. Snapshot: {snapshot_id}", airflow_run_id=airflow_run_id
                    )
                    allure.attach(f"Snapshot ID: {snapshot_id}", name="Metadata", attachment_type=allure.attachment_type.TEXT)
                    
            except Exception as e:
                save_validation_result(
                    table_name=table, test_type="metadata", snapshot_id=None,
                    row_count=0, pass_count=0, fail_count=1, status="fail",
                    details=f"Table unreachable: {str(e)}", airflow_run_id=os.environ.get("AIRFLOW_RUN_ID", "manual")
                )
                allure.attach(str(e), name="Error", attachment_type=allure.attachment_type.TEXT)
                pytest.fail(f"Table unreachable: {e}")
