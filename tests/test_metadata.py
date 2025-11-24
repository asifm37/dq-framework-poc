import pytest
import allure
import sys
import os

sys.path.insert(0, '/app')
from scripts.results_tracker import save_validation_result, init_database

@allure.feature("Metadata Check")
def test_metadata(spark, schema_registry):
    init_database()
    
    for table, config in schema_registry["tables"].items():
        with allure.step(f"Check {table}"):
            try:
                df = spark.read.format("iceberg").load(f"local.db.{table}")
                
                expected = set(config["expectations"]["schema"].keys())
                actual = set(df.columns)
                missing = expected - actual
                extra = actual - expected
                
                errors = []
                if missing:
                    errors.append(f"Missing columns: {missing}")
                if extra:
                    errors.append(f"Extra columns: {extra}")
                
                try:
                    snapshot_df = spark.sql(f"""
                        SELECT snapshot_id 
                        FROM local.db.{table}.snapshots 
                        ORDER BY committed_at DESC 
                        LIMIT 1
                    """)
                    snapshot_id = snapshot_df.collect()[0][0] if snapshot_df.count() > 0 else None
                except:
                    snapshot_id = None
                
                if errors:
                    save_validation_result(
                        table_name=table,
                        test_type="metadata",
                        snapshot_id=snapshot_id,
                        row_count=0,
                        pass_count=0,
                        fail_count=1,
                        status="fail",
                        details="; ".join(errors),
                        airflow_run_id=os.environ.get("AIRFLOW_RUN_ID", "manual")
                    )
                    allure.attach("\n".join(errors), name="Schema Errors", attachment_type=allure.attachment_type.TEXT)
                    pytest.fail(f"Schema validation failed: {'; '.join(errors)}")
                else:
                    save_validation_result(
                        table_name=table,
                        test_type="metadata",
                        snapshot_id=snapshot_id,
                        row_count=0,
                        pass_count=1,
                        fail_count=0,
                        status="pass",
                        details=f"Schema valid. Snapshot: {snapshot_id}",
                        airflow_run_id=os.environ.get("AIRFLOW_RUN_ID", "manual")
                    )
                    allure.attach(f"Snapshot ID: {snapshot_id}", name="Metadata", attachment_type=allure.attachment_type.TEXT)
                    
            except Exception as e:
                save_validation_result(
                    table_name=table,
                    test_type="metadata",
                    snapshot_id=None,
                    row_count=0,
                    pass_count=0,
                    fail_count=1,
                    status="fail",
                    details=f"Table unreachable: {str(e)}",
                    airflow_run_id=os.environ.get("AIRFLOW_RUN_ID", "manual")
                )
                allure.attach(str(e), name="Error", attachment_type=allure.attachment_type.TEXT)
                pytest.fail(f"Table unreachable: {e}")
