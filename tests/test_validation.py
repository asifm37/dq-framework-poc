import pytest
import allure
import os
import sys
from pyspark.sql.functions import col

# Add project root to path for imports
sys.path.insert(0, '/app')
from scripts.results_tracker import (
    save_validation_result, 
    get_last_validated_snapshot,
    update_snapshot_tracking,
    init_database
)

@allure.feature("Data Validation")
def test_validation(spark, schema_registry):
    """
    Validates data quality using Iceberg snapshot incremental reads.
    Only validates NEW data since last successful validation.
    """
    init_database()  # Ensure DB exists
    
    for table, config in schema_registry["tables"].items():
        with allure.step(f"Validate {table}"):
            
            try:
                # Get current snapshot
                snapshot_df = spark.sql(f"""
                    SELECT snapshot_id 
                    FROM local.db.{table}.snapshots 
                    ORDER BY committed_at DESC 
                    LIMIT 1
                """)
                
                if snapshot_df.count() == 0:
                    allure.attach("No snapshots found", name="Info", attachment_type=allure.attachment_type.TEXT)
                    continue
                
                current_snapshot = snapshot_df.collect()[0][0]
                
                # Get last validated snapshot
                last_validated = get_last_validated_snapshot(table)
                
                # Read incremental data
                if last_validated is None:
                    # First run - validate all data
                    allure.attach(
                        f"First validation - processing all data\nCurrent Snapshot: {current_snapshot}", 
                        name="Incremental Info", 
                        attachment_type=allure.attachment_type.TEXT
                    )
                    df = spark.read.format("iceberg").load(f"local.db.{table}")
                else:
                    # Incremental - only new data since last validation
                    allure.attach(
                        f"Incremental validation\nLast validated: {last_validated}\nCurrent: {current_snapshot}", 
                        name="Incremental Info", 
                        attachment_type=allure.attachment_type.TEXT
                    )
                    
                    if current_snapshot == last_validated:
                        allure.attach("No new data to validate", name="Info", attachment_type=allure.attachment_type.TEXT)
                        continue
                    
                    # Read only changes between snapshots
                    try:
                        df = spark.read.format("iceberg") \
                            .option("start-snapshot-id", last_validated) \
                            .option("end-snapshot-id", current_snapshot) \
                            .load(f"local.db.{table}")
                    except Exception as e:
                        # Fallback to full read if incremental fails
                        allure.attach(f"Incremental read failed, using full read: {e}", 
                                    name="Warning", attachment_type=allure.attachment_type.TEXT)
                        df = spark.read.format("iceberg").load(f"local.db.{table}")
                
                if df.isEmpty():
                    allure.attach("No data to validate", name="Info", attachment_type=allure.attachment_type.TEXT)
                    # Still update snapshot tracking
                    update_snapshot_tracking(table, current_snapshot, "success")
                    continue
                
                # Get required columns for checks
                checks = config["expectations"]["checks"]
                cols = {c["column"] for c in checks}
                for c in checks:
                    if c.get("compare_to_column"):
                        cols.add(c["compare_to_column"])
                
                # Select only required columns
                df = df.select(*list(cols))
                total_rows = df.count()
                
                # --- Vectorized Validation Checks ---
                conditions = []
                for check in checks:
                    c_name = check["column"]
                    rule = check["rule"]
                    
                    if rule == "not_null":
                        conditions.append(col(c_name).isNull())
                    
                    elif rule == "range":
                        conditions.append(
                            (col(c_name) < check["min"]) | (col(c_name) > check["max"])
                        )
                    
                    elif rule == "compare":
                        other = check["compare_to_column"]
                        operator = check["operator"]
                        
                        if operator == "<":
                            # Fail if c_name >= other
                            conditions.append(col(c_name) >= col(other))
                        elif operator == ">":
                            # Fail if c_name <= other
                            conditions.append(col(c_name) <= col(other))
                
                # Combine all conditions
                if conditions:
                    final_cond = conditions[0]
                    for c in conditions[1:]:
                        final_cond = final_cond | c
                    
                    fail_count = df.filter(final_cond).count()
                    pass_count = total_rows - fail_count
                    pass_rate = (pass_count / total_rows) * 100
                else:
                    fail_count = 0
                    pass_count = total_rows
                    pass_rate = 100.0
                
                # Create detailed report
                details = f"Rows: {total_rows} | Pass: {pass_count} | Fail: {fail_count} | Pass Rate: {pass_rate:.2f}%"
                allure.attach(details, name="Validation Stats", attachment_type=allure.attachment_type.TEXT)
                
                # Determine status (fail if > 10% bad data)
                status = "pass" if pass_rate >= 90.0 else "fail"
                
                # Save results
                save_validation_result(
                    table_name=table,
                    test_type="data_validation",
                    snapshot_id=current_snapshot,
                    row_count=total_rows,
                    pass_count=pass_count,
                    fail_count=fail_count,
                    status=status,
                    details=details,
                    airflow_run_id=os.environ.get("AIRFLOW_RUN_ID", "manual")
                )
                
                # Update snapshot tracking (only on success)
                if status == "pass":
                    update_snapshot_tracking(table, current_snapshot, "success")
                else:
                    update_snapshot_tracking(table, current_snapshot, "failed")
                
                # Fail test if quality threshold not met
                if pass_rate < 90.0:
                    pytest.fail(f"Quality Failure: {pass_rate:.2f}% pass rate (threshold: 90%)")
            
            except Exception as e:
                # Handle unexpected errors
                allure.attach(str(e), name="Error", attachment_type=allure.attachment_type.TEXT)
                save_validation_result(
                    table_name=table,
                    test_type="data_validation",
                    snapshot_id=None,
                    row_count=0,
                    pass_count=0,
                    fail_count=0,
                    status="error",
                    details=f"Validation error: {str(e)}",
                    airflow_run_id=os.environ.get("AIRFLOW_RUN_ID", "manual")
                )
                pytest.fail(f"Validation error: {e}")

