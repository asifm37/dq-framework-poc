import os
import sys
from functools import reduce
from operator import or_

import pytest
import allure
from pyspark.sql.functions import col, sum as _sum, when, count

sys.path.insert(0, '/app')
from scripts.results_tracker import (
    save_validation_result,
    get_last_validated_snapshot,
    update_snapshot_tracking,
    init_database
)


def get_current_snapshot(spark, table):
    """Get the latest snapshot ID for a table."""
    snapshot_df = spark.sql(f"""
        SELECT snapshot_id
        FROM local.db.{table}.snapshots
        ORDER BY committed_at DESC
        LIMIT 1
    """)
    return None if snapshot_df.isEmpty() else snapshot_df.collect()[0][0]


def read_table_data(spark, table, last_validated, current_snapshot):
    """Read table data incrementally or fully based on validation history."""
    if last_validated is None:
        allure.attach(
            f"First validation - processing all data\nCurrent Snapshot: {current_snapshot}",
            name="Incremental Info",
            attachment_type=allure.attachment_type.TEXT
        )
        return spark.read.format("iceberg").load(f"local.db.{table}")

    allure.attach(
        f"Incremental validation\nLast validated: {last_validated}\nCurrent: {current_snapshot}",
        name="Incremental Info",
        attachment_type=allure.attachment_type.TEXT
    )

    if current_snapshot == last_validated:
        allure.attach("No new data to validate", name="Info", attachment_type=allure.attachment_type.TEXT)
        return None

    try:
        return spark.read.format("iceberg") \
            .option("start-snapshot-id", last_validated) \
            .option("end-snapshot-id", current_snapshot) \
            .load(f"local.db.{table}")
    except Exception:
        allure.attach(
            "Incremental read failed, using full read",
            name="Warning",
            attachment_type=allure.attachment_type.TEXT
        )
        return spark.read.format("iceberg").load(f"local.db.{table}")


def get_required_columns(checks):
    """Extract all columns needed for validation."""
    cols = {c["column"] for c in checks}
    for c in checks:
        if c.get("compare_to_column"):
            cols.add(c["compare_to_column"])
    return list(cols)


def build_validation_conditions(checks):
    """Build Spark filter conditions from validation checks."""
    conditions = []

    for check in checks:
        c_name = check["column"]
        rule = check["rule"]

        if rule == "not_null":
            conditions.append(col(c_name).isNull())
        elif rule == "range":
            conditions.append((col(c_name) < check["min"]) | (col(c_name) > check["max"]))
        elif rule == "compare":
            other = check["compare_to_column"]
            operator = check["operator"]
            if operator == "<":
                conditions.append(col(c_name) >= col(other))
            elif operator == ">":
                conditions.append(col(c_name) <= col(other))

    return conditions


def calculate_validation_metrics(df, conditions):
    """Calculate pass/fail metrics in a single pass."""
    if not conditions:
        total_rows = df.count()
        return total_rows, total_rows, 0, 100.0

    final_cond = reduce(or_, conditions)
    result = df.select(
        _sum(when(final_cond, 1).otherwise(0)).alias("fail_count"),
        count("*").alias("total_rows")
    ).collect()[0]

    total_rows = result["total_rows"]
    fail_count = result["fail_count"]
    pass_count = total_rows - fail_count
    pass_rate = (pass_count / total_rows) * 100 if total_rows > 0 else 100.0

    return total_rows, pass_count, fail_count, pass_rate


@allure.feature("Data Validation")
def test_validation(spark, schema_registry):
    init_database()

    failures = []

    for table, config in schema_registry["tables"].items():
        with allure.step(f"Validate {table}"):
            try:
                current_snapshot = get_current_snapshot(spark, table)

                if current_snapshot is None:
                    allure.attach("No snapshots found", name="Info", attachment_type=allure.attachment_type.TEXT)
                    continue

                last_validated = get_last_validated_snapshot(table)
                df = read_table_data(spark, table, last_validated, current_snapshot)

                if df is None or df.isEmpty():
                    if df and df.isEmpty():
                        allure.attach("No data to validate", name="Info", attachment_type=allure.attachment_type.TEXT)
                        update_snapshot_tracking(table, current_snapshot, "success")
                    continue

                checks = config["expectations"]["checks"]
                required_cols = get_required_columns(checks)
                df = df.select(*required_cols)

                conditions = build_validation_conditions(checks)
                total_rows, pass_count, fail_count, pass_rate = calculate_validation_metrics(df, conditions)

                details = f"Rows: {total_rows} | Pass: {pass_count} | Fail: {fail_count} | Pass Rate: {pass_rate:.2f}%"
                allure.attach(details, name="Validation Stats", attachment_type=allure.attachment_type.TEXT)

                status = "pass" if pass_rate >= 90.0 else "fail"
                airflow_run_id = os.environ.get("AIRFLOW_RUN_ID", "manual")

                save_validation_result(
                    table_name=table, test_type="data_validation", snapshot_id=current_snapshot,
                    row_count=total_rows, pass_count=pass_count, fail_count=fail_count,
                    status=status, details=details, airflow_run_id=airflow_run_id
                )

                update_snapshot_tracking(table, current_snapshot, "success" if status == "pass" else "failed")

                if pass_rate < 90.0:
                    failures.append(f"{table}: {pass_rate:.2f}% pass rate (threshold: 90%)")

            except Exception as e:
                allure.attach(str(e), name="Error", attachment_type=allure.attachment_type.TEXT)
                save_validation_result(
                    table_name=table, test_type="data_validation", snapshot_id=None,
                    row_count=0, pass_count=0, fail_count=0, status="error",
                    details=f"Validation error: {str(e)}",
                    airflow_run_id=os.environ.get("AIRFLOW_RUN_ID", "manual")
                )
                failures.append(f"{table}: Validation error - {str(e)}")

    if failures:
        failure_msg = "\n".join([f"  - {f}" for f in failures])
        pytest.fail(f"Quality failures detected:\n{failure_msg}")
