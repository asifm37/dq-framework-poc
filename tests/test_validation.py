import pytest
import allure
import os
from pyspark.sql.functions import col

@allure.feature("Data Validation")
def test_validation(spark, schema_registry):
    # Airflow Timestamps (Defaults for manual testing)
    start = os.environ.get("BATCH_START_TS", "2023-01-01T10:00:00")
    end = os.environ.get("BATCH_END_TS", "2023-01-01T11:00:00")

    for table, config in schema_registry["tables"].items():
        with allure.step(f"Validate {table}"):
            
            # --- Optimization: Column Pruning ---
            time_col = config["watermark_column"]
            checks = config["expectations"]["checks"]
            
            # Identify required columns
            cols = {c["column"] for c in checks}
            cols.add(time_col)
            for c in checks:
                if c.get("compare_to_column"): cols.add(c["compare_to_column"])

            # Read + Select + Filter
            df = spark.read.format("iceberg").load(f"local.db.{table}") \
                .select(*list(cols)) \
                .filter((col(time_col) >= start) & (col(time_col) < end))

            if df.isEmpty(): continue

            # --- Vectorized Checks ---
            conditions = []
            for check in checks:
                c_name = check["column"]
                rule = check["rule"]

                if rule == "not_null":
                    conditions.append(col(c_name).isNull())
                elif rule == "range":
                    conditions.append((col(c_name) < check["min"]) | (col(c_name) > check["max"]))
                elif rule == "compare":
                    # Cross-column check
                    other = check["compare_to_column"]
                    if check["operator"] == "<":
                        conditions.append(col(c_name) >= col(other)) # Fail if Amount >= Limit

            if conditions:
                final_cond = conditions[0]
                for c in conditions[1:]: final_cond = final_cond | c
                
                bad_count = df.filter(final_cond).count()
                total = df.count()
                pct = (bad_count / total) * 100
                
                allure.attach(f"Invalid Rows: {bad_count}/{total} ({pct}%)", name="Stats")
                
                if pct > 10.0:
                    pytest.fail(f"Quality Failure: {pct}% invalid data detected.")

