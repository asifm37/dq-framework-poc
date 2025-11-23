import pytest
import allure

@allure.feature("Metadata Check")
def test_metadata(spark, schema_registry):
    for table, config in schema_registry["tables"].items():
        with allure.step(f"Check {table}"):
            try:
                # Lightweight check - loads no data
                df = spark.read.format("iceberg").load(f"local.db.{table}")
                
                # Check 1: Existence (Implied)
                # Check 2: Schema
                expected = set(config["expectations"]["schema"].keys())
                actual = set(df.columns)
                missing = expected - actual
                if missing:
                    pytest.fail(f"Schema Mismatch! Missing: {missing}")
            except Exception as e:
                pytest.fail(f"Table unreachable: {e}")

