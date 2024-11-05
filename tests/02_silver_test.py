import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

# Initialize SparkSession for tests
spark = SparkSession.builder.appName("ETL Silver Layer Tests").getOrCreate()

# Path variables for bronze and silver target
bronze_path = "s3://sales-pyspark-etl/target_files/bronze/sales/"
silver_path = "s3://sales-pyspark-etl/target_files/silver/sales/"


@pytest.fixture(scope="module")
def bronze_data():
    # Read processed data from Bronze layer for testing
    return spark.read.parquet(bronze_path)


@pytest.fixture(scope="module")
def silver_data():
    # Read processed data from Silver layer for testing
    return spark.read.parquet(silver_path)


def test_snake_case_columns(silver_data):
    """
    Test to check if all columns are in snake_case format.
    """
    invalid_columns = [c for c in silver_data.columns if not c.islower() or " " in c]
    assert len(invalid_columns) == 0, f"Non-snake_case columns found: {invalid_columns}"


def test_metadata_columns(silver_data):
    """
    Test to ensure data is correctly partitioned in the Silver layer.
    """
    metadata_columns = ["file_path", "execution_datetime"]
    for col_name in metadata_columns:
        assert (
            col_name in silver_data.columns
        ), f"metadata column {col_name} missing in Silver layer data"


def test_no_duplicates_on_keys(silver_data):
    """
    Test to ensure there are no duplicate records based on order_date, product_id, and order_id in the Silver layer.
    """
    # Count the records grouped by the key columns
    duplicate_count = (
        silver_data.groupBy("order_date", "product_id", "order_id")
        .count()
        .filter(col("count") > 1)
        .count()
    )

    # Assert that there are no duplicates
    assert (
        duplicate_count == 0
    ), f"Found {duplicate_count} duplicate records based on order_date, product_id, and order_id"
