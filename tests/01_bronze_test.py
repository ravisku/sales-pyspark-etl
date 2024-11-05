import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

# Initialize SparkSession for tests
spark = SparkSession.builder.appName("ETL Bronze Layer Tests").getOrCreate()

# Path variables for source and bronze target
source_path = "s3://sales-pyspark-etl/source_files/"
bronze_path = "s3://sales-pyspark-etl/target_files/bronze/sales/"


@pytest.fixture(scope="module")
def source_data():
    # Read source data for testing
    return spark.read.csv(source_path, header=True, inferSchema=True)


@pytest.fixture(scope="module")
def bronze_data():
    # Read processed data from Bronze layer for testing
    return spark.read.parquet(bronze_path)


def test_row_count(source_data, bronze_data):
    """
    Test to verify that the row count in the bronze layer matches the source row count.
    """
    source_count = source_data.count()
    bronze_count = bronze_data.count()
    assert (
        bronze_count == source_count
    ), f"Row count mismatch: Source={source_count}, Bronze={bronze_count}"


def test_no_nulls_in_critical_columns(bronze_data):
    """
    Test to check for nulls in critical columns in the Bronze layer.
    """
    critical_columns = ["Order ID", "Product_ID", "Order Date", "sales"]
    for column in critical_columns:
        null_count = bronze_data.filter(col(column).isNull()).count()
        assert null_count == 0, f"Null values found in critical column: {column}"


def test_partitioning(bronze_data):
    """
    Test to ensure data is correctly partitioned in the Bronze layer.
    """
    partition_columns = ["year", "month", "day"]
    for col_name in partition_columns:
        assert (
            col_name in bronze_data.columns
        ), f"Partition column {col_name} missing in Bronze layer data"
        null_count = bronze_data.filter(col(col_name).isNull()).count()
        assert (
            null_count == 0
        ), f"Partition column {col_name} contains {null_count} null values in Bronze layer data"


def test_metadata_columns(bronze_data):
    """
    Test to ensure data is correctly partitioned in the Bronze layer.
    """
    metadata_columns = ["file_path", "execution_datetime"]
    for col_name in metadata_columns:
        assert (
            col_name in bronze_data.columns
        ), f"metadata column {col_name} missing in Bronze layer data"
