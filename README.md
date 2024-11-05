# HEMA Data Engineer Assignment

This assignment consists of two main parts:

1. ETL Pipeline
2. AWS Design for Data Sharing

## Repository Structure

sales-pyspark-etl/ ├── etl_script/ │ ├── 01_bronze.py # ETL script for Bronze layer │ ├── 02_silver.py # ETL script for Silver layer │ ├── 03_gold.py # ETL script for Gold layer │ └── common_utils.py # Common utilities for all layers ├── tests/ │ ├── test_bronze_etl.py # Tests for Bronze layer │ ├── test_silver_etl.py # Tests for Silver layer │ ├── test_gold_etl.py # Tests for Gold layer ├── terraform/ │ ├── main.tf # Terraform configuration for AWS resources ├── .github/workflows/ │ └── deploy.yaml # GitHub Actions workflow for CI/CD └── README.md # Project documentation


## AWS Design for Data Sharing

The data-sharing architecture design is documented in `AWS_Data_Sharing_LF.pdf`, which includes solution notes and implementation steps.

## ETL Pipeline

The ETL pipeline ingests raw CSV data from S3, performs data transformations, and organizes it into Silver and Gold layers. This pipeline follows a medallion architecture with a focus on data quality, deduplication, and partitioning for optimized querying in Athena.

## Architecture
The ETL pipeline is structured using a **medallion architecture**:
1. **Bronze Layer**: Ingests raw data with minimal transformations.
2. **Silver Layer**: Cleans, deduplicates, and formats the data.
3. **Gold Layer**: Aggregates data to be ready for analysis.

## Technologies
- **AWS S3**: Stores raw, intermediate, and processed data.
- **AWS Glue**: Crawls data and creates tables in the data catalog.
- **AWS Athena**: SQL querying and analysis.
- **PySpark**: ETL data transformations.
- **Terraform**: Infrastructure as Code for AWS resources.
- **GitHub Actions**: Automates deployment through CI/CD.

## Infrastructure Setup with Terraform

All AWS resources are deployed using Terraform, with configurations available in the `terraform/` directory. The following resources are created:

1. **S3 Bucket**: Stores source and target data files.
2. **IAM Role**: Grants Glue permissions for ETL processes.
3. **Bucket Policy**: Allows Glue to access the S3 bucket.
4. **Glue ETL Jobs**: Executes ETL jobs for each data layer.
5. **Glue Crawlers**: Updates the Glue Catalog with new data.

## ETL Script Details

### Common Utilities

`common_utils.py`: Contains utility functions shared across ETL scripts, such as a function to trigger the Glue Crawler.

### Bronze Layer

`01_bronze.py`
1. Ingests raw CSV data from S3 and writes it as Parquet to the Bronze layer.
2. Adds metadata columns such as `file_path` and `etl_timestamp`.
3. Partitions data by `order_date` in `year/month/day` format.

### Silver Layer

`02_silver.py`
1. Loads data from the Bronze layer and applies data transformations.
2. Formats date columns, adds metadata, and deduplicates records.
3. Partitions data by `order_date` in `year/month/day` format.

### Gold Layer

`03_gold.py`: Aggregates data into two datasets: `Customer` and `Sales`.

- **Customer Dataset**:
    - Contains customer attributes and aggregated sales for the last 1, 6, and 12 months.

- **Sales Dataset**:
    - Partitioned by `order_date` in `yyyy-mm-dd` format for range-based queries.
    - Aggregates order data for analysis.

## Tests

Tests are available for each ETL layer in the `tests/` directory. **Note**: These tests are reference implementations and are not integrated with Glue ETL jobs (Pytest in Glue is not included to keep the assignment concise).

### Test Coverage
1. **Row Count Verification**: Ensures data row count matches between stages.
2. **Null Check in Critical Columns**: Verifies that important columns have no null values.
3. **Duplicate Check**: Identifies duplicate records in the data.
4. **Partition Validation**: Ensures that data is partitioned correctly.

## Gold Layer Outputs

The `gold_results` directory contains CSV outputs for the `Customer` and `Sales` tables in the Gold layer.

## Usage Notes

- **Schema Evolution**: Enabled with `.option("mergeSchema", "true")` to handle schema changes.
- **Error Handling**: Logging is configured in each ETL stage for troubleshooting.

