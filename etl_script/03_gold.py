import logging

import boto3
from common_utils import trigger_glue_crawler
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, current_timestamp, input_file_name

# Initialize Spark Session
spark = SparkSession.builder.appName("ETL Pipeline").getOrCreate()

# Define file paths for the Silver and Gold layers
silver_path = "s3://sales-pyspark-etl/target_files/silver/sales/"
gold_path = "s3://sales-pyspark-etl/target_files/gold/"
crawler_name = "gold_crawler"
customer_table_name = "customer"
sales_table_name = "sales"

# Set up logging to capture ETL process details
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_sales_table(silver_path, gold_path):
    """
    Loads Sales and Customer aggregated data in Gold layer.

    Parameters:
    - silver_path (str): S3 path for Silver layer data.
    - gold_path (str): S3 path for Gold layer storage.
    """
    try:
        logger.info("Starting Sales Table ETL process for Gold Layer.")

        # Define path for Sales table in the Gold layer
        sales_table_path = f"{gold_path}/{sales_table_name}/"

        # Load Silver layer data
        logger.info(f"Reading data from Silver path: {silver_path}")
        df = spark.read.option("mergeSchema", "true").parquet(silver_path)

        # Create a temporary view for SQL processing
        df.createOrReplaceTempView("sales_tmp_table")

        # Select relevant columns for the Sales table and remove duplicates
        logger.info("Selecting relevant columns and removing duplicates.")
        df = spark.sql(
            """
            SELECT
                order_id,
                order_date,
                ship_date,
                ship_mode,
                city
            FROM
                sales_tmp_table
            GROUP BY
                order_id, order_date, ship_date, ship_mode, city
            """
        )

        # Write to Gold layer, partitioned by order_date
        logger.info(f"Writing processed Sales data to Gold path: {sales_table_path}")
        df.repartition(1).write.partitionBy("order_date").mode("append").parquet(
            sales_table_path
        )

        logger.info("Sales Table ETL process for Gold Layer completed successfully.")
    except Exception as e:
        logger.error(f"Error in Sales Table ETL process for Gold Layer: {e}")
        raise


def get_latest_date(silver_path):
    """
    Retrieves the latest date from the Silver layer data based on partition columns.

    Parameters:
    - silver_path (str): S3 path for Silver layer data.

    Returns:
    - str: The latest date in "yyyy-MM-dd" format.
    """
    try:
        # Load data schema and partition columns only
        logger.info(f"Reading partition data from Silver path for latest date.")
        df = spark.read.format("parquet").load(silver_path)

        # Select distinct partition columns to find the latest date
        partition_df = df.select("year", "month", "day").distinct()
        latest_date = (
            partition_df.orderBy(
                col("year").desc(), col("month").desc(), col("day").desc()
            )
            .limit(1)
            .select(
                concat_ws("-", col("year"), col("month"), col("day")).alias(
                    "latest_date"
                )
            )
        ).collect()[0]["latest_date"]

        logger.info(f"Latest date obtained: {latest_date}")
        return latest_date

    except Exception as e:
        logger.error(f"Error fetching latest date from Silver layer: {e}")
        raise


def load_customer_table(silver_path, gold_path, latest_date):
    """
    Loads and processes data for the Customer table, calculating order quantities
    over different time windows, then writes it to the Gold layer.

    Parameters:
    - silver_path (str): S3 path for Silver layer data.
    - gold_path (str): S3 path for Gold layer storage.
    - latest_date (str): The reference date for calculating order quantities.
    """
    try:
        logger.info("Starting Customer Table ETL process for Gold Layer.")

        # Define path for Customer table in the Gold layer
        customer_table_path = f"{gold_path}/{customer_table_name}/"

        # Read Silver layer data
        logger.info(f"Reading data from Silver path: {silver_path}")
        df = spark.read.option("mergeSchema", "true").parquet(silver_path)

        # Create a temporary view for SQL processing
        df.createOrReplaceTempView("customer_tmp_table")

        # SQL query to calculate order quantities over different time frames
        logger.info("Calculating order quantities over different time frames.")
        query = f"""
            SELECT
                customer_id,
                split_part(customer_name, ' ', 1) AS customer_first_name,
                split_part(customer_name, ' ', 2) AS customer_last_name,
                segment,
                country,
                COUNT(DISTINCT CASE
                    WHEN order_date BETWEEN DATE_SUB('{latest_date}', 30) AND '{latest_date}'
                    THEN order_id ELSE NULL END) AS order_quantity_last_30_days,
                COUNT(DISTINCT CASE
                    WHEN order_date BETWEEN DATE_SUB('{latest_date}', 180) AND '{latest_date}'
                    THEN order_id ELSE NULL END) AS order_quantity_last_6_months,
                COUNT(DISTINCT CASE
                    WHEN order_date BETWEEN DATE_SUB('{latest_date}', 365) AND '{latest_date}'
                    THEN order_id ELSE NULL END) AS order_quantity_last_12_months,
                COUNT(DISTINCT order_id) AS total_quantity_of_orders
            FROM
                customer_tmp_table
            GROUP BY
                customer_id, customer_first_name, customer_last_name, segment, country
        """

        # Execute query to get Customer data
        df = spark.sql(query)

        # Write to Gold layer, overwriting existing data for Customer table
        logger.info(
            f"Writing processed Customer data to Gold path: {customer_table_path}"
        )
        df.repartition(1).write.mode("overwrite").parquet(customer_table_path)

        logger.info("Customer Table ETL process for Gold Layer completed successfully.")
    except Exception as e:
        logger.error(f"Error in Customer Table ETL process for Gold Layer: {e}")
        raise


def load_gold_layer(silver_path, gold_path):
    """
    Executes ETL processes for both Sales and Customer tables in the Gold layer.

    Parameters:
    - silver_path (str): S3 path for Silver layer data.
    - gold_path (str): S3 path for Gold layer storage.
    """
    logger.info("Starting Gold Layer ETL process.")
    load_sales_table(silver_path, gold_path)
    latest_date = get_latest_date(silver_path)
    load_customer_table(silver_path, gold_path, latest_date)
    logger.info("Gold Layer ETL process completed successfully.")


if __name__ == "__main__":
    # Execute Gold layer ETL
    logger.info("Starting ETL Pipeline for Gold layer.")
    load_gold_layer(silver_path, gold_path)
    logger.info("ETL Pipeline for Gold layer executed successfully.")

    # Trigger the Glue crawler to update the catalog with new data in the Gold layer.
    logger.info("Triggering Glue crawler for Gold layer tables.")
    trigger_glue_crawler(crawler_name)
    logger.info("Glue crawler for Gold layer tables executed successfully.")
