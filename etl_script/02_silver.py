import logging

import boto3
from common_utils import trigger_glue_crawler
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col,
    current_timestamp,
    date_format,
    dayofmonth,
    input_file_name,
    month,
    regexp_extract,
    row_number,
    to_date,
    year,
)

# Initialize Spark Session
spark = SparkSession.builder.appName("ETL Pipeline").getOrCreate()

# Define file paths for the Silver layer and the Bronze source.
silver_path = "s3://sales-pyspark-etl/target_files/silver/sales/"
bronze_path = "s3://sales-pyspark-etl/target_files/bronze/sales/"
crawler_name = "silver_crawler"

# Set up logging configuration to capture ETL flow details
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def transform_silver_layer(bronze_path, silver_path):
    """
    Transform data from the Bronze layer to the Silver layer, applying deduplication,
    metadata enrichment, and date formatting.

    Parameters:
    - bronze_path (str): S3 path for the Bronze layer data.
    - silver_path (str): S3 path for the Silver layer destination.
    """
    try:
        logger.info("Starting Silver Layer ETL process.")

        # Read from Bronze layer with schema evolution enabled.
        logger.info(f"Reading data from Bronze path: {bronze_path}")
        df = spark.read.option("mergeSchema", "true").parquet(bronze_path)

        # Standardize column names, format dates, and add metadata columns.
        logger.info("Transforming and formatting data.")
        df = (
            df.select([col(c).alias(c.lower().replace(" ", "_")) for c in df.columns])
            .withColumn(
                "order_date",
                date_format(to_date("order_date", "dd/MM/yyyy"), "yyyy-MM-dd"),
            )
            .withColumn(
                "ship_date",
                date_format(to_date("ship_date", "dd/MM/yyyy"), "yyyy-MM-dd"),
            )
            .withColumn("file_path", regexp_extract(input_file_name(), "(.*/)", 1))
            .withColumn("execution_datetime", current_timestamp())
        )

        # Deduplicate records by keeping only the most recent record per order_id/product_id/order_date.
        logger.info("Applying deduplication on order_id and product_id.")
        window_spec = Window.partitionBy(
            "order_id", "product_id", "order_date"
        ).orderBy(col("row_id").desc())
        df = df.withColumn("rnk", row_number().over(window_spec)).filter(
            col("rnk") == 1
        )

        # Drop temporary ranking column
        df = df.drop("rnk")

        # Write to Silver layer in partitioned format with schema evolution enabled.
        logger.info(f"Writing transformed data to Silver path: {silver_path}")
        df.repartition(1).write.partitionBy("year", "month", "day").option(
            "mergeSchema", "true"
        ).mode("append").parquet(silver_path)

        logger.info("Silver Layer ETL process completed successfully.")
    except Exception as e:
        logger.error(f"Error in Silver Layer ETL process: {e}")
        raise


if __name__ == "__main__":
    # Execute Silver layer ETL
    logger.info("Starting silver layer ETL Pipeline.")
    transform_silver_layer(bronze_path, silver_path)
    logger.info("ETL Pipeline for silver layer executed successfully.")

    # Trigger the Glue crawler to update the catalog with new data in the Silver layer.
    logger.info("Triggering Glue crawler for Silver layer table.")
    trigger_glue_crawler(crawler_name)
    logger.info("Glue crawler for Silver layer table executed successfully.")
