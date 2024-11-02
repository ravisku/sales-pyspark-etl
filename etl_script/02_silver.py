import boto3
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, current_timestamp

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("ETL Pipeline") \
    .getOrCreate()

# Define file paths
silver_path = "s3://wt-ravi-sandbox/hema/parquet_table/silver/"
bronze_path = "s3://wt-ravi-sandbox/hema/parquet_table/silver/"
crawler_name = ""

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def transform_silver_layer(bronze_path, silver_path):
    try:
        logger.info("Starting Silver Layer ETL process.")

        df = spark.read.parquet(bronze_pathinferSchema=True)
        
        df_silver = df \
            .select([col(c).alias(c.lower().replace(" ", "_")) for c in df_bronze.columns]) \
            .withColumn("year", year("order_date")) \
            .withColumn("month", month("order_date")) \
            .withColumn("day", dayofmonth("order_date"))
        
        df_silver.write.partitionBy("year", "month", "day").mode("overwrite").parquet(silver_path)
    except Exception as e:
        logger.error(f"Error in Bronze Layer ETL process: {e}")
        raise

if __name__ == "__main__":
    # Execute Bronze layer ETL
    logger.info("Starting ETL Pipeline.")
    transform_silver_layer(bronze_path, silver_path)
    logger.info("ETL Pipeline executed successfully.")

    logger.info("Trigger glue crawler for bronze table.")
    trigger_glue_crawler(crawler_name)
    logger.info("Crawler executed successfully.")
