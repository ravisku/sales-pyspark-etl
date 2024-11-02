import boto3
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, current_timestamp

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("ETL Pipeline") \
    .getOrCreate()

# Define file paths
bronze_path = "s3://wt-ravi-sandbox/hema/parquet_table/bronze/"
source_path = "s3://wt-ravi-sandbox/hema/"
crawler_name = ""

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def trigger_glue_crawler(crawler_name):
    client = boto3.client('glue', region_name='your-region')
    try:
        response = client.start_crawler(Name=crawler_name)
        print(f"Triggered Glue Crawler: {crawler_name}")
    except client.exceptions.CrawlerRunningException:
        print(f"Crawler {crawler_name} is already running.")
    except Exception as e:
        print(f"Error triggering Glue Crawler: {str(e)}")

def load_bronze_layer(source_path, bronze_path):
    try:
        logger.info("Starting Bronze Layer ETL process.")
        
        # Read source CSV file(s)
        logger.info(f"Reading data from source path: {source_path}")
        df = spark.read.csv(source_path, header=True, inferSchema=True)
        
        # Add metadata columns
        logger.info("Adding metadata columns: file_path and execution_datetime.")
        df = df.withColumn("file_path", input_file_name()).withColumn("execution_datetime", current_timestamp())
        
        # Write to Bronze layer with schema evolution enabled
        logger.info(f"Writing data to Bronze path with schema evolution enabled: {bronze_path}")
        df.write.mode("overwrite").option("mergeSchema", "true").parquet(bronze_path)
        
        logger.info("Bronze Layer ETL process completed successfully.")
    except Exception as e:
        logger.error(f"Error in Bronze Layer ETL process: {e}")
        raise

if __name__ == "__main__":
    # Execute Bronze layer ETL
    logger.info("Starting ETL Pipeline.")
    load_bronze_layer(source_path, bronze_path)
    logger.info("ETL Pipeline executed successfully.")

    logger.info("Trigger glue crawler for bronze table.")
    trigger_glue_crawler(crawler_name)
    logger.info("Crawler executed successfully.")
