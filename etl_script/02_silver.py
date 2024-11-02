import boto3
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, current_timestamp
from common_utils import trigger_glue_crawler

# Initialize Spark Session
spark = SparkSession.builder.appName("ETL Pipeline").getOrCreate()

# Define file paths
silver_path = "s3://sales-pyspark-etl/target_files/silver/sales/"
bronze_path = "s3://sales-pyspark-etl/target_files/bronze/sales/"
crawler_name = "silver_crawler"

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def transform_silver_layer(bronze_path, silver_path):
    try:
        logger.info("Starting Silver Layer ETL process.")

        df = spark.read.parquet(bronze_path)

        df_silver = (
            df.select(
                [col(c).alias(c.lower().replace(" ", "_")) for c in df.columns]
            )
            .withColumn("year", year("order_date"))
            .withColumn("month", month("order_date"))
            .withColumn("day", dayofmonth("order_date"))
        )

        df_silver.write.partitionBy("year", "month", "day").mode("overwrite").parquet(
            silver_path
        )
    except Exception as e:
        logger.error(f"Error in Silver Layer ETL process: {e}")
        raise


if __name__ == "__main__":
    # Execute Bronze layer ETL
    logger.info("Starting ETL Pipeline.")
    transform_silver_layer(bronze_path, silver_path)
    logger.info("ETL Pipeline executed successfully.")

    logger.info("Trigger glue crawler for silver table.")
    trigger_glue_crawler(crawler_name)
    logger.info("Crawler executed successfully.")
