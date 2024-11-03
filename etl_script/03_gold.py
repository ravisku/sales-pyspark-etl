import logging

import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name

# Initialize Spark Session
spark = SparkSession.builder.appName("ETL Pipeline").getOrCreate()

# Define file paths
silver_path = "s3://sales-pyspark-etl/target_files/silver/sales/"
gold_path = "s3://sales-pyspark-etl/target_files/gold/"
crawler_name = "gold_crawler"
customer_table_name = "customer"
sales_table_name = "sales"
latest_ate = "2018-12-30"

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_sales_table(silver_path, gold_path):
    try:
        logger.info("Starting Gold Layer ETL process.")

        # Read source CSV file(s)
        logger.info(f"Reading data from silver path: {silver_path}")

        sales_table_path = f"gold_path{sales_table_name}/"

        df = spark.read.parquet(silver_path)

        df.createOrReplaceTempView("sales_tmp_table")

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
                            1,
                            2,
                            3,
                            4,
                            5   
                    """
        )

        df.write.partitionBy("order_date").mode("overwrite").option(
            "partitionOverwriteMode", "dynamic"
        ).parquet(sales_table_path)

        logger.info("Bronze Layer ETL process completed successfully.")
    except Exception as e:
        logger.error(f"Error in Bronze Layer ETL process: {e}")
        raise


def get_latest_date(silver_path):
    try:

        # Read the table schema and partition columns only
        df = spark.read.format("parquet").load(silver_path)

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

        return latest_date

    except Exception as e:
        logger.error(f"Error in Bronze Layer ETL process: {e}")
        raise


def load_customer_table(silver_path, gold_path):
    try:
        logger.info("Starting Gold Layer ETL process.")

        # Read source CSV file(s)
        logger.info(f"Reading data from silver path: {silver_path}")

        customer_table_path = f"gold_path{customer_table_name}/"

        df = spark.read.parquet(silver_path)

        df.createOrReplaceTempview("customer_tmp_table")

        query = f"""
                SELECT
                    customer_id,
                    split_part(customer_name, ' ', 1) AS customer_first_name,
                    split_part(customer_name, ' ', 2) AS customer_last_name,
                    segment,
                    country,
                    COUNT( DISTINCT 
                    CASE
                        WHEN
                            order_date BETWEEN {latest_date}::DATE - 30 AND {latest_date} 
                        THEN
                            order_id 
                        ELSE
                            NULL 
                    END
                ) AS order_quantity_last_30_days, COUNT( DISTINCT 
                    CASE
                        WHEN
                            order_date BETWEEN {latest_date}::DATE - 180 AND {latest_date} 
                        THEN
                            order_id 
                        ELSE
                            NULL 
                    END
                ) AS order_quantity_last_6_months, COUNT( DISTINCT 
                    CASE
                        WHEN
                            order_date BETWEEN {latest_date}::DATE - 365 AND {latest_date} 
                        THEN
                            order_id 
                        ELSE
                            NULL 
                    END
                ) AS order_quantity_last_12_months, COUNT(DISTINCT order_id) AS total_quantity_of_orders 
                FROM
                    customer_tmp_table 
                GROUP BY
                    1, 2, 3, 4, 5
                """

        df = spark.sql(query)

        df.write.mode("overwrite").parquet(customer_table_path)

        logger.info("Bronze Layer ETL process completed successfully.")
    except Exception as e:
        logger.error(f"Error in Bronze Layer ETL process: {e}")
        raise


def load_gold_layer(silver_path, gold_path):

    load_sales_table(silver_path, gold_path)
    latest_date = get_latest_date(silver_path)
    load_customer_table(silver_path, gold_path, latest_date)


if __name__ == "__main__":
    # Execute Gold layer ETL
    logger.info("Starting ETL Pipeline.")
    load_gold_layer(silver_path, gold_path)
    logger.info("ETL Pipeline executed successfully.")

    logger.info("Trigger glue crawler for gold table.")
    trigger_glue_crawler(crawler_name)
    logger.info("Crawler executed successfully.")
