import logging
import argparse
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import col, expr, current_timestamp, row_number, broadcast
from scripts.utils.spark_manager import create_spark_connection
from scripts.utils.schema import FX_RATE_SCHEMA
from scripts.utils.utils import read_config

# Load configuration
CONFIG = read_config()

# Constants
POSTGRES_URL = CONFIG['postgres']['url']
POSTGRES_PROPERTIES = CONFIG['postgres']['properties']
S3_RAW_BUCKET = CONFIG['minio']['raw_bucket']
PROCESSED_RATES_TABLE = CONFIG['postgres']['processed_table']
REFERENCE_RATES_TABLE = CONFIG['postgres']['reference_table']

def load_fx_data(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Load raw FX rate data from S3.

    Args:
        spark (SparkSession): Spark session object.
        file_path (str): Path to the FX data file in S3.

    Returns:
        DataFrame: Spark DataFrame with the loaded data.
    """
    try:
        return spark.read.schema(FX_RATE_SCHEMA).parquet(file_path)
    except Exception as e:
        logging.error(f"Failed to load FX data from {file_path}: {e}")
        raise

def process_fx_rates(fx_rates_df: DataFrame) -> DataFrame:
    """
    Filter and process FX rates to identify the latest rates.

    Args:
        fx_rates_df (DataFrame): DataFrame containing FX rate data.

    Returns:
        DataFrame: Processed DataFrame with the latest rates.
    """
    try:
        active_rates = fx_rates_df.withColumn("current_time", current_timestamp()) \
            .filter((col("current_time").cast("long") - col("event_time").cast("long")) <= 3000000)

        window_spec = Window.partitionBy("ccy_couple").orderBy(col("event_time").desc())
        return active_rates.withColumn("row_number", row_number().over(window_spec)) \
            .filter(col("row_number") == 1) \
            .select("ccy_couple", "event_time", "rate") \
            .withColumnRenamed("event_time", "latest_event_time") \
            .withColumnRenamed("rate", "current_rate")
    except Exception as e:
        logging.error(f"Error processing FX rates: {e}")
        raise

def calculate_percentage_change(latest_rates: DataFrame, yesterday_rates_df: DataFrame) -> DataFrame:
    """
    Calculate percentage change between the latest and yesterday's rates using a broadcast join.

    Args:
        latest_rates (DataFrame): DataFrame with the latest FX rates.
        yesterday_rates_df (DataFrame): DataFrame with yesterday's FX rates.

    Returns:
        DataFrame: DataFrame with calculated percentage changes.
    """
    try:
        # Rename column for clarity in join
        yesterday_rates_df = yesterday_rates_df.withColumnRenamed("current_rate", "yesterday_rate")

        # Perform broadcast join for optimization
        result = latest_rates.alias("latest").join(
            broadcast(yesterday_rates_df.alias("yesterday")), "ccy_couple", "inner"
        ).withColumn(
            "percentage_change",
            expr("ROUND((latest.current_rate - yesterday.yesterday_rate) / yesterday.yesterday_rate * 100, 3)")
        ).withColumn(
            "change",
            expr("CONCAT(ROUND((latest.current_rate - yesterday.yesterday_rate) / yesterday.yesterday_rate * 100, 3), '%')")
        )

        # Select and rename columns for the output
        return result.select(
            "ccy_couple",
            expr("latest.current_rate AS rate"),
            "change"
        )
    except Exception as e:
        logging.error(f"Error calculating percentage change: {e}")
        raise

def load_reference_rates(spark: SparkSession, table: str) -> DataFrame:
    """
    Load yesterday's FX rates from PostgreSQL.

    Args:
        spark (SparkSession): Spark session object.
        table (str): Name of the PostgreSQL table.

    Returns:
        DataFrame: DataFrame containing yesterday's FX rates.
    """
    try:
        return spark.read.jdbc(url=POSTGRES_URL, table=table, properties=POSTGRES_PROPERTIES)
    except Exception as e:
        logging.error(f"Failed to load yesterday's rates from {table}: {e}")
        raise

def write_to_postgres(df: DataFrame, table: str, mode: str = "overwrite") -> None:
    """
    Write data to PostgreSQL.

    Args:
        df (DataFrame): Spark DataFrame to write.
        table (str): PostgreSQL table name.
        mode (str, optional): Write mode (default is "overwrite").
    """
    try:
        df.write.jdbc(url=POSTGRES_URL, table=table, mode=mode, properties=POSTGRES_PROPERTIES)
        logging.info(f"Data successfully written to {table}.")
    except Exception as e:
        logging.error(f"Error writing data to {table}: {e}")
        raise

def process_fx_data(file_name: str) -> None:
    """
    Main function to process the FX rates.

    Args:
        file_name (str): Name of the FX data file.
    """
    spark = create_spark_connection()
    if not spark:
        logging.error("Failed to create Spark session. Exiting...")
        return

    try:
        # Load raw FX data
        raw_data_path = f"s3a://{S3_RAW_BUCKET}/{file_name}"
        fx_rates_df = load_fx_data(spark, raw_data_path)
        logging.info("Raw FX data loaded successfully.")

        # Process FX rates
        latest_rates_df = process_fx_rates(fx_rates_df)

        # Write yesterday's rates if appropriate
        hour_part = file_name.split("_")[3].split(".")[0]
        if hour_part.startswith("17-00"):
            write_to_postgres(latest_rates_df, REFERENCE_RATES_TABLE)

        # Load yesterday's rates and calculate percentage change
        reference_rates_df = load_reference_rates(spark, REFERENCE_RATES_TABLE)
        processed_rates_df = calculate_percentage_change(latest_rates_df, reference_rates_df)

        # Save the processed data
        write_to_postgres(processed_rates_df, PROCESSED_RATES_TABLE)

    except Exception as e:
        logging.error(f"An error occurred during FX data processing: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process FX data.")
    parser.add_argument("file_name", type=str, help="Path to the FX data file.")
    args = parser.parse_args()
    process_fx_data(args.file_name)
