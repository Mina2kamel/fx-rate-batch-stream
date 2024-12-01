import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, broadcast, max as spark_max, current_timestamp, from_unixtime, to_timestamp, unix_timestamp
from scripts.utils.schema import FX_RATE_SCHEMA
from scripts.utils.utils import read_config

# Load configuration
CONFIG = read_config()

# Constants
POSTGRES_URL = CONFIG['postgres']['url']
POSTGRES_PROPERTIES = CONFIG['postgres']['properties']
STREAM_TABLE = CONFIG['postgres']['stream_table']
KAFKA_TOPIC = CONFIG['kafka']['KAFKA_TOPIC']
BOOTSTRAP_SERVER = CONFIG['kafka']['KAFKA_SERVER']
CHECKPOINT_PATH = CONFIG['kafka']['CHECKPOINT_PATH']

def create_spark_connection():
    """Create and return a Spark session."""
    try:
        spark = SparkSession.builder \
            .appName("SparkDataStreaming") \
            .config('spark.jars.packages', "org.postgresql:postgresql:42.7.4,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        logging.info("Spark session created successfully!")
        return spark
    except Exception as e:
        logging.error(f"Couldn't create Spark session due to: {e}")
        return None

def load_yesterday_rates(spark):
    """Simulate loading yesterday's 5 PM rates from a database."""
    try:
        # Replace this with actual database query logic
        return spark.createDataFrame([
            ("EURUSD", 1.08),
            ("GBPUSD", 1.25),
            ("USDJPY", 114.5),
            ("AUDUSD", 0.75),
            ("NZDUSD", 0.65)
        ], ["ccy_couple", "yesterday_rate"])
    except Exception as e:
        logging.error(f"Error loading yesterday's rates: {e}")
        return None

def connect_to_kafka(spark, kafka_servers, topic):
    """Connect to Kafka to read streaming data."""
    try:
        return spark.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', kafka_servers) \
            .option('subscribe', topic) \
            .option('startingOffsets', 'earliest') \
            .load()
    except Exception as e:
        logging.error(f"Error connecting to Kafka: {e}")
        return None

def parse_kafka_data(spark_df):
    """Extract and parse data from Kafka into a structured DataFrame."""
    try:
        parsed_df = spark_df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col('value'), FX_RATE_SCHEMA).alias('data')) \
            .select("data.*")
        logging.info("Parsed DataFrame created successfully!")
        return parsed_df
    except Exception as e:
        logging.error(f"Failed to parse Kafka data: {e}")
        return None

def process_fx_rates(fx_rates_df):
    """Process FX rates to filter and find the latest rates within the last 30 seconds."""
    try:
        # Convert event_time to timestamp
        fx_rates_with_timestamp = fx_rates_df.withColumn(
            "event_time", to_timestamp(from_unixtime(col("event_time") / 1000))
        )

        # Add a watermark to handle late data
        fx_rates_with_watermark = fx_rates_with_timestamp.withWatermark("event_time", "1 minute")

        # Get current timestamp as a Unix timestamp (seconds)
        current_time = unix_timestamp(current_timestamp())

        # Filter for rates within the last 30 seconds
        fx_rates_last_30s = fx_rates_with_watermark.filter(
            (current_time - unix_timestamp(col("event_time"))) <= 30
        )

        # Group by currency pair and get the latest rate within the last 30 seconds
        latest_rates = fx_rates_last_30s.groupBy(
            "ccy_couple"
        ).agg(
            spark_max("event_time").alias("latest_event_time"),
            expr("last(rate)").alias("current_rate")
        )
        return latest_rates
    except Exception as e:
        logging.error(f"Error processing FX rates: {e}")
        return None

def calculate_percentage_change(latest_rates, yesterday_rates_df):
    """Calculate the percentage change between latest and yesterday's rates using broadcast join."""
    try:
        if latest_rates is None or yesterday_rates_df is None:
            raise ValueError("Input DataFrames cannot be None")

        # Broadcast the smaller DataFrame (yesterday_rates_df)
        yesterday_rates_broadcasted = broadcast(yesterday_rates_df)

        # Join with yesterday's rates using broadcast join
        result = latest_rates.join(
            yesterday_rates_broadcasted, on="ccy_couple", how="left"
        ).withColumn(
            "percentage_change",
            expr("ROUND((current_rate - yesterday_rate) / yesterday_rate * 100, 3)")
        ).withColumn(
            "change",
            expr("CONCAT(ROUND((current_rate - yesterday_rate) / yesterday_rate * 100, 3), '%')")
        )

        # Select required columns
        return result.select(
            "ccy_couple", "current_rate", "change"
        ).withColumnRenamed("current_rate", "rate")
    except Exception as e:
        logging.error(f"Error calculating percentage change: {e}")
        return None

def write_to_postgres(batch_df, batch_id):
    """Writes data to PostgreSQL."""
    try:
        batch_df.write.jdbc(url=POSTGRES_URL, table=STREAM_TABLE, mode="overwrite", properties=POSTGRES_PROPERTIES)
        logging.info(f"Batch {batch_id} successfully written to PostgreSQL.")
    except Exception as e:
        logging.error(f"Error writing Batch {batch_id} to PostgreSQL: {e}")
        raise

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Create Spark session
    spark = create_spark_connection()

    if spark:
        # Load yesterday's rates
        yesterday_rates = load_yesterday_rates(spark)

        # Connect to Kafka and process data
        raw_rates = connect_to_kafka(spark, BOOTSTRAP_SERVER, KAFKA_TOPIC)

        if raw_rates:
            parsed_df = parse_kafka_data(raw_rates)
            # Process FX rates
            latest_rates = process_fx_rates(parsed_df)

            # Calculate percentage change
            results = calculate_percentage_change(latest_rates, yesterday_rates)

            # Write results to PostgreSQL
            query = latest_rates.writeStream \
                .outputMode("update") \
                .foreachBatch(write_to_postgres) \
                .option("checkpointLocation", CHECKPOINT_PATH) \
                .trigger(processingTime='30 seconds') \
                .start()

            query.awaitTermination()