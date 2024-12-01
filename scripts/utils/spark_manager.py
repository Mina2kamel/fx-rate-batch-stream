import logging
from pyspark.sql import SparkSession

def create_spark_connection():
    """Creates and returns a Spark session."""
    try:
        spark = (SparkSession.builder
                 .appName("FXDataBatchProcessing")
                 .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
                 .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
                 .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
                 .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                 .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                 .config("spark.hadoop.fs.s3a.path.style.access", "true")
                 .config("spark.jars", "/opt/spark/jars/hadoop-aws-3.2.2.jar,/opt/spark/jars/postgresql-42.7.4.jar")
                 .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                         "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
                 .getOrCreate())
        spark.sparkContext.setLogLevel("ERROR")
        logging.info("Spark session created successfully!")
        return spark
    except Exception as e:
        logging.error(f"Error creating Spark session: {e}")
        return None