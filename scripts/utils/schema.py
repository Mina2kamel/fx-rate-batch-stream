from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType

# FX Rate schema definition
FX_RATE_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_time", LongType(), True),
    StructField("ccy_couple", StringType(), True),
    StructField("rate", DoubleType(), True),
])
