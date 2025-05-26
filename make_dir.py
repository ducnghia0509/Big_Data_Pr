from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, LongType

spark = SparkSession.builder \
    .appName("CheckOHLCVQueryStatus") \
    .getOrCreate()

schema = StructType() \
    .add("symbol", StringType()) \
    .add("timestamp_ms", LongType()) \
    .add("@timestamp", StringType()) \
    .add("open", DoubleType()) \
    .add("high", DoubleType()) \
    .add("low", DoubleType()) \
    .add("close", DoubleType()) \
    .add("volume", DoubleType())

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "crypto_ohlcv_1m") \
    .option("startingOffsets", "latest") \
    .load()

df_parsed = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

query = df_parsed.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

import time
time.sleep(5)  # Chờ cho Spark query khởi động

print("===> QUERY STATUS:")
print(query.status)
query.stop()
