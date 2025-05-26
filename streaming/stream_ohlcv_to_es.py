from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, LongType

# Khởi tạo SparkSession
spark = SparkSession.builder \
    .appName("KafkaToElasticsearch_OHLCV_1m") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,"
            "org.elasticsearch:elasticsearch-spark-30_2.12:8.13.4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Định nghĩa schema cho JSON từ Kafka
schema = StructType() \
    .add("symbol", StringType()) \
    .add("timestamp_ms", LongType()) \
    .add("@timestamp", StringType()) \
    .add("open", DoubleType()) \
    .add("high", DoubleType()) \
    .add("low", DoubleType()) \
    .add("close", DoubleType()) \
    .add("volume", DoubleType())

# Đọc dữ liệu từ Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "crypto_ohlcv_1m") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON
df_json = df_kafka.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json("json", schema).alias("data")) \
    .select("data.*")

# Ghi dữ liệu vào Elasticsearch
def write_to_es(batch_df, batch_id):
    batch_df.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", "localhost") \
        .option("es.port", "9200") \
        .option("es.nodes.wan.only", "true") \
        .option("es.resource", "crypto-ohlcv") \
        .mode("append") \
        .save()

# Ghi bằng foreachBatch
query = df_json.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_es) \
    .option("checkpointLocation", "./checkpoint/stream_ohlcv_to_es") \
    .start()

query.awaitTermination()
