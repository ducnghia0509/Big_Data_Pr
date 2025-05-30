from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, window, avg, min, max, lit
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType
import os
from pyspark.sql import functions as F

from datetime import datetime, date, time, timedelta, timezone

# --- Configuration ---
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_OHLCV_1M_TOPIC = "crypto_ohlcv_1m"
ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST", "192.168.30.128")
ELASTICSEARCH_PORT = os.getenv("ELASTICSEARCH_PORT", "9200")

ELASTICSEARCH_OHLCV_STATS_INDEX = "crypto_ohlcv_1m_stats" 
ELASTICSEARCH_OHLCV_LATEST_INDEX = "crypto_ohlcv_1m_latest"
ELASTICSEARCH_CHART_1M_INDEX = "crypto_ohlcv_1m_chartdata"
ELASTICSEARCH_NODE_WAN_ONLY = os.getenv("ES_NODES_WAN_ONLY", "false")

CHECKPOINT_BASE_PATH = f"hdfs://localhost:9000/user/{os.environ.get('USER', 'hadoop')}/crypto_project/checkpoint/stream_ohlcv_1m_processor"
WINDOW_DURATION_FOR_STATS = os.getenv("OHLCV_WINDOW_DURATION", "10 minutes") 
SLIDE_DURATION_FOR_STATS = os.getenv("OHLCV_SLIDE_DURATION", "1 minute")   

# --- Schema cho dữ liệu OHLCV 1 phút từ Kafka ---
kafka_ohlcv_1m_schema = StructType([
    StructField("timestamp", LongType(), True), 
    StructField("symbol", StringType(), True),
    StructField("timeframe", StringType(), True), 
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", DoubleType(), True), 
    StructField("datetime_str", StringType(), True) # ISO datetime string
])

# --- Khởi tạo Spark Session ---
spark_kafka_pkg = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
es_spark_pkg = "org.elasticsearch:elasticsearch-spark-30_2.12:8.13.4"

print("Khởi tạo Spark Session cho OHLCV 1m Processing...")
spark = SparkSession.builder \
    .appName("CryptoOHLCV1mProcessing") \
    .config("spark.jars.packages", f"{spark_kafka_pkg},{es_spark_pkg}") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.elasticsearch.index.auto.create", "true") \
    .config("spark.es.nodes.wan.only", ELASTICSEARCH_NODE_WAN_ONLY) \
    .config("spark.es.nodes.discovery", "false") \
    .config("spark.es.net.ssl", "false") \
    .getOrCreate()
print("Spark Session đã được tạo.")

# --- Đọc dữ liệu OHLCV 1 phút từ Kafka ---
print(f"Đọc dữ liệu OHLCV 1m từ Kafka topic: {KAFKA_OHLCV_1M_TOPIC}")
kafka_ohlcv_1m_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_OHLCV_1M_TOPIC) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

parsed_ohlcv_df = kafka_ohlcv_1m_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), kafka_ohlcv_1m_schema).alias("data")) \
    .select(
        col("data.symbol").alias("symbol"),
        (col("data.timestamp") / 1000).cast(TimestampType()).alias("event_timestamp"), 
        col("data.timestamp").alias("timestamp_ms"), 
        col("data.open").alias("open"),
        col("data.high").alias("high"),
        col("data.low").alias("low"),
        col("data.close").alias("close_price"),
        col("data.volume").alias("volume")
    )

# --- Watermark ---
watermarked_ohlcv_df = parsed_ohlcv_df.withWatermark("event_timestamp", "2 minutes")

# --- 1. Dữ liệu cho Chart (Nến 1 phút gần nhất) và Latest Price/Volume ---
latest_ohlcv_candle_df = watermarked_ohlcv_df \
    .groupBy("symbol") \
    .agg(
        F.max("event_timestamp").alias("latest_event_timestamp"), 
        F.last("close_price").alias("current_price"),      
        F.last("volume").alias("current_volume"),         
        F.last("timestamp_ms").alias("timestamp_ms"),      
        F.last("open").alias("open"),
        F.last("high").alias("high"),
        F.last("low").alias("low")
    )

def write_latest_ohlcv_to_es(df, epoch_id):
    if df.rdd.isEmpty(): return
    # doc_id là symbol để dễ dàng get bằng ID
    df_to_write = df.withColumn("doc_id", col("symbol")) \
                    .select(
                        "doc_id", "symbol", "latest_event_timestamp", "current_price", "current_volume",
                        "open", "high", "low", "timestamp_ms" 
                    )
    print(f"Epoch {epoch_id} (Latest OHLCV 1m): Ghi {df_to_write.count()} nến mới nhất vào ES: {ELASTICSEARCH_OHLCV_LATEST_INDEX}")
    df_to_write.write.format("org.elasticsearch.spark.sql") \
        .option("es.resource", ELASTICSEARCH_OHLCV_LATEST_INDEX) \
        .option("es.mapping.id", "doc_id").option("es.write.operation", "index") \
        .option("es.nodes", ELASTICSEARCH_HOST).option("es.port", ELASTICSEARCH_PORT) \
        .mode("append").save() 

query_latest_ohlcv_es = latest_ohlcv_candle_df.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_latest_ohlcv_to_es) \
    .option("checkpointLocation", CHECKPOINT_BASE_PATH + "_latest_ohlcv") \
    .trigger(processingTime="15 seconds").start() 

# --- 2. Dữ liệu cho các thẻ thống kê (Avg, Min, Max Price Window) ---
ohlcv_stats_df = watermarked_ohlcv_df \
    .groupBy(
        col("symbol"),
        window(col("event_timestamp"), WINDOW_DURATION_FOR_STATS, SLIDE_DURATION_FOR_STATS).alias("time_window")
    ) \
    .agg(
        avg("close_price").alias("avg_price"),
        min("close_price").alias("min_price"),
        max("close_price").alias("max_price"),
        F.count("close_price").alias("event_count_in_window") 
    ) \
    .select(
        col("symbol"),
        col("time_window.start").alias("window_start"),
        col("time_window.end").alias("window_end"),
        "avg_price", "min_price", "max_price", "event_count_in_window"
    )

def write_ohlcv_stats_to_es(df, epoch_id):
    if df.rdd.isEmpty(): return
    df_to_write = df.withColumn("doc_id", expr("concat(replace(symbol, '/', '-'), '_stats_', cast(window_end as long))"))
    print(f"Epoch {epoch_id} (OHLCV 1m Stats): Ghi {df_to_write.count()} thống kê cửa sổ vào ES: {ELASTICSEARCH_OHLCV_STATS_INDEX}")
    df_to_write.write.format("org.elasticsearch.spark.sql") \
        .option("es.resource", ELASTICSEARCH_OHLCV_STATS_INDEX) \
        .option("es.mapping.id", "doc_id").option("es.write.operation", "index") \
        .option("es.nodes", ELASTICSEARCH_HOST).option("es.port", ELASTICSEARCH_PORT) \
        .mode("append").save()

query_ohlcv_stats_es = ohlcv_stats_df.writeStream \
    .outputMode("update") \
    .foreachBatch(write_ohlcv_stats_to_es) \
    .option("checkpointLocation", CHECKPOINT_BASE_PATH + "_ohlcv_stats") \
    .trigger(processingTime="1 minute").start() 

# --- 3. Dữ liệu thô OHLCV 1 phút cho biểu đồ (ghi liên tục) ---
def write_raw_ohlcv_for_chart_to_es(df, epoch_id):
    if df.rdd.isEmpty(): return
    df_with_es_timestamp = df.withColumnRenamed("event_timestamp", "@timestamp")

    # Lấy ngày hiện tại để tạo tên index động
    current_date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    target_index_name = f"{ELASTICSEARCH_CHART_1M_INDEX}-{current_date_str}"
    df_to_write = df_with_es_timestamp.withColumn("doc_id",
        expr("concat(replace(symbol, '/', '-'), '_1m_', cast(timestamp_ms as string))")
    )
    df_to_write = df_to_write.select(
        "doc_id", "symbol", "@timestamp", "timestamp_ms", 
        col("open"), col("high"), col("low"), col("close_price").alias("close"),
        col("volume")
    )
    print(f"Epoch {epoch_id} (Raw OHLCV 1m for Chart): Ghi {df_to_write.count()} nến vào ES: {target_index_name}")
    df_to_write.write.format("org.elasticsearch.spark.sql") \
        .option("es.resource", target_index_name) \
        .option("es.mapping.id", "doc_id").option("es.write.operation", "index") \
        .option("es.nodes", ELASTICSEARCH_HOST).option("es.port", ELASTICSEARCH_PORT) \
        .mode("append").save()

query_raw_ohlcv_for_chart_es = parsed_ohlcv_df.writeStream \
    .outputMode("append") \
    .foreachBatch(write_raw_ohlcv_for_chart_to_es) \
    .option("checkpointLocation", CHECKPOINT_BASE_PATH + "_raw_ohlcv_chart") \
    .trigger(processingTime="15 seconds").start()

print("Tất cả các streaming queries OHLCV 1m đã bắt đầu. Nhấn Ctrl+C để dừng.")
spark.streams.awaitAnyTermination()
