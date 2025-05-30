from pyspark.sql import SparkSession
from pyspark.sql.functions import col, input_file_name, regexp_extract, lit
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType, TimestampType
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import os

# --- Configuration ---
HDFS_USER_ENV = os.environ.get('USER', 'hadoop') 
HDFS_BASE_PATH = f"hdfs://localhost:9000/user/{HDFS_USER_ENV}/crypto_project"

HDFS_INPUT_PATHS = [
    f"{HDFS_BASE_PATH}/raw_historical_data/*.csv", 
    f"{HDFS_BASE_PATH}/raw_hourly_updates/*.csv"  
]

ELASTICSEARCH_HISTORICAL_INDEX = "crypto_historical_data"
ELASTICSEARCH_NODE_WAN_ONLY = "false"

# Schema cho dữ liệu CSV đầu vào
schema = StructType([
    StructField("timestamp", LongType(), True), 
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", DoubleType(), True),
    StructField("datetime_str", StringType(), True) 
])

# Đường dẫn đến Elasticsearch Spark JAR
es_spark_jar_path = "./elasticsearch-spark-30_2.12-8.14.0.jar"

# --- Spark Session ---
print("Khởi tạo Spark Session cho Batch Processing...")
spark = SparkSession.builder \
    .appName("CryptoBatchProcessingHourly") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.elasticsearch.index.auto.create", "true") \
    .config("spark.es.nodes.wan.only", ELASTICSEARCH_NODE_WAN_ONLY) \
    .config("spark.es.nodes.discovery", "false") \
    .config("spark.es.net.ssl", "false") \
    .config("spark.es.nodes", "192.168.30.128") \
    .config("spark.es.port", "9200") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.jars", es_spark_jar_path) \
    .getOrCreate()
print("Spark Session đã được tạo.")

# --- Đọc dữ liệu từ HDFS ---
print(f"Đang đọc dữ liệu từ các đường dẫn HDFS: {HDFS_INPUT_PATHS}")
try:
    raw_df = spark.read.schema(schema).option("header", "true").csv(HDFS_INPUT_PATHS)
    raw_df = raw_df.withColumn("input_file", input_file_name())

    if raw_df.rdd.isEmpty():
        print("Không tìm thấy dữ liệu trong các đường dẫn HDFS. Kết thúc.")
        if 'spark' in locals() and spark.getActiveSession(): spark.stop()
        exit()

    processed_df = raw_df.withColumn("filename_only", regexp_extract(col("input_file"), r'([^/]+)$', 1))

    regex_pattern = r"^([A-Z0-9]+(?:_[A-Z0-9]+)*)_([0-9]+[a-zA-Z]+)(?:_update_.*)?\.csv$"

    processed_df = processed_df.withColumn("symbol_match", F.regexp_extract(col("filename_only"), regex_pattern, 1))
    processed_df = processed_df.withColumn("timeframe_match", F.regexp_extract(col("filename_only"), regex_pattern, 2))

    processed_df = processed_df.withColumn("symbol", col("symbol_match"))
    processed_df = processed_df.withColumn("timeframe", col("timeframe_match"))

    processed_df_filtered = processed_df.filter(
        (col("symbol") != "") & (col("timeframe") != "") & col("symbol").isNotNull() & col("timeframe").isNotNull()
    )

    print("DEBUG: Dữ liệu bị loại sau khi lọc symbol/timeframe không hợp lệ (nếu có):")
    processed_df.filter(
        ~((col("symbol") != "") & (col("timeframe") != "") & col("symbol").isNotNull() & col("timeframe").isNotNull())
    ).select("input_file", "filename_only", "symbol", "timeframe").show(truncate=False)

    print("DEBUG: Dữ liệu sau khi trích xuất và lọc symbol/timeframe:")
    processed_df_filtered.select("input_file", "filename_only", "symbol", "timeframe").show(50, truncate=False)
    
    if processed_df_filtered.rdd.isEmpty():
        print("Không có dữ liệu hợp lệ sau khi trích xuất symbol/timeframe. Kết thúc.")
        if 'spark' in locals() and spark.getActiveSession(): spark.stop()
        exit()

    df_for_timestamp_processing = processed_df_filtered 

    # --- Xử lý timestamp ---
    df_for_timestamp_processing = df_for_timestamp_processing.withColumn("event_datetime", (col("timestamp") / 1000).cast(TimestampType()))
    df_for_timestamp_processing = df_for_timestamp_processing.withColumn("timestamp_seconds_for_es", col("event_datetime").cast(LongType()))

    df_for_processing = df_for_timestamp_processing.select(
        col("event_datetime").alias("timestamp_dt"),         
        col("timestamp_seconds_for_es").alias("timestamp"),  
        col("symbol"),
        col("timeframe"),
        col("open").cast(DoubleType()),
        col("high").cast(DoubleType()),
        col("low").cast(DoubleType()),
        col("close").cast(DoubleType()),
        col("volume").cast(DoubleType())
    ).orderBy("symbol", "timeframe", "timestamp_dt")

    print("Schema dữ liệu TRƯỚC feature engineering (sau khi xử lý timestamp):")
    df_for_processing.printSchema()

    # --- Feature Engineering (SMA) ---
    print("Đang thực hiện Feature Engineering (SMA)...")
    window_spec_7 = Window.partitionBy("symbol", "timeframe").orderBy("timestamp_dt").rowsBetween(-6, 0)
    window_spec_30 = Window.partitionBy("symbol", "timeframe").orderBy("timestamp_dt").rowsBetween(-29, 0)

    df_with_features = df_for_processing.withColumn("sma_7", F.avg("close").over(window_spec_7))
    df_with_features = df_with_features.withColumn("sma_30", F.avg("close").over(window_spec_30))

    # --- Chuẩn bị DataFrame để ghi vào Elasticsearch ---
    df_to_save_es = df_with_features.select(
        "timestamp",
        "symbol",
        "timeframe",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "sma_7",
        "sma_30"
    )

    # Tạo doc_id (sử dụng cột 'timestamp' đã là số giây)
    df_to_save_es_with_id = df_to_save_es.withColumn(
        "doc_id",
        F.concat(col("symbol"), lit("_"), col("timeframe"), lit("_hist_"), col("timestamp"))
    )

    print(f"Chuẩn bị ghi/cập nhật vào Elasticsearch index: {ELASTICSEARCH_HISTORICAL_INDEX}")
    print("Schema dữ liệu SẼ ĐƯỢC GHI VÀO Elasticsearch:")
    df_to_save_es_with_id.printSchema()

    # Ghi vào Elasticsearch
    df_to_save_es_with_id.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.resource", ELASTICSEARCH_HISTORICAL_INDEX) \
        .option("es.mapping.id", "doc_id") \
        .option("es.write.operation", "upsert") \
        .mode("append") \
        .save()

    print(f"Đã ghi/cập nhật dữ liệu thành công vào Elasticsearch: {ELASTICSEARCH_HISTORICAL_INDEX}")

except Exception as e:
    print(f"Lỗi trong Batch Processing: {e}")
    import traceback
    traceback.print_exc()
finally:
    if 'spark' in locals() and spark.getActiveSession():
        spark.stop()
        print("Spark Session đã dừng.")
