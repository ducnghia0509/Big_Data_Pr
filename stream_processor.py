#!/usr/bin/env python3
"""
Stream Processor – consume 1‑minute OHLCV candles from Kafka, build three data
streams and write them to Elasticsearch:
  1.  latest 1‑minute candle per symbol           → index crypto_ohlcv_1m_latest
  2.  sliding‑window stats (avg / min / max)     → index crypto_ohlcv_1m_stats
  3.  raw 1‑minute candles for charting          → daily index crypto_ohlcv_1m_chartdata‑YYYY‑MM‑DD

The script is self‑contained: adjust the environment variables below or export
at runtime, then run:
    $ python stream_processor.py

Requirements:
  • Spark 3.4.x + PySpark 3.4.x
  • Kafka broker reachable on KAFKA_BROKER
  • Elasticsearch 8.x reachable on ELASTICSEARCH_HOST:ELASTICSEARCH_PORT
"""
from __future__ import annotations

import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import (
    avg, col, expr, from_json, max as spark_max, min as spark_min, window
)
from pyspark.sql.types import (
    DoubleType, LongType, StringType, StructField, StructType, TimestampType,
)

# ──────────────────────────────────────────────────────────────────────────────
# Configuration (env vars have priority – values below are sane defaults)
# ──────────────────────────────────────────────────────────────────────────────
KAFKA_BROKER               = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC                = os.getenv("KAFKA_OHLCV_1M_TOPIC", "crypto_ohlcv_1m")

ELASTICSEARCH_HOST         = os.getenv("ELASTICSEARCH_HOST", "localhost")
ELASTICSEARCH_PORT         = os.getenv("ELASTICSEARCH_PORT", "9200")
ES_NODES_WAN_ONLY          = os.getenv("ES_NODES_WAN_ONLY", "true")  # safe for Docker

ES_IDX_LATEST              = os.getenv("ES_IDX_LATEST", "crypto_ohlcv_1m_latest")
ES_IDX_STATS               = os.getenv("ES_IDX_STATS",  "crypto_ohlcv_1m_stats")
ES_IDX_CHART_PREFIX        = os.getenv("ES_IDX_CHART_PREFIX", "crypto_ohlcv_1m_chartdata")

WINDOW_DURATION            = os.getenv("OHLCV_WINDOW_DURATION", "10 minutes")  # stats window
SLIDE_DURATION             = os.getenv("OHLCV_SLIDE_DURATION",  "1 minute")     # slide step

# Local checkpoint directory (can be HDFS/S3)
CHECKPOINT_DIR_BASE        = os.getenv(
    "CHECKPOINT_BASE_PATH",
    "file:///tmp/crypto_ohlcv_1m_processor_checkpoint",
)

# ──────────────────────────────────────────────────────────────────────────────
# Schema – the JSON payload produced by streaming_producer.py
# ──────────────────────────────────────────────────────────────────────────────
OHLCV_SCHEMA = StructType([
    StructField("timestamp",   LongType(),   True),  # ms epoch (candle open time)
    StructField("symbol",      StringType(), True),
    StructField("timeframe",   StringType(), True),
    StructField("open",        DoubleType(), True),
    StructField("high",        DoubleType(), True),
    StructField("low",         DoubleType(), True),
    StructField("close",       DoubleType(), True),
    StructField("volume",      DoubleType(), True),
    StructField("datetime_str",StringType(), True),  # ISO string (optional)
])

# ──────────────────────────────────────────────────────────────────────────────
# Spark Session
# ──────────────────────────────────────────────────────────────────────────────
print("\n▶️  Initialising Spark session …")
SPARK_KAFKA_PKG = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.4"
ES_SPARK_PKG    = "org.elasticsearch:elasticsearch-spark-30_2.12:8.13.4"

spark = (
    SparkSession.builder
    .appName("CryptoOHLCV1mProcessing")
    .config("spark.jars.packages", f"{SPARK_KAFKA_PKG},{ES_SPARK_PKG}")
    .config("spark.sql.session.timeZone", "UTC")
    # Elasticsearch‑Hadoop connector settings
    .config("spark.es.nodes",            ELASTICSEARCH_HOST)
    .config("spark.es.port",             ELASTICSEARCH_PORT)
    .config("spark.es.nodes.wan.only",   ES_NODES_WAN_ONLY)
    .config("spark.es.nodes.discovery",  "false")
    .config("spark.es.net.ssl",          "false")
    .config("spark.elasticsearch.index.auto.create", "true")
    .getOrCreate()
)
print("✅  Spark session created.\n")

# ──────────────────────────────────────────────────────────────────────────────
# Read Kafka stream → structured dataframe
# ──────────────────────────────────────────────────────────────────────────────
print(f"➡️  Subscribing to Kafka topic '{KAFKA_TOPIC}' @ {KAFKA_BROKER}")
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

parsed_df = (
    raw_df.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), OHLCV_SCHEMA).alias("data"))
    .select(
        # normalise symbol: keep original with '/'; we replace later before writing
        col("data.symbol"),
        (col("data.timestamp") / 1000).cast(TimestampType()).alias("event_ts"),
        col("data.timestamp").alias("ts_ms"),
        col("data.open"), col("data.high"), col("data.low"),
        col("data.close").alias("close"),
        col("data.volume"),
    )
)

# Watermark (allow 2‑minute lateness)
watermarked = parsed_df.withWatermark("event_ts", "2 minutes")

# ──────────────────────────────────────────────────────────────────────────────
# 1️⃣  Latest candle per symbol – complete mode, small dimension table
# ──────────────────────────────────────────────────────────────────────────────
latest_df = (
    watermarked.groupBy("symbol")
    .agg(
        spark_max("event_ts").alias("latest_ts"),
        F.last("close").alias("current_price"),
        F.last("volume").alias("current_volume"),
        F.last("ts_ms").alias("ts_ms"),
        F.last("open").alias("open"),
        F.last("high").alias("high"),
        F.last("low").alias("low"),
    )
)

def write_latest(batch_df, epoch_id):
    if batch_df.rdd.isEmpty():
        return

    out = (
        batch_df
        .withColumn("symbol", col("symbol"))
        .withColumn("doc_id", col("symbol"))
        .select(
            "doc_id", "symbol", "latest_ts", "current_price", "current_volume",
            "open", "high", "low", "ts_ms",
        )
    )

    print(f"Epoch {epoch_id} – latest → ES ({out.count()} docs)")
    (out.write
        .format("org.elasticsearch.spark.sql")
        .option("es.resource", ES_IDX_LATEST)
        .option("es.mapping.id", "doc_id")
        .option("es.write.operation", "index")
        .mode("append")
        .save()
    )

latest_q = (
    latest_df.writeStream
    .outputMode("complete")
    .foreachBatch(write_latest)
    .option("checkpointLocation", f"{CHECKPOINT_DIR_BASE}_latest")
    .trigger(processingTime="15 seconds")
    .start()
)

# ──────────────────────────────────────────────────────────────────────────────
# 2️⃣  Sliding‑window stats (avg / min / max)
# ──────────────────────────────────────────────────────────────────────────────
stats_df = (
    watermarked.groupBy(
        col("symbol"),
        window(col("event_ts"), WINDOW_DURATION, SLIDE_DURATION).alias("w"),
    )
    .agg(
        avg("close").alias("avg_price"),
        spark_min("close").alias("min_price"),
        spark_max("close").alias("max_price"),
        F.count("close").alias("events"),
    )
    .select(
        col("symbol"),
        col("w.start").alias("window_start"),
        col("w.end").alias("window_end"),
        "avg_price", "min_price", "max_price", "events",
    )
)

def write_stats(batch_df, epoch_id):
    if batch_df.rdd.isEmpty():
        return
    out = (
        batch_df
        .withColumn("symbol", F.regexp_replace("symbol", "/", "-"))
        .withColumn("doc_id", expr("concat(symbol,'_stats_',cast(window_end as long))"))
    )
    print(f"Epoch {epoch_id} – stats → ES ({out.count()} docs)")
    (out.write.format("org.elasticsearch.spark.sql")
        .option("es.resource", ES_IDX_STATS)
        .option("es.mapping.id", "doc_id")
        .option("es.write.operation", "index")
        .mode("append")
        .save()
    )

stats_q = (
    stats_df.writeStream
    .outputMode("update")
    .foreachBatch(write_stats)
    .option("checkpointLocation", f"{CHECKPOINT_DIR_BASE}_stats")
    .trigger(processingTime="1 minute")
    .start()
)

# ──────────────────────────────────────────────────────────────────────────────
# 3️⃣  Raw 1‑minute candles for intraday chart (append‑only)
# ──────────────────────────────────────────────────────────────────────────────

def write_chart(batch_df, epoch_id):
    if batch_df.rdd.isEmpty():
        return

    out = (
        batch_df
        .withColumnRenamed("event_ts", "@timestamp")
        .withColumn("symbol", F.regexp_replace("symbol", "/", "-"))
        .withColumn("doc_id", expr("concat(symbol,'_1m_',cast(ts_ms as string))"))
        .select(
            "doc_id", "symbol", "@timestamp", "ts_ms",
            "open", "high", "low", col("close").alias("close"), "volume",
        )
    )

    today_idx = f"{ES_IDX_CHART_PREFIX}-{datetime.now(timezone.utc).strftime('%Y-%m-%d')}"
    print(f"Epoch {epoch_id} – chart → ES ({out.count()} docs) → {today_idx}")
    (out.write.format("org.elasticsearch.spark.sql")
        .option("es.resource", today_idx)
        .option("es.mapping.id", "doc_id")
        .option("es.write.operation", "index")
        .mode("append")
        .save()
    )

chart_q = (
    parsed_df.writeStream
    .outputMode("append")
    .foreachBatch(write_chart)
    .option("checkpointLocation", f"{CHECKPOINT_DIR_BASE}_chart")
    .trigger(processingTime="10 seconds")
    .start()
)

print("🚀  All streaming queries started. Press Ctrl+C to stop.\n")

try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("⏹️  Stopping queries …")
    for q in spark.streams.active:
        q.stop()
    spark.stop()
    print("✅  Stopped.")
