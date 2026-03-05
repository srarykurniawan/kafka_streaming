#!/usr/bin/env python3
"""
PySpark Structured Streaming Job
─────────────────────────────────────────────────────────────────────────────
Pipeline:
  Kafka [transactions]
      → Validasi (5 aturan wajib + watermark late-event detection)
      → Routing: valid → [transactions_valid]  |  invalid → [transactions_dlq]
      → Tumbling Window (1 menit) + running total → Console
─────────────────────────────────────────────────────────────────────────────
"""

import os
import sys
import json
import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, TimestampType, BooleanType
)

# ─── Logging ─────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ─── Configuration ────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC_INPUT             = "transactions"
TOPIC_VALID             = "transactions_valid"
TOPIC_DLQ               = "transactions_dlq"
WATERMARK_DELAY         = "3 minutes"
WINDOW_DURATION         = "1 minute"
CHECKPOINT_BASE         = "/app/output_logs/checkpoints"
VALID_SOURCES           = {"mobile", "web", "pos"}
AMOUNT_MIN              = 1.0
AMOUNT_MAX              = 10_000_000.0

# ─── Schema Definition ───────────────────────────────────────────────────────────
TRANSACTION_SCHEMA = StructType([
    StructField("event_id",  StringType(),    True),
    StructField("user_id",   StringType(),    True),
    StructField("amount",    DoubleType(),    True),
    StructField("timestamp", StringType(),    True),   # raw string dulu
    StructField("source",    StringType(),    True),
])

# ─── SparkSession ────────────────────────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("TransactionStreamingPipeline")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1"
        )
        .getOrCreate()
    )

# ─── Read from Kafka ──────────────────────────────────────────────────────────────

def read_kafka_stream(spark: SparkSession) -> DataFrame:
    """
    Membaca stream dari Kafka topic [transactions].
    Deserialize JSON → DataFrame dengan schema terdefinisi.
    """
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC_INPUT)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Decode value bytes → string → parse JSON
    parsed = raw.select(
        F.col("offset").alias("kafka_offset"),
        F.col("partition").alias("kafka_partition"),
        F.col("timestamp").alias("kafka_ingest_time"),
        F.from_json(
            F.col("value").cast("string"),
            TRANSACTION_SCHEMA
        ).alias("data")
    ).select("kafka_offset", "kafka_partition", "kafka_ingest_time", "data.*")

    return parsed

# ─── Validation Logic ─────────────────────────────────────────────────────────────

def apply_validations(df: DataFrame) -> DataFrame:
    """
    5 Validasi Wajib:
      V1. Mandatory field check  → user_id, amount, timestamp tidak null/kosong
      V2. Type validation        → amount harus numerik (sudah di-cast DoubleType)
      V3. Range validation       → amount antara 1 dan 10.000.000
      V4. Source validation      → harus "mobile", "web", atau "pos"
      V5. Duplicate detection    → user_id + timestamp kombinasi unik (dedup di watermark window)

    Tambahkan kolom: is_valid (BooleanType), error_reason (StringType)
    """
    valid_sources_list = list(VALID_SOURCES)

    # ── V1: Mandatory fields ──────────────────────────────────────────────────
    v1_fail = (
        F.col("user_id").isNull()   | (F.trim(F.col("user_id"))   == "") |
        F.col("amount").isNull()    |
        F.col("timestamp").isNull() | (F.trim(F.col("timestamp")) == "")
    )
    v1_reason = F.when(
        F.col("user_id").isNull() | (F.trim(F.col("user_id")) == ""),
        F.lit("MISSING_USER_ID")
    ).when(
        F.col("amount").isNull(),
        F.lit("MISSING_AMOUNT")
    ).when(
        F.col("timestamp").isNull() | (F.trim(F.col("timestamp")) == ""),
        F.lit("MISSING_TIMESTAMP")
    )

    # ── V2 + V3: Type & Range validation ─────────────────────────────────────
    # Amount sudah di-cast ke DoubleType; null setelah cast = type error
    v2_fail   = F.col("amount").isNull()
    v2_reason = F.when(v2_fail, F.lit("INVALID_AMOUNT_TYPE"))

    v3_fail   = (~v2_fail) & ((F.col("amount") < AMOUNT_MIN) | (F.col("amount") > AMOUNT_MAX))
    v3_reason = F.when(v3_fail, F.lit(f"AMOUNT_OUT_OF_RANGE({AMOUNT_MIN}-{AMOUNT_MAX})"))

    # ── V4: Source validation ─────────────────────────────────────────────────
    v4_fail   = ~F.col("source").isin(valid_sources_list)
    v4_reason = F.when(v4_fail, F.concat(F.lit("INVALID_SOURCE:"), F.col("source")))

    # ── Timestamp parsing & Watermark column ─────────────────────────────────
    # Parse timestamp string → TimestampType (null jika format salah = V2/invalid)
    df = df.withColumn(
        "event_time",
        F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
    )

    # Invalid timestamp = null after parsing
    ts_fail   = F.col("event_time").isNull() & F.col("timestamp").isNotNull() & (F.trim(F.col("timestamp")) != "")
    ts_reason = F.when(ts_fail, F.lit("INVALID_TIMESTAMP_FORMAT"))

    # ── Gabungkan semua validasi ──────────────────────────────────────────────
    any_fail = v1_fail | v2_fail | v3_fail | v4_fail | ts_fail

    # error_reason: ambil pesan pertama yang cocok (prioritas)
    error_reason = (
        F.coalesce(v1_reason, v2_reason, ts_reason, v3_reason, v4_reason)
    )

    df = df.withColumn("is_valid",     ~any_fail) \
           .withColumn("error_reason",  F.when(any_fail, error_reason).otherwise(F.lit(None).cast(StringType())))

    # ── Apply Watermark (untuk late-event detection & V5 dedup) ──────────────
    # Hanya terapkan ke baris yang event_time-nya valid (tidak null)
    df = df.withColumn(
        "event_time",
        F.when(F.col("event_time").isNull(), F.current_timestamp()).otherwise(F.col("event_time"))
    )
    df = df.withWatermark("event_time", WATERMARK_DELAY)

    return df

# ─── Routing dengan foreachBatch ──────────────────────────────────────────────────

# Akumulator running total (module-level)
_running_total: int = 0

def route_and_output(batch_df: DataFrame, batch_id: int) -> None:
    """
    foreachBatch handler:
      1. Route valid   → Kafka [transactions_valid]
      2. Route invalid → Kafka [transactions_dlq]
      3. Hitung Tumbling Window 1 menit
      4. Output running_total ke console
    """
    global _running_total

    if batch_df.isEmpty():
        logger.info("[Batch %d] Empty batch, skipping.", batch_id)
        return

    # ── Deteksi Late Events (event_time < watermark = sudah lewat) ────────────
    # Tandai baris yg timestamp jauh di masa lalu sebagai late
    cutoff = F.current_timestamp() - F.expr("INTERVAL 3 MINUTES")
    batch_df = batch_df.withColumn(
        "is_late",
        (F.col("event_time") < cutoff) & F.col("is_valid")
    ).withColumn(
        "is_valid",
        F.when(F.col("is_late"), F.lit(False)).otherwise(F.col("is_valid"))
    ).withColumn(
        "error_reason",
        F.when(
            F.col("is_late"),
            F.lit("LATE_EVENT_EXCEEDS_WATERMARK")
        ).otherwise(F.col("error_reason"))
    )

    # ── V5: Duplicate Detection (user_id + timestamp) ────────────────────────
    from pyspark.sql.window import Window
    dedup_window = Window.partitionBy("user_id", "timestamp").orderBy("kafka_offset")
    batch_df = batch_df.withColumn("_row_num", F.row_number().over(dedup_window))
    batch_df = batch_df.withColumn(
        "is_duplicate", (F.col("_row_num") > 1) & F.col("is_valid")
    ).withColumn(
        "is_valid",
        F.when(F.col("is_duplicate"), F.lit(False)).otherwise(F.col("is_valid"))
    ).withColumn(
        "error_reason",
        F.when(F.col("is_duplicate"), F.lit("DUPLICATE_EVENT")).otherwise(F.col("error_reason"))
    ).drop("_row_num", "is_duplicate")

    batch_df.cache()

    # ── Split valid / invalid ─────────────────────────────────────────────────
    valid_df   = batch_df.filter(F.col("is_valid") == True)
    invalid_df = batch_df.filter(F.col("is_valid") == False)

    valid_count   = valid_df.count()
    invalid_count = invalid_df.count()

    # ── Publish valid → Kafka ─────────────────────────────────────────────────
    if valid_count > 0:
        (
            valid_df
            .select(
                F.col("user_id").alias("key"),
                F.to_json(F.struct(
                    "event_id", "user_id", "amount", "timestamp", "source", "is_valid"
                )).alias("value")
            )
            .write
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("topic", TOPIC_VALID)
            .save()
        )

    # ── Publish invalid → Kafka DLQ ───────────────────────────────────────────
    if invalid_count > 0:
        (
            invalid_df
            .select(
                F.col("user_id").alias("key"),
                F.to_json(F.struct(
                    "event_id", "user_id", "amount", "timestamp", "source",
                    "is_valid", "error_reason"
                )).alias("value")
            )
            .write
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("topic", TOPIC_DLQ)
            .save()
        )

    # ── Tumbling Window (1 menit) ─────────────────────────────────────────────
    window_agg = (
        valid_df
        .groupBy(
            F.window(F.col("event_time"), WINDOW_DURATION).alias("window")
        )
        .agg(
            F.count("*").alias("transactions_in_window")
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("transactions_in_window")
        )
    )

    # ── Update Running Total ──────────────────────────────────────────────────
    _running_total += valid_count
    spark_output_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # ── Console Output ────────────────────────────────────────────────────────
    print("\n" + "═" * 70)
    print(f"  BATCH ID         : {batch_id}")
    print(f"  Spark Output Time: {spark_output_time}")
    print(f"  Valid Events     : {valid_count}")
    print(f"  Invalid/DLQ      : {invalid_count}")
    print(f"  Running Total    : {_running_total}")
    print("═" * 70)

    print("\n[Tumbling Window - 1 Minute Aggregation]")
    print(f"{'Window Start':<26} {'Window End':<26} {'Transactions':>12}")
    print("-" * 66)
    if window_agg.count() > 0:
        for row in window_agg.orderBy("window_start").collect():
            print(f"{str(row.window_start):<26} {str(row.window_end):<26} {row.transactions_in_window:>12}")
    else:
        print("  (no valid events in this batch)")

    print("\n[Console Output Format (required columns)]")
    print(f"{'timestamp':<30} {'running_total':>15}")
    print("-" * 47)
    print(f"{spark_output_time:<30} {_running_total:>15}")

    print("\n[Batch Summary - All Events]")
    batch_df.select(
        "user_id", "amount", "source", "timestamp",
        "is_valid", "error_reason", "is_late"
    ).show(truncate=False)

    batch_df.unpersist()
    logger.info(
        "[Batch %d] Done | valid=%d | invalid=%d | running_total=%d",
        batch_id, valid_count, invalid_count, _running_total
    )

# ─── Main ─────────────────────────────────────────────────────────────────────────

def main() -> None:
    global spark

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    logger.info("=== Transaction Streaming Job Starting ===")
    logger.info("Reading from Kafka topic  : %s", TOPIC_INPUT)
    logger.info("Valid output topic         : %s", TOPIC_VALID)
    logger.info("DLQ topic                  : %s", TOPIC_DLQ)
    logger.info("Watermark                  : %s", WATERMARK_DELAY)
    logger.info("Tumbling Window            : %s", WINDOW_DURATION)

    # 1. Read from Kafka
    raw_df = read_kafka_stream(spark)

    # 2. Apply validations (5 wajib) + watermark
    validated_df = apply_validations(raw_df)

    # 3. Start stream with foreachBatch routing
    query = (
        validated_df
        .writeStream
        .outputMode("update")
        .foreachBatch(route_and_output)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/main")
        .trigger(processingTime="10 seconds")
        .start()
    )

    logger.info("Streaming query started. Waiting for data...")
    query.awaitTermination()

if __name__ == "__main__":
    main()
