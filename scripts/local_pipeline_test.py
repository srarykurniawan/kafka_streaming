#!/usr/bin/env python3
"""
local_pipeline_test.py
──────────────────────────────────────────────────────────────────────────────
Test runner lokal untuk memvalidasi logika pipeline TANPA memerlukan
Kafka / Spark / Docker.

Cara jalankan:
  pip install pyspark
  python3 scripts/local_pipeline_test.py
──────────────────────────────────────────────────────────────────────────────
"""

import sys
import json
from datetime import datetime, timezone, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType
)

# ─── Sample Data ──────────────────────────────────────────────────────────────

def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def late_iso(minutes=5):
    return (datetime.now(timezone.utc) - timedelta(minutes=minutes))\
           .strftime("%Y-%m-%dT%H:%M:%SZ")

SAMPLE_EVENTS = [
    # === VALID ===
    {"event_id": "E001", "user_id": "U11111", "amount": 150000.0,    "timestamp": now_iso(),          "source": "mobile"},
    {"event_id": "E002", "user_id": "U22222", "amount": 9999999.0,   "timestamp": now_iso(),          "source": "web"},
    {"event_id": "E003", "user_id": "U33333", "amount": 1.0,         "timestamp": now_iso(),          "source": "pos"},
    {"event_id": "E004", "user_id": "U44444", "amount": 500000.0,    "timestamp": now_iso(),          "source": "mobile"},
    {"event_id": "E005", "user_id": "U55555", "amount": 75000.0,     "timestamp": now_iso(),          "source": "web"},

    # === INVALID: amount negatif ===
    {"event_id": "E006", "user_id": "U66666", "amount": -500.0,      "timestamp": now_iso(),          "source": "mobile"},
    # === INVALID: amount terlalu besar ===
    {"event_id": "E007", "user_id": "U77777", "amount": 50000000.0,  "timestamp": now_iso(),          "source": "web"},
    # === INVALID: timestamp tidak valid ===
    {"event_id": "E008", "user_id": "U88888", "amount": 100000.0,    "timestamp": "not-a-timestamp",  "source": "pos"},
    # === INVALID: source tidak dikenal ===
    {"event_id": "E009", "user_id": "U99999", "amount": 200000.0,    "timestamp": now_iso(),          "source": "telegram"},
    # === INVALID: user_id kosong ===
    {"event_id": "E010", "user_id": "",        "amount": 30000.0,    "timestamp": now_iso(),          "source": "mobile"},
    # === INVALID: amount nol ===
    {"event_id": "E011", "user_id": "U11112", "amount": 0.0,         "timestamp": now_iso(),          "source": "web"},

    # === DUPLICATE (event_id berbeda tapi user_id+timestamp sama) ===
    {"event_id": "E012", "user_id": "U55555", "amount": 75000.0,     "timestamp": now_iso(),          "source": "web"},

    # === LATE EVENTS (> 3 menit) ===
    {"event_id": "E013", "user_id": "U11113", "amount": 88000.0,     "timestamp": late_iso(4),        "source": "mobile"},
    {"event_id": "E014", "user_id": "U11114", "amount": 120000.0,    "timestamp": late_iso(10),       "source": "pos"},
    {"event_id": "E015", "user_id": "U11115", "amount": 55000.0,     "timestamp": late_iso(15),       "source": "web"},
]

# ─── Validation Logic (mirror dari streaming_job.py) ─────────────────────────

VALID_SOURCES = {"mobile", "web", "pos"}
AMOUNT_MIN    = 1.0
AMOUNT_MAX    = 10_000_000.0

def run_validation_test():
    spark = (
        SparkSession.builder
        .appName("LocalPipelineTest")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    schema = StructType([
        StructField("event_id",  StringType(), True),
        StructField("user_id",   StringType(), True),
        StructField("amount",    DoubleType(), True),
        StructField("timestamp", StringType(), True),
        StructField("source",    StringType(), True),
    ])

    df = spark.createDataFrame(SAMPLE_EVENTS, schema=schema)

    # ── Parse timestamp ───────────────────────────────────────────────────────
    df = df.withColumn(
        "event_time",
        F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
    )

    # ── V1: Mandatory fields ──────────────────────────────────────────────────
    v1_fail   = F.col("user_id").isNull() | (F.trim(F.col("user_id")) == "") | \
                F.col("amount").isNull()  | \
                F.col("timestamp").isNull() | (F.trim(F.col("timestamp")) == "")
    v1_reason = F.when(F.col("user_id").isNull() | (F.trim(F.col("user_id")) == ""), F.lit("MISSING_USER_ID")) \
                 .when(F.col("amount").isNull(),   F.lit("MISSING_AMOUNT")) \
                 .when(F.col("timestamp").isNull() | (F.trim(F.col("timestamp")) == ""), F.lit("MISSING_TIMESTAMP"))

    # ── V2: Type (amount null setelah cast = error) ───────────────────────────
    v2_fail   = F.col("amount").isNull()
    v2_reason = F.when(v2_fail, F.lit("INVALID_AMOUNT_TYPE"))

    # ── V3: Range ─────────────────────────────────────────────────────────────
    v3_fail   = (~v2_fail) & ((F.col("amount") < AMOUNT_MIN) | (F.col("amount") > AMOUNT_MAX))
    v3_reason = F.when(v3_fail, F.lit(f"AMOUNT_OUT_OF_RANGE"))

    # ── V4: Source ────────────────────────────────────────────────────────────
    v4_fail   = ~F.col("source").isin(list(VALID_SOURCES))
    v4_reason = F.when(v4_fail, F.concat(F.lit("INVALID_SOURCE:"), F.col("source")))

    # ── Timestamp format ──────────────────────────────────────────────────────
    ts_fail   = F.col("event_time").isNull() & F.col("timestamp").isNotNull() & (F.trim(F.col("timestamp")) != "")
    ts_reason = F.when(ts_fail, F.lit("INVALID_TIMESTAMP_FORMAT"))

    any_fail = v1_fail | v2_fail | v3_fail | v4_fail | ts_fail
    error_reason = F.coalesce(v1_reason, v2_reason, ts_reason, v3_reason, v4_reason)

    df = df.withColumn("is_valid",     ~any_fail) \
           .withColumn("error_reason",  F.when(any_fail, error_reason).otherwise(F.lit(None).cast(StringType())))

    # ── Late event detection ──────────────────────────────────────────────────
    cutoff = F.current_timestamp() - F.expr("INTERVAL 3 MINUTES")
    df = df.withColumn("is_late",  (F.col("event_time") < cutoff) & F.col("is_valid")) \
           .withColumn("is_valid", F.when(F.col("is_late"), F.lit(False)).otherwise(F.col("is_valid"))) \
           .withColumn("error_reason", F.when(F.col("is_late"), F.lit("LATE_EVENT_EXCEEDS_WATERMARK"))
                       .otherwise(F.col("error_reason")))

    # ── V5: Duplicate detection ───────────────────────────────────────────────
    from pyspark.sql.window import Window
    dedup_window = Window.partitionBy("user_id", "timestamp").orderBy("event_id")
    df = df.withColumn("_row_num", F.row_number().over(dedup_window)) \
           .withColumn("is_dup",   (F.col("_row_num") > 1) & F.col("is_valid")) \
           .withColumn("is_valid", F.when(F.col("is_dup"), F.lit(False)).otherwise(F.col("is_valid"))) \
           .withColumn("error_reason", F.when(F.col("is_dup"), F.lit("DUPLICATE_EVENT"))
                       .otherwise(F.col("error_reason"))) \
           .drop("_row_num", "is_dup")

    # ── Output ────────────────────────────────────────────────────────────────
    print("\n" + "═" * 90)
    print("  LOCAL PIPELINE VALIDATION TEST")
    print("═" * 90)

    print("\n[1] ALL EVENTS WITH VALIDATION RESULTS")
    df.select("event_id", "user_id", "amount", "source", "timestamp",
              "is_valid", "error_reason", "is_late").show(50, truncate=False)

    valid_df   = df.filter(F.col("is_valid") == True)
    invalid_df = df.filter(F.col("is_valid") == False)

    valid_count   = valid_df.count()
    invalid_count = invalid_df.count()

    print(f"\n[2] ROUTING SUMMARY")
    print(f"  → transactions_valid : {valid_count} events")
    print(f"  → transactions_dlq   : {invalid_count} events")

    print("\n[3] VALID EVENTS → transactions_valid")
    valid_df.select("event_id", "user_id", "amount", "source").show(truncate=False)

    print("\n[4] INVALID EVENTS → transactions_dlq")
    invalid_df.select("event_id", "user_id", "amount", "source", "error_reason").show(truncate=False)

    print("\n[5] TUMBLING WINDOW (1 minute)")
    window_agg = (
        valid_df
        .filter(F.col("event_time").isNotNull())
        .groupBy(F.window(F.col("event_time"), "1 minute"))
        .agg(F.count("*").alias("transactions_in_window"))
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("transactions_in_window")
        )
    )
    window_agg.show(truncate=False)

    print("\n[6] CONSOLE OUTPUT (required columns)")
    spark_output_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"{'timestamp':<30} {'running_total':>15}")
    print("-" * 47)
    print(f"{spark_output_time:<30} {valid_count:>15}")

    print("\n" + "═" * 90)
    print("  TEST SELESAI")
    print("═" * 90 + "\n")

    spark.stop()

if __name__ == "__main__":
    run_validation_test()
