#!/usr/bin/env python3
"""
run_job.py — Wrapper untuk menjalankan streaming_job.py
tanpa spark-submit, langsung via: python streaming\run_job.py

Cara pakai:
  pip install pyspark==3.4.1
  python streaming\run_job.py
"""

import os
import sys

# ── Tambahkan kafka package ke classpath via env var ──────────────
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

# Set package untuk Kafka connector
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1"

# Inject ke SparkConf sebelum import streaming_job
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    f"--packages {KAFKA_PACKAGE} "
    "--conf spark.sql.shuffle.partitions=4 "
    "--conf spark.streaming.stopGracefullyOnShutdown=true "
    "--conf spark.driver.extraJavaOptions=-Duser.timezone=UTC "
    "pyspark-shell"
)

# ── Sekarang import dan jalankan job ─────────────────────────────
print("=" * 60)
print("  Starting Transaction Streaming Job")
print(f"  Python  : {sys.executable}")
print(f"  Package : {KAFKA_PACKAGE}")
print("=" * 60)

# Tambahkan folder streaming ke path agar bisa import
sys.path.insert(0, os.path.dirname(__file__))
import streaming_job

streaming_job.main()