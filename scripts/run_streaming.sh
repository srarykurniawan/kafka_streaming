#!/bin/bash
# ─────────────────────────────────────────────────────────────
# run_streaming.sh
# Menjalankan PySpark Structured Streaming job di dalam container
# ─────────────────────────────────────────────────────────────

set -euo pipefail

SPARK_CONTAINER="spark-master"
KAFKA_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-kafka:29092}"

echo "=== Menjalankan PySpark Streaming Job ==="
echo "Spark container  : $SPARK_CONTAINER"
echo "Kafka bootstrap  : $KAFKA_SERVERS"
echo ""

# Copy streaming job ke container (jika perlu)
docker cp "$(dirname "$0")/../streaming/streaming_job.py" \
  "$SPARK_CONTAINER":/app/streaming/streaming_job.py

# Submit spark job di dalam container
docker exec -it "$SPARK_CONTAINER" \
  spark-submit \
    --master local[*] \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
    --conf spark.sql.shuffle.partitions=4 \
    --conf spark.streaming.stopGracefullyOnShutdown=true \
    --conf "spark.driver.extraJavaOptions=-Duser.timezone=UTC" \
    --conf "spark.executor.extraJavaOptions=-Duser.timezone=UTC" \
    /app/streaming/streaming_job.py
