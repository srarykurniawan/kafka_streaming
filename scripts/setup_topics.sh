#!/bin/bash
# ─────────────────────────────────────────────────────────────
# setup_topics.sh
# Membuat Kafka topics yang dibutuhkan pipeline
# Jalankan SETELAH docker-compose up
# ─────────────────────────────────────────────────────────────

set -euo pipefail

KAFKA_CONTAINER="kafka"
BOOTSTRAP="kafka:29092"

echo "=== Menunggu Kafka siap... ==="
until docker exec "$KAFKA_CONTAINER" kafka-topics \
  --bootstrap-server "$BOOTSTRAP" --list > /dev/null 2>&1; do
  echo "  Kafka belum siap, tunggu 5 detik..."
  sleep 5
done
echo "  Kafka siap!"

echo ""
echo "=== Membuat Topics ==="

TOPICS=("transactions" "transactions_valid" "transactions_dlq")

for topic in "${TOPICS[@]}"; do
  # Cek apakah sudah ada
  if docker exec "$KAFKA_CONTAINER" kafka-topics \
    --bootstrap-server "$BOOTSTRAP" \
    --describe --topic "$topic" > /dev/null 2>&1; then
    echo "  [SKIP] Topic '$topic' sudah ada."
  else
    docker exec "$KAFKA_CONTAINER" kafka-topics \
      --bootstrap-server "$BOOTSTRAP" \
      --create \
      --topic "$topic" \
      --partitions 3 \
      --replication-factor 1
    echo "  [OK]   Topic '$topic' dibuat."
  fi
done

echo ""
echo "=== Daftar Topics ==="
docker exec "$KAFKA_CONTAINER" kafka-topics \
  --bootstrap-server "$BOOTSTRAP" --list

echo ""
echo "=== Setup selesai! ==="
