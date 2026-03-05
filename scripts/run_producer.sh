#!/bin/bash
# ─────────────────────────────────────────────────────────────
# run_producer.sh
# Menjalankan event producer (di host, bukan di dalam container)
# Requirements: pip install kafka-python faker
# ─────────────────────────────────────────────────────────────

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

TOTAL_EVENTS="${1:-100}"
BOOTSTRAP="${2:-localhost:9092}"

echo "=== Transaction Event Producer ==="
echo "Bootstrap   : $BOOTSTRAP"
echo "Total Events: $TOTAL_EVENTS"
echo ""

# Install dependencies jika belum ada
if ! python3 -c "import kafka" > /dev/null 2>&1; then
  echo "  Installing kafka-python..."
  pip3 install kafka-python faker --quiet
fi

# Jalankan producer
python3 "$ROOT_DIR/producer/producer.py" \
  --bootstrap-servers "$BOOTSTRAP" \
  --topic transactions \
  --total-events "$TOTAL_EVENTS"
