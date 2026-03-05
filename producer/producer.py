#!/usr/bin/env python3
"""
Event Producer for Transaction Streaming Pipeline
Menghasilkan event transaksi valid, invalid, dan late events ke Kafka topic: transactions
"""

import json
import time
import random
import uuid
import logging
import argparse
from datetime import datetime, timezone, timedelta
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ─── Logging Setup ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ─── Configuration ───────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME              = "transactions"
VALID_SOURCES           = ["mobile", "web", "pos"]
AMOUNT_MIN              = 1
AMOUNT_MAX              = 10_000_000

# ─── Helper Functions ────────────────────────────────────────────────────────────

def now_iso() -> str:
    """Return current UTC time in ISO-8601 format."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def random_user_id() -> str:
    return f"U{random.randint(10000, 99999)}"

def random_amount(valid: bool = True) -> float:
    if valid:
        return round(random.uniform(AMOUNT_MIN, AMOUNT_MAX), 2)
    return random.choice([
        -500,                     # negatif
        -1,                       # negatif kecil
        10_000_001,               # terlalu besar
        99_999_999,               # jauh melebihi batas
        0,                        # nol (invalid)
    ])

def random_source(valid: bool = True) -> str:
    if valid:
        return random.choice(VALID_SOURCES)
    return random.choice(["atm", "branch", "telegram", "unknown", "fax", ""])

# ─── Event Builders ──────────────────────────────────────────────────────────────

def build_valid_event() -> dict:
    """Membangun satu event transaksi yang sepenuhnya valid."""
    return {
        "event_id":  str(uuid.uuid4()),
        "user_id":   random_user_id(),
        "amount":    random_amount(valid=True),
        "timestamp": now_iso(),
        "source":    random_source(valid=True),
    }

def build_invalid_events() -> list[dict]:
    """
    Minimal 3 event INVALID:
      1. amount negatif
      2. amount terlalu besar
      3. timestamp tidak valid (format salah)
      4. source tidak dikenal
      5. duplicate (sama persis dengan event sebelumnya, disimpan di modul level)
    """
    base_ts = now_iso()
    events = [
        # 1. Amount negatif
        {
            "event_id":  str(uuid.uuid4()),
            "user_id":   random_user_id(),
            "amount":    -9999,
            "timestamp": base_ts,
            "source":    "mobile",
        },
        # 2. Amount terlalu besar (> 10.000.000)
        {
            "event_id":  str(uuid.uuid4()),
            "user_id":   random_user_id(),
            "amount":    50_000_000,
            "timestamp": base_ts,
            "source":    "web",
        },
        # 3. Timestamp tidak valid
        {
            "event_id":  str(uuid.uuid4()),
            "user_id":   random_user_id(),
            "amount":    75000,
            "timestamp": "not-a-real-timestamp",
            "source":    "pos",
        },
        # 4. Source tidak dikenal
        {
            "event_id":  str(uuid.uuid4()),
            "user_id":   random_user_id(),
            "amount":    120000,
            "timestamp": base_ts,
            "source":    "telegram",
        },
        # 5. Missing mandatory field (user_id kosong)
        {
            "event_id":  str(uuid.uuid4()),
            "user_id":   "",
            "amount":    50000,
            "timestamp": base_ts,
            "source":    "mobile",
        },
    ]
    return events

# Simpan satu event untuk keperluan duplicate injection
_duplicate_seed: dict | None = None

def build_duplicate_event() -> dict:
    """Mengembalikan event duplicate (event_id dan konten sama)."""
    global _duplicate_seed
    if _duplicate_seed is None:
        _duplicate_seed = {
            "event_id":  "DUPLICATE-FIXED-ID-001",
            "user_id":   "U55555",
            "amount":    200000,
            "timestamp": "2025-01-01T10:00:00Z",
            "source":    "web",
        }
    return dict(_duplicate_seed)   # kembalikan salinan

def build_late_events(count: int = 3) -> list[dict]:
    """
    Minimal 3 event TERLAMBAT:
    Timestamp lebih dari 3 menit di masa lalu → melewati watermark.
    """
    late_events = []
    for i in range(count):
        delay_minutes = random.randint(4, 15)          # > 3 menit watermark
        late_ts = (
            datetime.now(timezone.utc) - timedelta(minutes=delay_minutes)
        ).strftime("%Y-%m-%dT%H:%M:%SZ")
        late_events.append({
            "event_id":  str(uuid.uuid4()),
            "user_id":   random_user_id(),
            "amount":    random_amount(valid=True),
            "timestamp": late_ts,
            "source":    random_source(valid=True),
            "_late_by_minutes": delay_minutes,         # meta, bisa dihapus jika diinginkan
        })
    return late_events

# ─── Kafka Producer ──────────────────────────────────────────────────────────────

def create_producer(bootstrap_servers: str) -> KafkaProducer:
    """Membuat dan mengembalikan KafkaProducer yang dikonfigurasi."""
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
        max_block_ms=10_000,
    )

def send_event(
    producer: KafkaProducer,
    topic: str,
    event: dict,
    label: str = "VALID",
) -> None:
    """Kirim satu event ke Kafka dan log hasilnya."""
    key = event.get("user_id") or str(uuid.uuid4())
    try:
        future = producer.send(topic, key=key, value=event)
        metadata = future.get(timeout=10)
        logger.info(
            "[%s] → topic=%s partition=%d offset=%d | user_id=%s amount=%s source=%s ts=%s",
            label,
            metadata.topic,
            metadata.partition,
            metadata.offset,
            event.get("user_id", "N/A"),
            event.get("amount", "N/A"),
            event.get("source", "N/A"),
            event.get("timestamp", "N/A"),
        )
    except KafkaError as exc:
        logger.error("[%s] Gagal kirim event: %s | event=%s", label, exc, event)

# ─── Main Loop ───────────────────────────────────────────────────────────────────

def run_producer(
    bootstrap_servers: str,
    topic: str,
    total_events: int,
    inject_invalid_every: int = 8,
    inject_late_every: int   = 10,
    inject_dup_every: int    = 12,
) -> None:
    logger.info("=== Transaction Event Producer Starting ===")
    logger.info("Bootstrap  : %s", bootstrap_servers)
    logger.info("Topic      : %s", topic)
    logger.info("Total plan : %d valid events + injected invalids/lates/dups", total_events)

    producer = create_producer(bootstrap_servers)

    # --- Inject invalid events di awal (minimal 3) ---
    logger.info("--- Injecting INVALID events (batch awal) ---")
    for evt in build_invalid_events():
        send_event(producer, topic, evt, label="INVALID")
        time.sleep(0.3)

    # --- Inject duplicate events di awal ---
    logger.info("--- Injecting DUPLICATE events (batch awal) ---")
    for _ in range(2):
        send_event(producer, topic, build_duplicate_event(), label="DUPLICATE")
        time.sleep(0.3)

    # --- Inject late events di awal (minimal 3) ---
    logger.info("--- Injecting LATE events (batch awal) ---")
    for evt in build_late_events(count=3):
        send_event(producer, topic, evt, label="LATE")
        time.sleep(0.3)

    # --- Loop event valid + periodic injection ---
    logger.info("--- Starting main valid event loop ---")
    sent_count = 0
    while sent_count < total_events:
        # Periodic invalid injection
        if sent_count > 0 and sent_count % inject_invalid_every == 0:
            invalid_batch = [
                {
                    "event_id":  str(uuid.uuid4()),
                    "user_id":   random_user_id(),
                    "amount":    random_amount(valid=False),
                    "timestamp": now_iso(),
                    "source":    random_source(valid=True),
                },
                {
                    "event_id":  str(uuid.uuid4()),
                    "user_id":   random_user_id(),
                    "amount":    random_amount(valid=True),
                    "timestamp": now_iso(),
                    "source":    random_source(valid=False),
                },
            ]
            for evt in invalid_batch:
                send_event(producer, topic, evt, label="INVALID-PERIODIC")

        # Periodic late injection
        if sent_count > 0 and sent_count % inject_late_every == 0:
            for evt in build_late_events(count=1):
                send_event(producer, topic, evt, label="LATE-PERIODIC")

        # Periodic duplicate injection
        if sent_count > 0 and sent_count % inject_dup_every == 0:
            send_event(producer, topic, build_duplicate_event(), label="DUPLICATE-PERIODIC")

        # Normal valid event
        event = build_valid_event()
        send_event(producer, topic, event, label="VALID")
        sent_count += 1

        # Jeda 1–2 detik sesuai spesifikasi
        time.sleep(random.uniform(1.0, 2.0))

    producer.flush()
    logger.info("=== Producer selesai. Total valid events: %d ===", sent_count)

# ─── Entry Point ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transaction Event Producer")
    parser.add_argument("--bootstrap-servers", default=KAFKA_BOOTSTRAP_SERVERS)
    parser.add_argument("--topic",             default=TOPIC_NAME)
    parser.add_argument("--total-events",      type=int, default=100)
    args = parser.parse_args()

    run_producer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        total_events=args.total_events,
    )
