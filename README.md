# Kafka Streaming

## Transaction Event Pipeline dengan PySpark Structured Streaming

---

## 📁 Struktur Folder

```
kafka-streaming-assignment/
├── docker-compose.yml              # Kafka + Zookeeper + Spark + Kafka UI
├── producer/
│   ├── producer.py                 # Event producer (valid, invalid, late, duplicate)
│   └── requirements.txt            # Dependensi Python producer
├── streaming/
│   └── streaming_job.py            # PySpark Structured Streaming job
├── scripts/
│   ├── setup_topics.sh             # Buat Kafka topics
│   ├── run_producer.sh             # Jalankan producer
│   ├── run_streaming.sh            # Jalankan PySpark job di container
│   └── local_pipeline_test.py      # Test lokal tanpa Docker/Kafka
├── config/
│   └── (reserved untuk konfigurasi tambahan)
└── output_logs/
    └── checkpoints/                # Spark streaming checkpoints (auto-generated)
```

---

## 🚀 Cara Menjalankan (Full Stack dengan Docker)

### Prasyarat
- Docker & Docker Compose terinstall
- Python 3.8+

### Langkah 1: Jalankan infrastruktur

```bash
cd kafka-streaming-assignment
docker-compose up -d
```

Tunggu sekitar 30–60 detik hingga Kafka siap.

### Langkah 2: Buat Kafka Topics

```bash
chmod +x scripts/setup_topics.sh
./scripts/setup_topics.sh
```

Output yang diharapkan:
```
=== Kafka siap! ===
[OK] Topic 'transactions' dibuat.
[OK] Topic 'transactions_valid' dibuat.
[OK] Topic 'transactions_dlq' dibuat.
```

### Langkah 3: Jalankan PySpark Streaming Job

Di terminal baru:
```bash
chmod +x scripts/run_streaming.sh
./scripts/run_streaming.sh
```

### Langkah 4: Jalankan Producer

Di terminal lain:
```bash
chmod +x scripts/run_producer.sh
./scripts/run_producer.sh 100          # 100 valid events
# atau dengan custom bootstrap:
./scripts/run_producer.sh 50 localhost:9092
```

---

## 🧪 Test Lokal (Tanpa Docker/Kafka)

Untuk memvalidasi logika validasi dan output format:

```bash
pip install pyspark
python3 scripts/local_pipeline_test.py
```

---

## 📋 Spesifikasi Pipeline

### Event Producer (`producer/producer.py`)

| Kategori         | Jumlah Minimal | Keterangan |
|-----------------|----------------|------------|
| Valid events    | Terus-menerus  | Setiap 1–2 detik |
| Invalid events  | ≥ 5            | Amount negatif, terlalu besar, timestamp invalid, source unknown, user_id kosong |
| Duplicate events | ≥ 2           | event_id + konten sama |
| Late events     | ≥ 3            | Timestamp > 3 menit di masa lalu |

### 5 Validasi Wajib (PySpark)

| # | Validasi | Keterangan | Error Reason |
|---|----------|------------|--------------|
| V1 | Mandatory Field | user_id, amount, timestamp tidak boleh null/kosong | `MISSING_USER_ID` / `MISSING_AMOUNT` / `MISSING_TIMESTAMP` |
| V2 | Type Validation | amount harus numerik (Double) | `INVALID_AMOUNT_TYPE` |
| V3 | Range Validation | amount antara 1 dan 10.000.000 | `AMOUNT_OUT_OF_RANGE` |
| V4 | Source Validation | harus "mobile", "web", atau "pos" | `INVALID_SOURCE:<value>` |
| V5 | Duplicate Detection | user_id + timestamp unik per batch | `DUPLICATE_EVENT` |
| + | Late Event | event_time < (now - 3 menit) | `LATE_EVENT_EXCEEDS_WATERMARK` |

### Routing Output

| Kondisi | Kafka Topic |
|---------|-------------|
| `is_valid = true` | `transactions_valid` |
| `is_valid = false` | `transactions_dlq` |

### Watermark & Window

```python
.withWatermark("event_time", "3 minutes")   # late event threshold
F.window(F.col("event_time"), "1 minute")   # tumbling window aggregation
```

### Console Output (required columns)

```
timestamp                      running_total
──────────────────────────────────────────────
2025-12-14T09:00:20Z                      47
```

---

## 📊 Kafka UI

Buka browser: **http://localhost:8080**

Monitor topics: `transactions`, `transactions_valid`, `transactions_dlq`

---

## 🔧 Konfigurasi

| Parameter | Default | Keterangan |
|-----------|---------|------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` (producer) / `kafka:29092` (spark) | Bootstrap server |
| `WATERMARK_DELAY` | `3 minutes` | Batas toleransi late event |
| `WINDOW_DURATION` | `1 minute` | Ukuran tumbling window |
| `AMOUNT_MIN` | `1` | Batas bawah amount |
| `AMOUNT_MAX` | `10,000,000` | Batas atas amount |
| Interval producer | `1–2 detik` | Random antara 1 dan 2 detik |

---

## 📝 Format Event

### Input (JSON ke Kafka topic `transactions`)
```json
{
  "event_id": "uuid-v4",
  "user_id": "U12345",
  "amount": 150000,
  "timestamp": "2025-12-14T09:00:20Z",
  "source": "mobile"
}
```

### Output Valid (topic `transactions_valid`)
```json
{
  "event_id": "...",
  "user_id": "U12345",
  "amount": 150000,
  "timestamp": "2025-12-14T09:00:20Z",
  "source": "mobile",
  "is_valid": true
}
```

### Output DLQ (topic `transactions_dlq`)
```json
{
  "event_id": "...",
  "user_id": "U99999",
  "amount": -500,
  "timestamp": "2025-12-14T09:00:20Z",
  "source": "mobile",
  "is_valid": false,
  "error_reason": "AMOUNT_OUT_OF_RANGE(1.0-1.0E7)"
}
```
