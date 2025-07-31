![Python 3.11](https://img.shields.io/badge/python-3.11-blue)
![clickhouse-connect](https://img.shields.io/badge/clickhouse--connect-0.8.15-blue)
![confluent-kafka](https://img.shields.io/badge/confluent--kafka-2.10.0-blue)
![lz4](https://img.shields.io/badge/lz4-4.3.2-blue)
![orjson](https://img.shields.io/badge/orjson-3.10.15-blue)
![pandas](https://img.shields.io/badge/pandas-2.2.3-blue)
![polars](https://img.shields.io/badge/polars-1.25.2-blue)
![psutil](https://img.shields.io/badge/psutil-7.0.0-blue)
![pyarrow](https://img.shields.io/badge/pyarrow-19.0.1-blue)
![python-snappy](https://img.shields.io/badge/python--snappy-0.7.3-blue)
![rich](https://img.shields.io/badge/rich-13.9.4-blue)
![python-dotenv](https://img.shields.io/badge/python--dotenv-1.0.0-blue)
![fastjsonschema](https://img.shields.io/badge/fastjsonschema-2.16.2-blue)

# kafka2clickhouse-py-streamer
## Kafka-to-ClickHouse Ingestion Service


## Description

This service consumes JSON data from a Kafka topic, validates and transforms the data according to the schema of a target ClickHouse table, and inserts valid records into ClickHouse. Invalid or failed records are sent to a Dead Letter Queue (DLQ) Kafka topic for further inspection.

---

## Features

- **High-performance ingestion**: Uses `polars` and `pyarrow` for fast in-memory processing of large batches.
- **Schema-aware transformation**: Dynamically loads schema metadata from ClickHouse and maps incoming records accordingly.
- **Strict validation**: Enforces data structure and type conformity using `jsonschema-rs` and column-level casting.
- **Graceful degradation**: If batch insert to ClickHouse fails, the system isolates and retries record-wise fallback insert for maximum data retention.
- **DLQ handling**: Sends malformed or unprocessable records to a DLQ Kafka topic for later inspection and triage.
- **ClickHouse async insert**: Leverages async insert mode for throughput optimization.
- **Schema drift handling**: Auto-fetches latest schema from ClickHouse if data mismatch is detected.
- **Structured logging**: Logs validation failures, insert errors, and offsets committed.
- **Configurable**: All behavior is environment-driven (supports `.env` or injected secrets).

---

## Environment Variables

The following environment variables must be set (either in your environment or a `.env` file):

- `KAFKA_BROKER`: Kafka broker address
- `KAFKA_CONSUMER_GROUP_ID`: Kafka consumer group ID
- `KAFKA_PRODUCER_CLIENT_ID`: Kafka producer client ID (for DLQ)
- `KAFKA_SOURCE_TOPIC`: Kafka topic to consume from
- `KAFKA_DLQ_TOPIC`: Kafka topic for DLQ
- `KAFKA_AUTO_OFFSET_RESET`: Kafka offset reset policy (e.g., `earliest`)
- `NUM_MESSAGES`: Number of messages to consume per poll (default: 25000)
- `CLICKHOUSE_HOST`: ClickHouse server host
- `CLICKHOUSE_DATABASE`: ClickHouse database name
- `CLICKHOUSE_USER`: ClickHouse username
- `CLICKHOUSE_PASSWORD`: ClickHouse password
- `CLICKHOUSE_TABLE`: ClickHouse table name
- `REQUIRED_COLUMNS`: Comma-separated list of required columns 
- `DATETIME_COLUMNS`: Comma-separated list of datetime columns 
- `STRING_ENUM_COLUMNS`: Comma-separated list of string-enum columns 

---

## Installation

1. Clone this repository.
2. Install dependencies:
    ```sh
    pip install -r requirements.txt
    ```
3. Set up your `.env` file or export the required environment variables.
---

## Usage

Run the service with:

```sh
python main.py 
#-------------- 
python3 main.py
```

The service will connect to Kafka, process messages in batches, validate and insert them into ClickHouse, and handle errors as described above.

---

## Logging

Logs are output to the console with timestamps and log levels.

---

## Error Handling

- Records that fail schema validation or insertion are sent to the DLQ Kafka topic.
- All errors are logged for troubleshooting.

---

## Requirements

- Python 3.11+
- Kafka cluster
- ClickHouse server

---

### 🛠️ Tech Stack

- **Kafka** (Producer/Consumer)
- **ClickHouse** (via `clickhouse-connect`)
- **Polars** (for fast DataFrame manipulation)
- **PyArrow** (for Arrow-based ClickHouse inserts)
- **orjson** (for efficient JSON serialization/deserialization)
- **jsonschema-rs** (ultra-fast schema validation)
- **Python 3.11+**

---

### 🧪 Validation & Transformation Flow

1. **Consume messages** from Kafka in batches.
2. **Validate JSON structure** using `jsonschema-rs`.
3. **Transform** records into `polars` DataFrame based on target ClickHouse schema.
4. **Split** valid vs invalid rows.
5. **Insert valid records** into ClickHouse using `insert_arrow()`.
6. **Send invalid records** to Kafka DLQ with rejection reason.

---

### 📤 Dead Letter Queue (DLQ)

Invalid records are enriched with a `failure_reason` and original metadata, then pushed to a dedicated DLQ Kafka topic. This enables observability and root-cause debugging without blocking the main pipeline.

---

### 📈 Performance Considerations

- Processes batches of ~25,000 records for optimal throughput.
- Uses `async_insert_threads` and `async_insert_max_data_size` to scale inserts.
- Bypasses pandas and uses Arrow-native ingestion for minimal serialization overhead.

---

### 🔐 Production-Ready Notes

- Includes graceful shutdown on interrupt.
- Commits Kafka offsets only after successful processing.
- Fails loudly and transparently on schema mismatch or validation error.
- Can be containerized and deployed in K8s (e.g., AKS, EKS) with health checks.

---

## License

Apache License 2.0

---

**Author:** [JARUPULA PREM KUMAR]  
**Created:** [2025-07-17]
