# kafka2clickhouse-py-streamer
This service consumes JSON data from a Kafka topic, validates and transforms the data according to the schema of a target ClickHouse table, and inserts valid records into ClickHouse. Invalid or failed records are sent to a Dead Letter Queue (DLQ) Kafka topic for further inspection.
