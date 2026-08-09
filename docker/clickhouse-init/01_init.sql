CREATE DATABASE IF NOT EXISTS disvent;
USE disvent;

-- 1. Main transactions table (ReplacingMergeTree for idempotency)
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id String,
    user_id String,
    amount Float64,
    currency String DEFAULT 'USD',
    merchant_id String,
    location String,
    device_fingerprint String,
    timestamp_ms Int64,
    event_time DateTime MATERIALIZED toDateTime(timestamp_ms / 1000)
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (user_id, event_time, transaction_id);

-- 2. Aggregation Table for Merchant Statistics (SummingMergeTree)
CREATE TABLE IF NOT EXISTS merchant_hourly_stats (
    merchant_id String,
    hour DateTime,
    total_amount Float64,
    transaction_count UInt64
) ENGINE = SummingMergeTree()
ORDER BY (merchant_id, hour);

-- 3. Materialized View to feed merchant_hourly_stats
CREATE MATERIALIZED VIEW IF NOT EXISTS merchant_hourly_mv 
TO merchant_hourly_stats AS
SELECT 
    merchant_id,
    toStartOfHour(event_time) AS hour,
    sum(amount) AS total_amount,
    count() AS transaction_count
FROM transactions
GROUP BY merchant_id, hour;

-- 4. Risk Scores Table (populated by PySpark)
CREATE TABLE IF NOT EXISTS risk_scores (
    user_id String,
    window_start DateTime,
    window_end DateTime,
    total_amount_60s Float64,
    transaction_count_60s UInt64,
    is_anomaly UInt8,
    calculated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (user_id, window_start);

-- 5. Kafka Consumer Engine Table (Direct Ingestion)
CREATE TABLE IF NOT EXISTS transactions_kafka (
    transaction_id String,
    user_id String,
    amount Float64,
    currency String,
    merchant_id String,
    location String,
    device_fingerprint String,
    timestamp_ms Int64
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'redpanda:29092',
         kafka_topic_list = 'financial-transactions',
         kafka_group_name = 'clickhouse_consumer_group',
         kafka_format = 'AvroConfluent',
         format_avro_schema_registry_url = 'http://redpanda:8081';

-- 6. Materialized View to stream from Kafka into the main transactions table
CREATE MATERIALIZED VIEW IF NOT EXISTS transactions_consumer_mv 
TO transactions AS 
SELECT * FROM transactions_kafka;
