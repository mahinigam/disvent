CREATE TABLE IF NOT EXISTS transactions (
    transaction_id String,
    user_id String,
    amount Float64,
    currency LowCardinality(String) DEFAULT 'USD',
    merchant_id LowCardinality(String),
    location String,
    latitude Float64,
    longitude Float64,
    device_fingerprint String,
    timestamp_ms Int64,
    event_time DateTime MATERIALIZED toDateTime(timestamp_ms / 1000)
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (user_id, event_time, transaction_id);

CREATE TABLE IF NOT EXISTS merchant_hourly_stats (
    merchant_id LowCardinality(String),
    hour DateTime,
    total_amount Float64,
    transaction_count UInt64
) ENGINE = SummingMergeTree()
ORDER BY (merchant_id, hour);

CREATE MATERIALIZED VIEW IF NOT EXISTS merchant_hourly_mv
TO merchant_hourly_stats AS
SELECT
    merchant_id,
    toStartOfHour(event_time) AS hour,
    sum(amount) AS total_amount,
    count() AS transaction_count
FROM transactions
GROUP BY merchant_id, hour;

CREATE TABLE IF NOT EXISTS risk_scores (
    user_id String,
    window_start DateTime,
    window_end DateTime,
    total_amount_60s Float64,
    transaction_count_60s UInt64,
    distinct_devices_60s UInt64,
    geo_spread_km Float64,
    risk_score Float64,
    reason LowCardinality(String),
    calculated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (user_id, window_start, calculated_at);

CREATE TABLE IF NOT EXISTS transactions_kafka (
    transaction_id String,
    user_id String,
    amount Float64,
    currency String,
    merchant_id String,
    location String,
    latitude Float64,
    longitude Float64,
    device_fingerprint String,
    timestamp_ms Int64
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'redpanda:29092',
         kafka_topic_list = 'financial-transactions',
         kafka_group_name = 'clickhouse_transactions_consumer',
         kafka_format = 'AvroConfluent',
         format_avro_schema_registry_url = 'http://redpanda:18081',
         kafka_num_consumers = 2;

CREATE MATERIALIZED VIEW IF NOT EXISTS transactions_consumer_mv
TO transactions AS
SELECT * FROM transactions_kafka;

CREATE TABLE IF NOT EXISTS risk_scores_kafka (
    user_id String,
    window_start DateTime,
    window_end DateTime,
    total_amount_60s Float64,
    transaction_count_60s UInt64,
    distinct_devices_60s UInt64,
    geo_spread_km Float64,
    risk_score Float64,
    reason String
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'redpanda:29092',
         kafka_topic_list = 'fraud-alerts',
         kafka_group_name = 'clickhouse_risk_scores_consumer',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS risk_scores_consumer_mv
TO risk_scores AS
SELECT
    user_id,
    window_start,
    window_end,
    total_amount_60s,
    transaction_count_60s,
    distinct_devices_60s,
    geo_spread_km,
    risk_score,
    reason,
    now() AS calculated_at
FROM risk_scores_kafka;
