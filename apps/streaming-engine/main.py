import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, expr, from_unixtime, lit, struct, to_json, to_timestamp, window


SCHEMA_PATH_ENV = os.getenv("SCHEMA_PATH")
if SCHEMA_PATH_ENV:
    SCHEMA_PATH = Path(SCHEMA_PATH_ENV)
else:
    SCHEMA_PATH = Path(__file__).resolve().parents[2] / "schemas" / "transaction.avsc"


def load_schema() -> str:
    return SCHEMA_PATH.read_text()


def main() -> None:
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    source_topic = os.getenv("TOPIC_NAME", "financial-transactions")
    alerts_topic = os.getenv("ALERTS_TOPIC", "fraud-alerts")
    checkpoint_location = os.getenv("CHECKPOINT_LOCATION", "file:///tmp/spark-checkpoints/fraud-alerts")
    amount_threshold = float(os.getenv("RISK_AMOUNT_THRESHOLD", "8000"))
    count_threshold = int(os.getenv("RISK_COUNT_THRESHOLD", "5"))
    device_threshold = int(os.getenv("RISK_DEVICE_THRESHOLD", "3"))
    geo_spread_threshold_km = float(os.getenv("RISK_GEO_SPREAD_KM", "1500"))

    spark = (
        SparkSession.builder.appName("Disvent-Fraud-Detection")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.spark:spark-avro_2.12:3.5.3",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", source_topic)
        .option("startingOffsets", "latest")
        .load()
    )

    # Confluent Avro payloads have a magic byte and four-byte schema id before the Avro body.
    parsed_df = (
        raw_df.withColumn("clean_value", expr("substring(value, 6, length(value) - 5)"))
        .withColumn("parsed", from_avro(col("clean_value"), load_schema()))
        .select("parsed.*")
        .withColumn("event_time", to_timestamp(from_unixtime(col("timestamp_ms") / 1000)))
    )

    windowed_df = (
        parsed_df.withWatermark("event_time", "1 minute")
        .groupBy(window(col("event_time"), "1 minute", "30 seconds"), col("user_id"))
        .agg(
            expr("sum(amount)").alias("total_amount_60s"),
            expr("count(transaction_id)").alias("transaction_count_60s"),
            expr("count(DISTINCT device_fingerprint)").alias("distinct_devices_60s"),
            expr("max(latitude) - min(latitude)").alias("latitude_spread"),
            expr("max(longitude) - min(longitude)").alias("longitude_spread"),
        )
        .withColumn(
            "geo_spread_km",
            expr("sqrt(pow(latitude_spread * 111.0, 2) + pow(longitude_spread * 111.0, 2))"),
        )
    )

    anomalies_df = (
        windowed_df.filter(
            (col("total_amount_60s") >= lit(amount_threshold))
            | (col("transaction_count_60s") >= lit(count_threshold))
            | (col("distinct_devices_60s") >= lit(device_threshold))
            | (col("geo_spread_km") >= lit(geo_spread_threshold_km))
        )
        .withColumn(
            "risk_score",
            expr(
                f"least(100.0, "
                f"(total_amount_60s / {amount_threshold}) * 55.0 + "
                f"(transaction_count_60s / {count_threshold}) * 25.0 + "
                f"(distinct_devices_60s / {device_threshold}) * 10.0 + "
                f"(geo_spread_km / {geo_spread_threshold_km}) * 10.0)"
            ),
        )
        .withColumn(
            "reason",
            expr(
                f"CASE "
                f"WHEN geo_spread_km >= {geo_spread_threshold_km} THEN 'geo_impossibility_window' "
                f"WHEN distinct_devices_60s >= {device_threshold} THEN 'device_fanout' "
                f"WHEN total_amount_60s >= {amount_threshold} AND transaction_count_60s >= {count_threshold} "
                f"THEN 'high_amount_and_velocity' "
                f"WHEN total_amount_60s >= {amount_threshold} THEN 'high_amount_velocity' "
                f"ELSE 'high_transaction_velocity' END"
            ),
        )
    )

    alert_payload = anomalies_df.select(
        col("user_id").cast("string").alias("key"),
        to_json(
            struct(
                col("user_id"),
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("total_amount_60s"),
                col("transaction_count_60s"),
                col("distinct_devices_60s"),
                col("geo_spread_km"),
                col("risk_score"),
                col("reason"),
            )
        ).alias("value"),
    )

    query = (
        alert_payload.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("topic", alerts_topic)
        .option("checkpointLocation", checkpoint_location)
        .outputMode("append")
        .start()
    )

    print(
        "streaming_job_started "
        f"source_topic={source_topic} alerts_topic={alerts_topic} "
        f"amount_threshold={amount_threshold} count_threshold={count_threshold} "
        f"device_threshold={device_threshold} geo_spread_threshold_km={geo_spread_threshold_km}",
        flush=True,
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
