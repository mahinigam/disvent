import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, window, to_timestamp, to_json, struct
from pyspark.sql.avro.functions import from_avro

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("TOPIC_NAME", "financial-transactions")
ALERTS_TOPIC = os.getenv("ALERTS_TOPIC", "fraud-alerts")
SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "../../schemas/transaction.avsc")

def load_schema(schema_path: str) -> str:
    with open(schema_path, "r") as f:
        return f.read()

def main():
    spark = SparkSession.builder \
        .appName("FinTech-Fraud-Detection") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    avro_schema = load_schema(SCHEMA_PATH)

    # 1. Read from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # 2. Parse Confluent Avro (skip 5-byte header)
    df = df.withColumn("clean_value", expr("substring(value, 6, length(value)-5)"))
    
    parsed_df = df.withColumn("parsed", from_avro(col("clean_value"), avro_schema)) \
        .select("parsed.*")

    # Convert timestamp_ms (Int64) to Spark TimestampType
    parsed_df = parsed_df.withColumn("event_time", to_timestamp(col("timestamp_ms") / 1000))

    # 3. Apply Watermarking to handle late data
    watermarked_df = parsed_df.withWatermark("event_time", "1 minute")

    # 4. Define sliding window for fraud detection
    # E.g., User velocity: more than $8000 or >5 transactions in 1 minute
    windowed_df = watermarked_df.groupBy(
        window(col("event_time"), "1 minute", "30 seconds"),
        col("user_id")
    ).agg(
        expr("sum(amount)").alias("total_amount_60s"),
        expr("count(transaction_id)").alias("transaction_count_60s")
    )

    # 5. Filter for anomalies
    anomalies_df = windowed_df.filter(
        (col("total_amount_60s") > 8000) | (col("transaction_count_60s") > 5)
    )
    
    # 6. Format alert for Kafka (JSON)
    alert_payload = anomalies_df.select(
        col("user_id").cast("string").alias("key"),
        to_json(struct(
            col("user_id"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("total_amount_60s"),
            col("transaction_count_60s")
        )).alias("value")
    )

    # 7. Write anomalies to alerts topic
    query = alert_payload.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("topic", ALERTS_TOPIC) \
        .option("checkpointLocation", "/tmp/spark-checkpoints/fraud-alerts") \
        .outputMode("append") \
        .start()

    print("Started PySpark Fraud Detection Streaming Job...")
    query.awaitTermination()

if __name__ == "__main__":
    main()
