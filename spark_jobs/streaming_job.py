import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, when, lit,
    current_timestamp, udf
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, BooleanType, TimestampType
)

# Spark session
spark = (
    SparkSession.builder
    .appName("PaymentPulse-StreamingJob")
    .config("spark.hadoop.fs.s3a.endpoint",          "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key",        "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key",        "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Schema
TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id",  StringType(),  True),
    StructField("event_time",      StringType(),  True),
    StructField("customer_id",     StringType(),  True),
    StructField("merchant_id",     StringType(),  True),
    StructField("service_type",    StringType(),  True),
    StructField("amount_egp",      DoubleType(),  True),
    StructField("governorate",     StringType(),  True),
    StructField("payment_method",  StringType(),  True),
    StructField("status",          StringType(),  True),
    StructField("is_fraud",        BooleanType(), True),
])

# Rule-based fraud scorer (placeholder until ML model is trained)
# Returns a fraud probability score between 0.0 and 1.0
def score_fraud(amount: float, service_type: str,
                payment_method: str, status: str) -> float:
    score = 0.0

    if amount is None:
        return score

    # High amount is the strongest fraud signal
    if amount > 15000:
        score += 0.6
    elif amount > 8000:
        score += 0.3
    elif amount > 3000:
        score += 0.1

    # Credit card payments at high amounts are riskier
    if payment_method == "credit_card" and amount > 5000:
        score += 0.15

    # Failed transactions that were large are suspicious
    if status == "failed" and amount > 2000:
        score += 0.1

    # University fees and credit card payments have higher legitimate highs
    if service_type in ("university_fees", "credit_card_payment"):
        score = max(0.0, score - 0.1)

    return round(min(score, 1.0), 4)

score_fraud_udf = udf(score_fraud, DoubleType())

# Reading stream from Kafka topic: raw_transactions
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "raw_transactions")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

# Parse JSON payload from Kafka
parsed = (
    raw_stream
    .selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), TRANSACTION_SCHEMA).alias("data"))
    .select("data.*")
)

# Transforming: cleaning, enriching, and scoring
enriched = (
    parsed
    # Parse the event_time string into a proper timestamp
    .withColumn("event_time", to_timestamp(col("event_time")))
    # Fill any null amounts with 0
    .withColumn("amount_egp", when(col("amount_egp").isNull(), lit(0.0))
                .otherwise(col("amount_egp")))
    # Apply fraud scoring UDF
    .withColumn("fraud_score", score_fraud_udf(
        col("amount_egp"),
        col("service_type"),
        col("payment_method"),
        col("status")
    ))
    # Override is_fraud flag based on score threshold or simulator flag
    .withColumn("is_fraud", (col("is_fraud") | (col("fraud_score") >= 0.5)))
    # Adding pipeline ingestion timestamp
    .withColumn("ingested_at", current_timestamp())
)

# Sink 1: Write to MinIO Bronze bucket as Parquet files
# Every 30 seconds a new file is written
bronze_query = (
    enriched.writeStream
    .format("parquet")
    .option("path", "s3a://bronze/transactions/")
    .option("checkpointLocation", "/tmp/checkpoint/bronze")
    .trigger(processingTime="30 seconds")
    .start()
)

# Sink 2: Write to PostgreSQL data warehouse
# foreachBatch is used to write micro-batches using JDBC
PG_URL = "jdbc:postgresql://postgres-dw:5432/pulse_dw"
PG_PROPERTIES = {
    "user":     "pulse",
    "password": "pulse",
    "driver":   "org.postgresql.Driver",
}

def write_to_postgres(batch_df, batch_id):
    if batch_df.count() == 0:
        return
    (
        batch_df
        .select(
            "transaction_id", "event_time", "customer_id",
            "merchant_id", "service_type", "amount_egp",
            "governorate", "payment_method", "status",
            "is_fraud", "fraud_score", "ingested_at"
        )
        .write
        .jdbc(
            url=PG_URL,
            table="transactions",
            mode="append",
            properties=PG_PROPERTIES,
        )
    )
    print(f"Batch {batch_id}: wrote {batch_df.count()} rows to PostgreSQL")

postgres_query = (
    enriched.writeStream
    .foreachBatch(write_to_postgres)
    .option("checkpointLocation", "/tmp/checkpoint/postgres")
    .trigger(processingTime="30 seconds")
    .start()
)

# Keeping the job running until manually stopped
print("Streaming job started. Waiting for data from Kafka")

# Sink 3: Update Redis counters for the live dashboard
import redis as redis_client

def write_to_redis(batch_df, batch_id):
    if batch_df.count() == 0:
        return

    r = redis_client.Redis(host="redis", port=6379, decode_responses=True)
    rows = batch_df.collect()

    for row in rows:
        r.incr("pulse:total_transactions")
        if row["is_fraud"]:
            r.incr("pulse:total_fraud")
        r.incrbyfloat("pulse:total_revenue", float(row["amount_egp"] or 0))

        # Per-service and per-governorate counters
        if row["service_type"]:
            r.hincrby("pulse:by_service", row["service_type"], 1)
        if row["governorate"]:
            r.hincrby("pulse:by_governorate", row["governorate"], 1)

        # TPS window: a list of per-batch counts capped at 60 entries
    r.lpush("pulse:tps_window", len(rows))
    r.ltrim("pulse:tps_window", 0, 59)

redis_query = (
    enriched.writeStream
    .foreachBatch(write_to_redis)
    .option("checkpointLocation", "/tmp/checkpoint/redis")
    .trigger(processingTime="10 seconds")
    .start()
)

spark.streams.awaitAnyTermination()