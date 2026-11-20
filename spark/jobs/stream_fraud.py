"""Real-time fraud scoring: Kafka airport.transactions -> rules -> airport.fraud-alerts (+ Delta).

Features come from the latest daily snapshot (static, broadcast). The blocklist is read at start-up.
Run detached: make stream-start    Stop: make stream-stop
"""
import fraud_rules as fr
from pyspark.sql import functions as F
from pyspark.sql.types import (BooleanType, DoubleType, IntegerType, LongType, StringType, StructField,
                               StructType, TimestampType)

from common import (ALERTS_TOPIC, GOLD_FEATURES, GOLD_STREAM_SCORES, KAFKA_INTERNAL, QUARANTINE_STREAM,
                    SILVER_BLOCKLIST, TRANSACTION_JSON_SCHEMA, TRANSACTIONS_TOPIC, read_delta, session, ship_rules,
                    table_exists)

spark = session("stream_fraud")
spark.sparkContext.setLogLevel("WARN")
ship_rules(spark)

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", KAFKA_INTERNAL)
       .option("subscribe", TRANSACTIONS_TOPIC)
       .option("startingOffsets", "latest")
       .option("maxOffsetsPerTrigger", "2000")
       .load())
# try_to_timestamp: under ANSI mode (Spark 4 default) to_timestamp fails the whole micro-batch on one bad value.
# Bad rows are routed to quarantine with a reason, like the batch silver job.
parsed = (raw.select(F.from_json(F.col("value").cast("string"), TRANSACTION_JSON_SCHEMA).alias("p"))
          .select("p.*")
          .withColumn("event_timestamp", F.try_to_timestamp("event_timestamp"))
          .withColumn("reject_reason",
                      F.when(F.col("transaction_id").isNull() | F.col("passenger_id").isNull(), F.lit("NULL_KEY"))
                      .when(F.col("event_timestamp").isNull(), F.lit("BAD_TIMESTAMP"))))
payments = parsed.filter(F.col("reject_reason").isNull()).drop("reject_reason")
rejected = parsed.filter(F.col("reject_reason").isNotNull()).withColumn("quarantined_at", F.current_timestamp())

# Static inputs: latest feature snapshot and blocklist
latest_as_of = read_delta(spark, GOLD_FEATURES).agg(F.max("as_of")).collect()[0][0]
features = (read_delta(spark, GOLD_FEATURES).filter(F.col("as_of") == F.lit(latest_as_of))
            .select("passenger_id", "avg_amount_30d", "distinct_airports_30d", "account_age_days"))
if table_exists(spark, SILVER_BLOCKLIST):
    blocked = read_delta(spark, SILVER_BLOCKLIST).select("passenger_id").withColumn("blocked", F.lit(True))
else:
    blocked = spark.createDataFrame([], "passenger_id long, blocked boolean")
print(f"features as of {latest_as_of}")

joined = (payments.join(F.broadcast(features), "passenger_id", "left")
          .join(F.broadcast(blocked), "passenger_id", "left")
          .withColumn("blocked", F.coalesce(F.col("blocked"), F.lit(False))))

input_cols = ["transaction_id", "passenger_id", "event_timestamp", "airport", "merchant_category", "amount",
              "hour_of_day", "is_international", "card_present", "minutes_to_departure",
              "avg_amount_30d", "distinct_airports_30d", "account_age_days", "blocked"]
out_cols = fr.output_columns()
out_schema = StructType([
    StructField("transaction_id", StringType()), StructField("passenger_id", LongType()),
    StructField("event_timestamp", TimestampType()), StructField("airport", StringType()),
    StructField("merchant_category", StringType()), StructField("amount", DoubleType()),
    StructField("amount_ratio", DoubleType()), StructField("score", IntegerType()),
    StructField("rules", StringType()), StructField("is_alert", BooleanType()),
])


def score_partition(batches):
    for pdf in batches:
        yield fr.score_frame(pdf)[out_cols]


scored = joined.select(*input_cols).mapInPandas(score_partition, out_schema)

alerts = (scored.filter(F.col("is_alert"))
          .select(F.col("passenger_id").cast("string").alias("key"),
                  F.to_json(F.struct(
                      "transaction_id", "passenger_id", "airport", "merchant_category", "amount",
                      "score", "rules", "event_timestamp")).alias("value")))

alert_query = (alerts.writeStream.format("kafka")
               .option("kafka.bootstrap.servers", KAFKA_INTERNAL)
               .option("topic", ALERTS_TOPIC)
               .option("checkpointLocation", "s3a://checkpoints/fraud_alerts_kafka/")
               .start())

store_query = (scored.withColumn("event_date", F.to_date("event_timestamp"))
               .withColumn("scored_at", F.current_timestamp())
               .writeStream.format("delta").outputMode("append")
               .option("path", GOLD_STREAM_SCORES)
               .option("mergeSchema", "true")  # allow scored_at on tables created by earlier lab versions
               .option("checkpointLocation", "s3a://checkpoints/fraud_stream_scores/")
               .start())

quarantine_query = (rejected.writeStream.format("delta").outputMode("append")
                    .option("path", QUARANTINE_STREAM)
                    .option("checkpointLocation", "s3a://checkpoints/fraud_stream_quarantine/")
                    .start())

print("streaming: scoring airport.transactions; alerts -> airport.fraud-alerts")
spark.streams.awaitAnyTermination()
