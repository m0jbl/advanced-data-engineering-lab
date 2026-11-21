"""Batch scoring of one day with the same rules as the stream. Run: docker compose exec -T spark-master /opt/spark/bin/spark-submit /opt/spark-apps/score_batch.py --date 2026-11-18

Uses features as of that day, and the blocklist (silver) if it exists.
"""
import argparse

from pyspark.sql import functions as F
import fraud_rules as fr
from common import (GOLD_FEATURES, GOLD_SCORES, SILVER_BLOCKLIST, SILVER_TRANSACTIONS, push_metrics, read_delta,
                    session, ship_rules, table_exists, write_delta_partitions)

p = argparse.ArgumentParser()
p.add_argument("--date", default=None, help="event_date to score (default: latest in silver)")
args = p.parse_args()

spark = session("score_batch")
ship_rules(spark)

silver = read_delta(spark, SILVER_TRANSACTIONS)
day = args.date or silver.agg(F.max("event_date")).collect()[0][0].isoformat()
txns = silver.filter(F.col("event_date") == F.lit(day))
if not txns.take(1):
    raise SystemExit(f"No silver payments for {day}. Run without DATE to score the latest available day.")

features = read_delta(spark, GOLD_FEATURES).filter(F.col("as_of") == F.lit(day)) \
                .select("passenger_id", "avg_amount_30d", "distinct_airports_30d", "account_age_days")
if table_exists(spark, SILVER_BLOCKLIST):
    blocked = read_delta(spark, SILVER_BLOCKLIST).select("passenger_id").withColumn("blocked", F.lit(True))
else:
    blocked = spark.createDataFrame([], "passenger_id long, blocked boolean")

joined = (txns.join(F.broadcast(features), "passenger_id", "left")
              .join(blocked, "passenger_id", "left")
              .withColumn("blocked", F.coalesce(F.col("blocked"), F.lit(False))))

input_cols = ["transaction_id", "passenger_id", "event_timestamp", "airport", "merchant_category", "amount",
              "hour_of_day", "is_international", "card_present", "minutes_to_departure",
              "avg_amount_30d", "distinct_airports_30d", "account_age_days", "blocked"]
out_cols = fr.output_columns()


def score_partition(batches):
    for pdf in batches:
        yield fr.score_frame(pdf)[out_cols]


from pyspark.sql.types import (BooleanType, DoubleType, IntegerType, StringType, StructField, StructType,
                               TimestampType, LongType)
schema = StructType([
    StructField("transaction_id", StringType()), StructField("passenger_id", LongType()),
    StructField("event_timestamp", TimestampType()), StructField("airport", StringType()),
    StructField("merchant_category", StringType()), StructField("amount", DoubleType()),
    StructField("amount_ratio", DoubleType()), StructField("score", IntegerType()),
    StructField("rules", StringType()), StructField("is_alert", BooleanType()),
])

scored = (joined.select(*input_cols).mapInPandas(score_partition, schema)
          .withColumn("event_date", F.to_date("event_timestamp")))
scored.cache()
write_delta_partitions(spark, scored, GOLD_SCORES)

n = scored.count()
alerts = scored.filter("is_alert").count()
print(f"scored day={day} payments={n} alerts={alerts} alert_rate={alerts / max(n, 1):.4f}")
push_metrics("fraud_batch", [
    f'fraud_batch_payments{{day="{day}"}} {n}',
    f'fraud_batch_alerts{{day="{day}"}} {alerts}',
])
spark.stop()
