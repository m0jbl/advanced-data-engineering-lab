"""Bronze CSV -> silver Delta table. Rejected rows go to quarantine with the reason (Lab 07).

Run: docker compose exec -T spark-master /opt/spark/bin/spark-submit /opt/spark-apps/silver_transactions.py
"""
import argparse
from datetime import date

from pyspark.sql import functions as F

from common import (BRONZE_TRANSACTIONS, QUARANTINE_TRANSACTIONS, SILVER_TRANSACTIONS,
                    TRANSACTION_CSV_SCHEMA, session, write_delta_partitions)

p = argparse.ArgumentParser()
p.add_argument("--date", type=date.fromisoformat, help="replace one UTC business day (default: all bronze rows)")
args = p.parse_args()
spark = session("silver_transactions")
AIRPORTS = ["LIS", "OPO", "MAD", "BCN", "LHR", "CDG", "FRA", "AMS", "DXB", "JFK"]
CHECKS = [
    ("NULL_KEY", F.col("transaction_id").isNull() | F.col("passenger_id").isNull()),
    ("NON_POSITIVE_AMOUNT", F.col("amount").isNull() | F.isnan("amount") | (F.col("amount") <= 0)),
    ("UNKNOWN_AIRPORT", F.col("airport").isNull() | ~F.col("airport").isin(AIRPORTS)),
    ("BAD_HOUR", F.col("hour_of_day").isNull() | ~F.col("hour_of_day").between(0, 23)),
    ("BAD_TIMESTAMP", F.col("event_timestamp").isNull()),
    ("BAD_SCORING_INPUT", F.col("minutes_to_departure").isNull()
     | F.col("card_present").isNull() | F.col("is_international").isNull()),
]

raw = (spark.read.option("header", "true").schema(TRANSACTION_CSV_SCHEMA).csv(BRONZE_TRANSACTIONS)
       .withColumn("event_timestamp", F.try_to_timestamp("event_timestamp")))
if args.date:
    raw = raw.filter(F.to_date("event_timestamp") == F.lit(args.date))
raw = raw.cache()

reason = F.lit(None).cast("string")
for name, failed in reversed(CHECKS):
    reason = F.when(failed, F.lit(name)).otherwise(reason)
tagged = raw.withColumn("reject_reason", reason)

rejected = tagged.filter(F.col("reject_reason").isNotNull()).withColumn("quarantined_at", F.current_timestamp())
accepted = (tagged.filter(F.col("reject_reason").isNull()).drop("reject_reason")
            .dropDuplicates(["transaction_id"])
            .withColumn("event_date", F.to_date("event_timestamp")))

n_accepted = accepted.count()
n_rejected = rejected.count()
if not n_accepted:
    # Don't overwrite an existing healthy partition when a day contains no usable rows.
    if n_rejected:
        rejected.write.format("delta").mode("append").save(QUARANTINE_TRANSACTIONS)
    raise SystemExit("No valid payments; the silver table was left unchanged.")
write_delta_partitions(spark, accepted, SILVER_TRANSACTIONS)
if n_rejected:
    rejected.write.format("delta").mode("append").save(QUARANTINE_TRANSACTIONS)

counts = rejected.groupBy("reject_reason").count().collect()
print(f"bronze={raw.count()} silver={n_accepted} rejected={n_rejected} "
      f"reasons={[(r['reject_reason'], r['count']) for r in counts]}")
spark.stop()
