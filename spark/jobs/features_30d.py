"""Passenger profile features, computed as of each day with no leakage.

For each event_date D, features use only payments BEFORE D:
  avg_amount_30d         mean amount over the previous 30 days
  txn_count_7d           payments in the previous 7 days
  distinct_airports_30d  airports used in the previous 30 days
  account_age_days       from the passenger file

Output: gold/passenger_features partitioned by as_of. Run: make features
"""
from pyspark.sql import functions as F

from common import BRONZE_PASSENGERS, GOLD_FEATURES, PASSENGER_CSV_SCHEMA, SILVER_TRANSACTIONS, read_delta, session

spark = session("features_30d")
silver = read_delta(spark, SILVER_TRANSACTIONS)
passengers = spark.read.option("header", "true").schema(PASSENGER_CSV_SCHEMA).csv(BRONZE_PASSENGERS)
days = silver.select(F.col("event_date").alias("as_of")).distinct().cache()
n_days = days.count()
if not n_days:
    raise SystemExit("No valid days in silver; run make silver with valid transactions first.")

# Match each payment only to later snapshots within its 30-day lifetime.
history = silver.select("passenger_id", "amount", "airport", "event_date").join(
    F.broadcast(days),
    (F.col("event_date") < F.col("as_of")) & (F.col("event_date") >= F.date_sub("as_of", 30)),
)
agg = history.groupBy("passenger_id", "as_of").agg(
    F.avg("amount").alias("avg_amount_30d"),
    F.countDistinct("airport").alias("distinct_airports_30d"),
    F.count(F.when(F.col("event_date") >= F.date_sub("as_of", 7), 1)).alias("txn_count_7d"),
)
out = (passengers.select("passenger_id", "account_age_days").crossJoin(F.broadcast(days))
       .join(agg, ["passenger_id", "as_of"], "left")
       .fillna({"txn_count_7d": 0, "distinct_airports_30d": 0}))
out.write.format("delta").mode("overwrite").partitionBy("as_of").save(GOLD_FEATURES)
print(f"features written for {n_days} as-of dates")
spark.stop()
