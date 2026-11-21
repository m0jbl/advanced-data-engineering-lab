"""Analyst-safe dataset: direct identifiers removed, passenger key pseudonymised (Lab 12).

Run: docker compose exec -T spark-master /opt/spark/bin/spark-submit /opt/spark-apps/mask_payments.py
"""
from pyspark.sql import functions as F

from common import GOLD_MASKED, SILVER_TRANSACTIONS, read_delta, session, write_delta_partitions

SALT = "lab12-training-salt"   # a managed secret in production, rotated per policy

spark = session("mask_payments")

silver = read_delta(spark, SILVER_TRANSACTIONS)
masked = (silver
          .withColumn("passenger_key", F.sha2(F.concat(F.col("passenger_id").cast("string"), F.lit(SALT)), 256))
          .drop("passenger_id", "transaction_id"))
write_delta_partitions(spark, masked, GOLD_MASKED)
print("masked rows:", masked.count(), "columns:", masked.columns)
spark.stop()
