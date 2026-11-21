"""Scored payments of blocked passengers (Lab 08). Run: docker compose exec -T spark-master /opt/spark/bin/spark-submit /opt/spark-apps/blocked_payments.py 7"""
import argparse
from pyspark.sql import functions as F
from common import GOLD_SCORES, read_delta, session

p = argparse.ArgumentParser()
p.add_argument("passenger", type=int, nargs="?", default=7)
p.add_argument("--date", help="only show this scoring day")
args = p.parse_args()
spark = session("blocked_payments")
payments = read_delta(spark, GOLD_SCORES).filter(F.col("passenger_id") == args.passenger)
if args.date:
    payments = payments.filter(F.col("event_date") == F.lit(args.date))
payments.select("event_date", "transaction_id", "amount", "score", "rules", "is_alert").show(20, truncate=False)
spark.stop()
