"""Compare batch alerts with the simulator's ground truth (labels never seen by the pipeline).

Writes a driver-local report; `docker compose cp spark-master:/tmp/airport-evaluation.json data/reports/evaluation.json` copies it for the Prefect quality gate.
"""
import argparse
import json
import os

from pyspark.sql import functions as F

from common import EVALUATION_REPORT, GOLD_SCORES, LABELS_FILE, read_delta, session

p = argparse.ArgumentParser()
p.add_argument("--date", help="scoring day to evaluate (default: latest scored day)")
args = p.parse_args()

spark = session("evaluate")
scores = read_delta(spark, GOLD_SCORES).select("transaction_id", "is_alert", "event_date")
day = args.date or scores.agg(F.max("event_date")).first()[0]
scores = scores.filter(F.col("event_date") == F.lit(day))
labels = spark.read.option("header", "true").option("inferSchema", "true").csv(LABELS_FILE)
joined = scores.join(labels, "transaction_id", "inner").cache()

tp = joined.filter(F.col("is_alert") & (F.col("is_fraud") == 1)).count()
fp = joined.filter(F.col("is_alert") & (F.col("is_fraud") == 0)).count()
fn = joined.filter(~F.col("is_alert") & (F.col("is_fraud") == 1)).count()
n = joined.count()
if n == 0:
    raise SystemExit(f"No scored payments match ground-truth labels for {day}.")
report = {
    "event_date": str(day),
    "payments": n,
    "frauds": tp + fn,
    "alerts": tp + fp,
    "precision": round(tp / max(tp + fp, 1), 4),
    "recall": round(tp / max(tp + fn, 1), 4),
}
print(json.dumps(report, indent=2))
os.makedirs(os.path.dirname(EVALUATION_REPORT), exist_ok=True)
with open(EVALUATION_REPORT, "w") as f:
    json.dump(report, f, indent=2)
spark.stop()
