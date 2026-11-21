"""Read the Delta Change Data Feed of silver/blocklist: every block (insert) and unblock (delete).

Run: docker compose exec -T spark-master /opt/spark/bin/spark-submit /opt/spark-apps/blocklist_changes.py
"""
from pyspark.sql import functions as F

from common import SILVER_BLOCKLIST, session

spark = session("blocklist_changes")
changes = (spark.read.format("delta")
           .option("readChangeFeed", "true")
           .option("startingVersion", 0)
           .load(SILVER_BLOCKLIST))

changes.select("_commit_version", "_change_type", "passenger_id", "reason", "blocked_at") \
       .orderBy("_commit_version", "passenger_id") \
       .show(50, truncate=False)

counts = changes.groupBy("_change_type").agg(F.count("*").alias("rows"))
counts.show()
spark.stop()
