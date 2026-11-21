"""Delta history of the gold features table: one row per commit (Lab 06). Run: docker compose exec -T spark-master /opt/spark/bin/spark-submit /opt/spark-apps/history.py"""
from common import GOLD_FEATURES, session

spark = session("history")
spark.sql(f"DESCRIBE HISTORY delta.`{GOLD_FEATURES}`") \
     .select("version", "timestamp", "operation", "operationParameters").show(truncate=False)
spark.stop()
