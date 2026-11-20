"""Delta history of the gold features table: one row per commit (Lab 06). Run: make history"""
from common import GOLD_FEATURES, session

spark = session("history")
spark.sql(f"DESCRIBE HISTORY delta.`{GOLD_FEATURES}`") \
     .select("version", "timestamp", "operation", "operationParameters").show(truncate=False)
spark.stop()
