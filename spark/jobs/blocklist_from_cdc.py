"""CDC events (Debezium, topic fraud.public.passenger_blocklist) -> silver/blocklist (Delta, MERGE).

silver/blocklist always holds the ACTIVE blocklist (one row per blocked passenger).
The table has the Delta Change Data Feed on, so every block (insert) and unblock (delete)
is also recorded as a table change. Run: docker compose exec -T spark-master /opt/spark/bin/spark-submit /opt/spark-apps/blocklist_from_cdc.py
"""
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from common import KAFKA_INTERNAL, SILVER_BLOCKLIST, session, table_exists

CHANGE_SCHEMA = StructType([
    StructField("passenger_id", LongType()), StructField("reason", StringType()),
    StructField("blocked_at", LongType()), StructField("unblocked_at", LongType()),
    StructField("__op", StringType()), StructField("__source_ts_ms", LongType()),
])

spark = session("blocklist_from_cdc")
changes = (spark.read.format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_INTERNAL)
           .option("subscribe", "fraud.public.passenger_blocklist")
           .option("startingOffsets", "earliest")
           .load()
           .select(F.from_json(F.col("value").cast("string"), CHANGE_SCHEMA).alias("e"),
                   F.col("offset").alias("kafka_offset"))
           .select("e.*", "kafka_offset")
           .filter(F.col("passenger_id").isNotNull()))

# Latest state per passenger, including unblocks and deletes (they must reach the MERGE)
# A passenger's Kafka key always maps to one partition; offset breaks millisecond timestamp ties.
w = Window.partitionBy("passenger_id").orderBy(F.desc("kafka_offset"))
latest = (changes.withColumn("rn", F.row_number().over(w))
          .filter(F.col("rn") == 1).drop("rn")
          .select("passenger_id", "reason", "blocked_at", "unblocked_at", F.col("__op").alias("op")))
latest.createOrReplaceTempView("cdc_latest")

if not table_exists(spark, SILVER_BLOCKLIST):
    # First run: create the table with the change feed enabled, containing only active rows
    (latest.filter((F.col("op") != "d") & F.col("unblocked_at").isNull())
           .select("passenger_id", "reason", "blocked_at")
           .write.format("delta")
           .option("delta.enableChangeDataFeed", "true")
           .save(SILVER_BLOCKLIST))
    print("created silver/blocklist with change data feed")
else:
    spark.sql(f"""
        MERGE INTO delta.`{SILVER_BLOCKLIST}` AS t
        USING cdc_latest AS s
        ON t.passenger_id = s.passenger_id
        WHEN MATCHED AND (s.op = 'd' OR s.unblocked_at IS NOT NULL) THEN DELETE
        WHEN MATCHED AND (NOT (t.reason <=> s.reason) OR NOT (t.blocked_at <=> s.blocked_at))
            THEN UPDATE SET t.reason = s.reason, t.blocked_at = s.blocked_at
        WHEN NOT MATCHED AND s.op != 'd' AND s.unblocked_at IS NULL
            THEN INSERT (passenger_id, reason, blocked_at) VALUES (s.passenger_id, s.reason, s.blocked_at)
    """)
    print("merged CDC changes into silver/blocklist")

print("active blocked passengers:", spark.read.format("delta").load(SILVER_BLOCKLIST).count())
spark.stop()
