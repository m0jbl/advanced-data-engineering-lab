"""Quick runtime contract for Spark and Delta, using an isolated MinIO prefix."""
import os
from datetime import date
from uuid import uuid4

from pyspark.sql import functions as F

from common import read_delta, session, table_exists, write_delta_partitions

spark = session("smoke_delta")
spark.sparkContext.setLogLevel("WARN")
base = f"{os.getenv('LAB_DELTA_SMOKE_ROOT', 's3a://silver/_smoke').rstrip('/')}/{uuid4().hex}"
path = f"{base}/payments"
cdc = f"{base}/blocklist"

def records(rows):
    return spark.createDataFrame(rows, "payment_id long, event_date date")

first, second = date(2026, 1, 1), date(2026, 1, 2)
try:
    assert not table_exists(spark, path)
    write_delta_partitions(spark, records([(1, first), (2, second)]), path)
    write_delta_partitions(spark, records([(3, second)]), path)
    assert {r.payment_id for r in read_delta(spark, path).collect()} == {1, 3}
    assert {r.payment_id for r in spark.read.format("delta").option("versionAsOf", 0).load(path).collect()} == {1, 2}

    spark.createDataFrame([(7,)], "passenger_id long").write.format("delta").option(
        "delta.enableChangeDataFeed", "true").save(cdc)
    spark.sql(f"""MERGE INTO delta.`{cdc}` t USING (SELECT 7L AS passenger_id) s
                  ON t.passenger_id = s.passenger_id WHEN MATCHED THEN DELETE""")
    changes = spark.read.format("delta").option("readChangeFeed", "true").option("startingVersion", 0).load(cdc)
    assert {r._change_type for r in changes.select("_change_type").collect()} == {"insert", "delete"}
    assert read_delta(spark, cdc).count() == 0
    print(f"PASS: Delta write, replaceWhere, time travel, MERGE and Change Data Feed ({base})")
finally:
    jvm = spark.sparkContext._jvm
    root = jvm.org.apache.hadoop.fs.Path(base)
    root.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration()).delete(root, True)
    spark.stop()
