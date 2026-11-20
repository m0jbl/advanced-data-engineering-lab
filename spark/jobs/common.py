"""Shared Spark helpers. Mounted at /opt/spark-apps in the Spark containers."""
import os

from pyspark.sql import SparkSession
from pyspark.sql.types import (BooleanType, DoubleType, IntegerType, LongType, StringType, StructField,
                               StructType)

BRONZE_TRANSACTIONS = "s3a://bronze/transactions/"
BRONZE_PASSENGERS = "s3a://bronze/passengers/"
SILVER_TRANSACTIONS = "s3a://silver/transactions/"
QUARANTINE_TRANSACTIONS = "s3a://quarantine/transactions/"
QUARANTINE_STREAM = "s3a://quarantine/stream_transactions/"
SILVER_BLOCKLIST = "s3a://silver/blocklist/"
GOLD_FEATURES = "s3a://gold/passenger_features/"
GOLD_SCORES = "s3a://gold/fraud_scores/"
GOLD_STREAM_SCORES = "s3a://gold/stream_scores/"
GOLD_MASKED = "s3a://gold/transactions_masked/"
ALERTS_TOPIC = "airport.fraud-alerts"
TRANSACTIONS_TOPIC = "airport.transactions"
KAFKA_INTERNAL = "kafka:9092"
EVALUATION_REPORT = "/tmp/airport-evaluation.json"
LABELS_FILE = "/opt/spark-data/raw/labels.csv"


def session(name: str) -> SparkSession:
    return (SparkSession.builder.appName(name)
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.databricks.delta.snapshotPartitions", "2")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .getOrCreate())


def table_exists(spark: SparkSession, path: str) -> bool:
    """True when a Delta table exists at `path`."""
    from delta.tables import DeltaTable
    # Do not silently treat authentication, network or corrupt-table errors as 'missing'.
    return DeltaTable.isDeltaTable(spark, path)


def read_delta(spark: SparkSession, path: str):
    return spark.read.format("delta").load(path)


def write_delta_partitions(spark: SparkSession, df, path: str, partition_col: str = "event_date") -> None:
    """Idempotent daily write: replace only the partitions present in `df` (Delta replaceWhere).

    The first write creates the table. Later writes leave every other partition untouched.
    """
    dates = sorted({str(r[0]) for r in df.select(partition_col).distinct().collect()})
    if not dates:
        return
    w = df.write.format("delta").partitionBy(partition_col)
    if table_exists(spark, path):
        predicate = f"{partition_col} IN ({', '.join(repr(d) for d in dates)})"
        w.mode("overwrite").option("replaceWhere", predicate).save(path)
    else:
        w.mode("errorifexists").save(path)


def ship_rules(spark: SparkSession) -> None:
    """Executors do not see the script folder; ship the rule module explicitly."""
    spark.sparkContext.addPyFile(os.path.join("/opt/spark-apps", "fraud_rules.py"))


def push_metrics(job: str, lines: list) -> None:
    """Send metrics to the Pushgateway when PUSHGATEWAY_URL is set (Lab 10).

    The Pushgateway runs only with the monitoring profile (`make up LAB=10`); when it is down, skip with a note.
    """
    url = os.getenv("PUSHGATEWAY_URL")
    if not url:
        return
    import urllib.error
    import urllib.request
    req = urllib.request.Request(f"{url}/metrics/job/{job}", data=("\n".join(lines) + "\n").encode(), method="PUT")
    try:
        urllib.request.urlopen(req, timeout=5)
    except (urllib.error.URLError, OSError) as exc:
        print(f"metrics not pushed ({exc}); start the monitoring profile with `make up LAB=10` to collect them")


# CSV written by the batch generator (event_timestamp read as text, parsed with to_timestamp)
TRANSACTION_CSV_SCHEMA = StructType([
    StructField("transaction_id", StringType()), StructField("passenger_id", LongType()),
    StructField("event_timestamp", StringType()), StructField("airport", StringType()),
    StructField("merchant_category", StringType()), StructField("amount", DoubleType()),
    StructField("hour_of_day", IntegerType()), StructField("is_international", IntegerType()),
    StructField("card_present", IntegerType()), StructField("minutes_to_departure", DoubleType()),
])

PASSENGER_CSV_SCHEMA = StructType([
    StructField("passenger_id", LongType()), StructField("home_airport", StringType()),
    StructField("home_country", StringType()), StructField("typical_spend", DoubleType()),
    StructField("account_age_days", LongType()), StructField("travel_freq", DoubleType()),
    StructField("compromised", BooleanType()),
])

# Kafka JSON produced by lab.producer (extra fields such as `scenario` are ignored)
TRANSACTION_JSON_SCHEMA = StructType([
    StructField("transaction_id", StringType()), StructField("passenger_id", LongType()),
    StructField("event_timestamp", StringType()), StructField("airport", StringType()),
    StructField("merchant_category", StringType()), StructField("amount", DoubleType()),
    StructField("hour_of_day", IntegerType()), StructField("is_international", IntegerType()),
    StructField("card_present", IntegerType()), StructField("minutes_to_departure", DoubleType()),
])
