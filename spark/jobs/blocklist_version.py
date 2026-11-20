"""The active blocklist as it was at a Delta version (Lab 08 time travel).
Run: make blocklist-version V=2
"""
import sys
from common import SILVER_BLOCKLIST, session

version = int(sys.argv[1])
spark = session("blocklist_version")
spark.read.format("delta").option("versionAsOf", version).load(SILVER_BLOCKLIST).show()
spark.stop()
