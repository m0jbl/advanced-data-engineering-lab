"""Lab configuration. Infrastructure comes from environment variables (defaults = host ports)."""
import os
from functools import lru_cache
from pathlib import Path

import yaml

ROOT = Path(os.getenv("LAB_ROOT", Path(__file__).resolve().parents[2]))
DATA_RAW = ROOT / "data" / "raw"
CONTROL_DIR = ROOT / "control"
SCENARIO_FILE = CONTROL_DIR / "scenario"

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
TOPIC_TRANSACTIONS = "airport.transactions"
TOPIC_ALERTS = "airport.fraud-alerts"
PUSHGATEWAY_URL = os.getenv("PUSHGATEWAY_URL", "")


@lru_cache
def params() -> dict:
    with open(ROOT / "params.yaml") as f:
        return yaml.safe_load(f)
