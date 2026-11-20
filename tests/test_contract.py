"""The generator's output must match the payment contract (schemas/payment_event.json)."""
import json
import math
import pathlib

import numpy as np
import pandas as pd
import pytest
from jsonschema import validate

from lab.simulation import make_passengers, make_transactions

SCHEMA = json.loads((pathlib.Path(__file__).resolve().parents[1] / "schemas" / "payment_event.json").read_text())


def _event(row: dict) -> dict:
    ev = {k: (v.item() if hasattr(v, "item") else v) for k, v in row.items() if k != "is_fraud"}
    ev["event_timestamp"] = pd.Timestamp(ev["event_timestamp"]).isoformat()
    return ev


def test_generated_events_match_contract():
    pax = make_passengers(200, seed=9)
    ts = pd.date_range("2024-03-01", periods=200, freq="7min", tz="UTC")
    df = make_transactions(pax, ts, np.random.default_rng(9), "attack")
    for row in df.to_dict("records"):
        validate(_event(row), SCHEMA)


def test_contract_rejects_unknown_airport():
    bad = {"transaction_id": "x", "passenger_id": 1, "event_timestamp": "2024-01-01T00:00:00+00:00",
           "airport": "XXX", "merchant_category": "lounge", "amount": 10.0, "hour_of_day": 3,
           "is_international": 0, "card_present": 1, "minutes_to_departure": 5.0}
    with pytest.raises(Exception):
        validate(bad, SCHEMA)
