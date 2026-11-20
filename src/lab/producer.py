"""Airport payment terminals -> Kafka topic `airport.transactions` (key = passenger_id).

Switch scenario live without restarting: make scenario-normal | scenario-drift | scenario-attack
"""
import argparse
import json
import os
import time

import numpy as np
import pandas as pd
from confluent_kafka import Producer

from lab import config
from lab.simulation import SCENARIOS, make_passengers, make_transactions

TPS = float(os.getenv("TRANSACTIONS_PER_SECOND", "5"))


def current_scenario() -> str:
    try:
        scenario = config.SCENARIO_FILE.read_text().strip()
    except FileNotFoundError:
        scenario = os.getenv("SCENARIO", "normal")
    return scenario if scenario in SCENARIOS else "normal"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seconds", type=float, help="stop after this many seconds (default: until Ctrl+C)")
    args = parser.parse_args()
    if TPS <= 0 or (args.seconds is not None and args.seconds <= 0):
        parser.error("the rate and duration must be positive")
    p = config.params()["data"]
    passengers = make_passengers(p["n_passengers"], p["seed"])
    rng = np.random.default_rng()
    # Idempotent producer: the broker de-duplicates retries, so a retry never writes a payment twice (acks=all).
    producer = Producer({"bootstrap.servers": config.KAFKA_BOOTSTRAP, "linger.ms": 5,
                         "enable.idempotence": True})
    print(f"Producing ~{TPS}/s to {config.TOPIC_TRANSACTIONS} on {config.KAFKA_BOOTSTRAP}")

    active = None
    failures = []
    deadline = None if args.seconds is None else time.monotonic() + args.seconds

    def on_delivery(err, _message):
        if err is not None:
            failures.append(str(err))

    try:
        while deadline is None or time.monotonic() < deadline:
            scenario = current_scenario()
            if scenario != active:
                print(f"--- scenario: {scenario} ---")
                active = scenario

            txn = make_transactions(passengers, [pd.Timestamp.now(tz="UTC")], rng, scenario).iloc[0].to_dict()
            txn.pop("is_fraud")  # ground truth is not known at payment time
            txn["event_timestamp"] = txn["event_timestamp"].isoformat()
            event = {k: (v.item() if hasattr(v, "item") else v) for k, v in txn.items()}
            event["scenario"] = scenario

            producer.produce(config.TOPIC_TRANSACTIONS, key=str(event["passenger_id"]),
                             value=json.dumps(event), on_delivery=on_delivery)
            producer.poll(0)
            print(f"queued {event['airport']} {event['merchant_category']:<17} EUR {event['amount']:>9,.2f} "
                  f"passenger {event['passenger_id']}")
            time.sleep(1 / TPS)
    except KeyboardInterrupt:
        pass
    finally:
        pending = producer.flush(10)
    if pending or failures:
        raise SystemExit(f"Kafka delivery incomplete: {pending} pending, {len(failures)} failed {failures[:1]}")


if __name__ == "__main__":
    main()
