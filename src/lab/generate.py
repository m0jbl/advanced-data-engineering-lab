"""Batch generator: historical airport payments -> data/raw/*.csv (+ labels for evaluation only).

Labels (is_fraud) are written to labels.csv, never to the transaction file: the pipeline must not see them.
"""
import numpy as np
import pandas as pd

from lab import config
from lab.simulation import make_passengers, make_transactions


def main() -> None:
    p = config.params()["data"]
    rng = np.random.default_rng(p["seed"])
    passengers = make_passengers(p["n_passengers"], p["seed"])

    end = pd.Timestamp.now(tz="UTC").normalize()
    start = end - pd.Timedelta(days=p["history_days"])
    n = p["txn_per_day"] * p["history_days"]
    offsets = np.sort(rng.uniform(0, (end - start).total_seconds(), n))
    timestamps = start + pd.to_timedelta(offsets, unit="s")

    txns = make_transactions(passengers, timestamps, rng)

    config.DATA_RAW.mkdir(parents=True, exist_ok=True)
    labels = txns[["transaction_id", "is_fraud"]]
    features = txns.drop(columns=["is_fraud"])
    passengers.to_csv(config.DATA_RAW / "passengers.csv", index=False)
    features.to_csv(config.DATA_RAW / "transactions.csv", index=False)
    labels.to_csv(config.DATA_RAW / "labels.csv", index=False)
    print(f"passengers: {len(passengers):,}")
    print(f"transactions: {len(txns):,} ({start.date()} .. {end.date()}), fraud rate {txns['is_fraud'].mean():.2%}")


if __name__ == "__main__":
    main()
