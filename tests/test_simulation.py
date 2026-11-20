import numpy as np
import pandas as pd
from lab.simulation import SCENARIOS, make_passengers, make_transactions


def test_passengers_are_reproducible_for_a_seed():
    a = make_passengers(100, seed=1)
    b = make_passengers(100, seed=1)
    pd.testing.assert_frame_equal(a, b)


def test_fraud_rate_is_in_expected_range_for_normal_traffic():
    pax = make_passengers(500, seed=3)
    rng = np.random.default_rng(3)
    ts = pd.date_range("2024-01-01", periods=5000, freq="5min", tz="UTC")
    txns = make_transactions(pax, ts, rng, "normal")
    assert 0.005 < txns["is_fraud"].mean() < 0.06


def test_attack_scenario_raises_fraud_rate():
    pax = make_passengers(500, seed=3)
    ts = pd.date_range("2024-01-01", periods=5000, freq="5min", tz="UTC")
    normal = make_transactions(pax, ts, np.random.default_rng(4), "normal")["is_fraud"].mean()
    attack = make_transactions(pax, ts, np.random.default_rng(4), "attack")["is_fraud"].mean()
    assert attack > 3 * normal


def test_all_scenarios_produce_required_columns():
    pax = make_passengers(50, seed=5)
    ts = pd.date_range("2024-01-01", periods=20, freq="1h", tz="UTC")
    for s in SCENARIOS:
        df = make_transactions(pax, ts, np.random.default_rng(0), s)
        assert {"transaction_id", "airport", "merchant_category", "amount", "is_fraud"} <= set(df.columns)
