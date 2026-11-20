"""Transparent fraud rules for airport payments. Pure pandas: runs in Spark (mapInPandas) and in unit tests.

Each rule adds points to a score. A payment is an alert when score >= ALERT_THRESHOLD.
Rules are the lab's teaching baseline; they are deliberately simple and easy to tune.
"""
import numpy as np
import pandas as pd

ALERT_THRESHOLD = 50
DEFAULT_TYPICAL_SPEND = 50.0           # used when a passenger has no history yet
HIGH_RISK_MERCHANTS = ("currency_exchange", "electronics")

# name -> points. Keep names stable: they appear in alerts, dashboards and tests.
RULE_POINTS = {
    "AMOUNT_SPIKE": 40,              # amount >= 4x the passenger's 30-day average
    "NIGHT_PAYMENT": 15,             # between 00:00 and 05:59 local hour
    "LAST_MINUTE": 15,               # less than 30 minutes before departure
    "CARD_NOT_PRESENT_HIGH_RISK": 20,  # no card present at exchange/electronics
    "NEW_FOREIGN_PATTERN": 10,       # international payment, few airports in 30 days
    "NEW_ACCOUNT": 10,               # account younger than 30 days
    "BLOCKED_PASSENGER": 100,        # passenger on the blocklist
}

FEATURE_COLUMNS = ["avg_amount_30d", "txn_count_7d", "distinct_airports_30d", "account_age_days"]


def score_frame(df: pd.DataFrame, threshold: int = ALERT_THRESHOLD) -> pd.DataFrame:
    """Score a frame of payments that already has the feature columns joined (missing values allowed)."""
    out = df.copy()
    avg = pd.to_numeric(out["avg_amount_30d"], errors="coerce").fillna(0.0)
    baseline = np.where(avg > 0, avg, DEFAULT_TYPICAL_SPEND)
    out["amount_ratio"] = out["amount"].astype(float) / baseline

    flags = {
        "AMOUNT_SPIKE": out["amount_ratio"] >= 4,
        "NIGHT_PAYMENT": out["hour_of_day"].astype(int) < 6,
        "LAST_MINUTE": out["minutes_to_departure"].astype(float) < 30,
        "CARD_NOT_PRESENT_HIGH_RISK": (out["card_present"].astype(int) == 0)
                                      & out["merchant_category"].isin(HIGH_RISK_MERCHANTS),
        "NEW_FOREIGN_PATTERN": (out["is_international"].astype(int) == 1)
                               & (out["distinct_airports_30d"].fillna(0).astype(float) < 2),
        "NEW_ACCOUNT": out["account_age_days"].fillna(9999).astype(float) < 30,
        "BLOCKED_PASSENGER": out["blocked"].fillna(False).astype(bool),
    }
    score = np.zeros(len(out))
    reasons = [[] for _ in range(len(out))]
    for name, mask in flags.items():
        mask = np.asarray(mask, dtype=bool)
        score += mask * RULE_POINTS[name]
        for i in np.flatnonzero(mask):
            reasons[i].append(name)

    out["score"] = score.astype(int)
    out["rules"] = [",".join(r) for r in reasons]
    out["is_alert"] = out["score"] >= threshold
    return out


def output_columns() -> list:
    return ["transaction_id", "passenger_id", "event_timestamp", "airport", "merchant_category",
            "amount", "amount_ratio", "score", "rules", "is_alert"]
