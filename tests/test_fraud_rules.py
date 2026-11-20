import sys, pathlib
import pandas as pd
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "spark" / "jobs"))
import fraud_rules as fr


def base(**overrides):
    row = {"transaction_id": "t", "passenger_id": 1, "event_timestamp": "2024-01-15T13:00:00Z",
           "airport": "LIS", "merchant_category": "food_beverage", "amount": 12.0,
           "hour_of_day": 13, "is_international": 0, "card_present": 1, "minutes_to_departure": 120.0,
           "avg_amount_30d": 15.0, "txn_count_7d": 5.0, "distinct_airports_30d": 3.0,
           "account_age_days": 800.0, "blocked": False}
    row.update(overrides)
    return row


def score_one(**overrides):
    return fr.score_frame(pd.DataFrame([base(**overrides)])).iloc[0]


def test_normal_payment_is_not_an_alert():
    r = score_one()
    assert r["score"] == 0 and not r["is_alert"] and r["rules"] == ""


def test_amount_spike_fires_at_four_times_average():
    assert "AMOUNT_SPIKE" in score_one(amount=60.0, avg_amount_30d=15.0)["rules"]
    assert "AMOUNT_SPIKE" not in score_one(amount=59.0, avg_amount_30d=15.0)["rules"]


def test_no_history_uses_default_baseline():
    r = score_one(amount=250.0, avg_amount_30d=None)   # 250 / 50 = 5x
    assert "AMOUNT_SPIKE" in r["rules"]


def test_night_and_last_minute():
    r = score_one(hour_of_day=3, minutes_to_departure=10.0)
    assert r["score"] == fr.RULE_POINTS["NIGHT_PAYMENT"] + fr.RULE_POINTS["LAST_MINUTE"]


def test_card_not_present_only_for_high_risk_merchants():
    assert "CARD_NOT_PRESENT_HIGH_RISK" in score_one(card_present=0, merchant_category="currency_exchange")["rules"]
    assert "CARD_NOT_PRESENT_HIGH_RISK" not in score_one(card_present=0, merchant_category="lounge")["rules"]


def test_new_account_and_foreign_pattern():
    r = score_one(account_age_days=5.0, is_international=1, distinct_airports_30d=1.0)
    assert "NEW_ACCOUNT" in r["rules"] and "NEW_FOREIGN_PATTERN" in r["rules"]


def test_blocked_passenger_alerts_alone():
    r = score_one(blocked=True)
    assert r["score"] == fr.RULE_POINTS["BLOCKED_PASSENGER"] and r["is_alert"]


def test_combined_fraud_pattern_crosses_threshold():
    r = score_one(amount=200.0, avg_amount_30d=20.0, hour_of_day=2, card_present=0,
                  merchant_category="electronics", minutes_to_departure=15.0)
    assert r["is_alert"] and r["score"] >= fr.ALERT_THRESHOLD


def test_frame_scoring_matches_row_scoring():
    rows = pd.DataFrame([base(), base(hour_of_day=2, minutes_to_departure=5.0)])
    scored = fr.score_frame(rows)
    assert list(scored["score"]) == [0, fr.RULE_POINTS["NIGHT_PAYMENT"] + fr.RULE_POINTS["LAST_MINUTE"]]
