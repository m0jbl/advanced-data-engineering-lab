-- Passenger blocklist maintained by fraud operations. Debezium captures every change.
CREATE TABLE IF NOT EXISTS passenger_blocklist (
  passenger_id BIGINT PRIMARY KEY,
  reason       VARCHAR(100) NOT NULL,
  blocked_at   BIGINT NOT NULL,        -- epoch millis, same unit as payment timestamps
  unblocked_at BIGINT
);
ALTER TABLE passenger_blocklist REPLICA IDENTITY FULL;
-- Two starting entries so the lab has something to see
INSERT INTO passenger_blocklist (passenger_id, reason, blocked_at)
VALUES (7, 'chargeback_history', 1700000000000)
ON CONFLICT (passenger_id) DO NOTHING;
