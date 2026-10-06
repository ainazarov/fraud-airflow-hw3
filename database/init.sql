CREATE TABLE IF NOT EXISTS scores (
    transaction_id TEXT PRIMARY KEY,
    score DOUBLE PRECISION NOT NULL CHECK (score >= 0 AND score <= 1),
    fraud_flag SMALLINT NOT NULL CHECK (fraud_flag IN (0, 1)),
    model_version TEXT NOT NULL CHECK (length(model_version) > 0),
    scored_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    sequence BIGSERIAL UNIQUE NOT NULL
);
CREATE INDEX IF NOT EXISTS scores_version_idx ON scores (model_version, sequence DESC);
