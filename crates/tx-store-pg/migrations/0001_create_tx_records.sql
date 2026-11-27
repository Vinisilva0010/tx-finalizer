CREATE TABLE IF NOT EXISTS tx_records (
    idempotency_key TEXT PRIMARY KEY,
    tx_hash TEXT NOT NULL,
    payload_sha256 BYTEA NOT NULL CHECK (octet_length(payload_sha256) = 32),
    raw_bytes BYTEA NOT NULL,
    constraint_json JSONB NOT NULL,
    state_json JSONB NOT NULL,
    reorg_count INTEGER NOT NULL DEFAULT 0 CHECK (reorg_count >= 0),
    version BIGINT NOT NULL CHECK (version >= 1),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tx_records_tx_hash ON tx_records (tx_hash);
