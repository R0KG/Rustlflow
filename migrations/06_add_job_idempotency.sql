ALTER TABLE jobs
    ADD COLUMN IF NOT EXISTS idempotency_key text;

CREATE UNIQUE INDEX IF NOT EXISTS jobs_idempotency_key_unique_idx
    ON jobs (idempotency_key)
    WHERE idempotency_key IS NOT NULL;
