ALTER TABLE jobs
    ADD COLUMN IF NOT EXISTS worker_id text,
    ADD COLUMN IF NOT EXISTS lease_expires_at timestamptz,
    ADD COLUMN IF NOT EXISTS attempt_count integer NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS jobs_reclaimable_lease_idx
    ON jobs (lease_expires_at)
    WHERE status = 'running';
