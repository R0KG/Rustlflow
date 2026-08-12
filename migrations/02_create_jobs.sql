CREATE TABLE jobs (
    id uuid PRIMARY KEY,
    dataset_id uuid NOT NULL REFERENCES datasets (id),
    sql text NOT NULL,
    status text NOT NULL CHECK (status IN ('queued', 'running', 'completed', 'failed', 'cancelled')),
    result jsonb,
    error text,
    created_at timestamptz NOT NULL DEFAULT NOW(),
    started_at timestamptz,
    completed_at timestamptz
);
