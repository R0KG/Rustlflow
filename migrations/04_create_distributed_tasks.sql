CREATE TABLE dataset_partitions (
    id uuid PRIMARY KEY,
    dataset_id uuid NOT NULL REFERENCES datasets (id) ON DELETE CASCADE,
    partition_number integer NOT NULL CHECK (partition_number >= 0),
    storage_path text NOT NULL,
    row_count bigint NOT NULL CHECK (row_count >= 0),
    created_at timestamptz NOT NULL DEFAULT NOW(),
    UNIQUE (dataset_id, partition_number)
);

CREATE TABLE job_tasks (
    id uuid PRIMARY KEY,
    job_id uuid NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    dataset_partition_id uuid NOT NULL REFERENCES dataset_partitions (id),
    status text NOT NULL CHECK (status IN ('queued', 'running', 'completed', 'failed', 'cancelled')),
    result jsonb,
    error text,
    worker_id text,
    lease_expires_at timestamptz,
    attempt_count integer NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT NOW(),
    started_at timestamptz,
    completed_at timestamptz,
    UNIQUE (job_id, dataset_partition_id)
);

CREATE INDEX job_tasks_pending_idx
    ON job_tasks (job_id, status);

CREATE INDEX job_tasks_reclaimable_lease_idx
    ON job_tasks (lease_expires_at)
    WHERE status = 'running';
