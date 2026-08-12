ALTER TABLE datasets
    ADD COLUMN IF NOT EXISTS storage_key TEXT;

ALTER TABLE dataset_partitions
    ADD COLUMN IF NOT EXISTS storage_key TEXT;

ALTER TABLE jobs
    ADD COLUMN IF NOT EXISTS result_key TEXT;

ALTER TABLE job_tasks
    ADD COLUMN IF NOT EXISTS result_key TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS datasets_storage_key_unique
    ON datasets (storage_key)
    WHERE storage_key IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS dataset_partitions_storage_key_unique
    ON dataset_partitions (storage_key)
    WHERE storage_key IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS jobs_result_key_unique
    ON jobs (result_key)
    WHERE result_key IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS job_tasks_result_key_unique
    ON job_tasks (result_key)
    WHERE result_key IS NOT NULL;
