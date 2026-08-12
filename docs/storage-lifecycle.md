# Storage lifecycle

Each dataset owns one source object and one object for each partition. Each
job owns one result object per task and one merged result object.

Objects use UUID-based keys and immutable writes. A retry may verify identical
bytes at an existing key, but it must not replace different bytes.

## Write order

1. Upload or partition code writes an object.
2. The process records its object key in PostgreSQL.
3. A worker writes a task result before completing the task row.
4. The coordinator writes the merged result before completing the parent job.

A completed database record therefore does not point to a missing object. A
database failure after an object write can leave an orphan object.

## Retention

There is no dataset deletion endpoint or scheduled orphan cleanup job yet.
An operator can identify orphan keys by comparing object storage with these
columns:

- datasets.storage_key
- dataset_partitions.storage_key
- job_tasks.result_key
- jobs.result_key

Temporary files created for DataFusion live in the operating system temporary
directory and are removed after a query completes. A process crash can leave
temporary files behind.
