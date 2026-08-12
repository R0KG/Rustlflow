# 005: Immutable object storage

## Context

Filesystem paths tie workers to a shared local volume. They do not provide a
safe storage identifier for MinIO or other S3-compatible services.

## Decision

RustFlow uses a BlobStorage trait with put, get, and delete operations.
LocalBlobStorage maps validated relative keys to one configured directory.
MinioBlobStorage uses MinIO through Apache Arrow's object_store crate.

Object keys are immutable. A write creates a new key or verifies identical
bytes at an existing key. Absolute paths, dot components, and parent
components are rejected.

The storage layer returns bytes rather than filesystem paths. DataFusion reads
from a temporary local materialization, which is removed after the query.

~~~text
datasets/<dataset-id>/source.csv
datasets/<dataset-id>/partitions/part-00000.csv
jobs/<job-id>/tasks/<task-id>/result.json
jobs/<job-id>/result.json
~~~

New records store storage_key and result_key. Legacy records without keys can
still use storage_path.

## Consequences

- Deterministic partition and result keys are safe to retry.
- Compose creates the local MinIO bucket through minio-init.
- Production deployments need HTTPS, non-default credentials, and bucket
  provisioning outside the application.
- Object writes and PostgreSQL commits are separate operations. A failed
  metadata write can leave an orphan object.
