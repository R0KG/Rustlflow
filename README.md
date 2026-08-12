# RustFlow

RustFlow is a Rust service for running a limited SQL query over uploaded CSV
telemetry. The API stores dataset metadata in PostgreSQL, splits large files
into partitions, and sends partition tasks to Rust workers through NATS
JetStream. Workers execute queries with DataFusion, store immutable results in
MinIO, and return a merged JSON result.

The project is intended for local development and systems learning. It has no
authentication or multi-tenant isolation, and its distributed SQL grammar is
intentionally small.

## Architecture

~~~text
client -> Axum API -> PostgreSQL
                     |
                     v
                NATS JetStream -> Rust workers -> DataFusion
                     |                              |
                     +---------- MinIO <------------+
~~~

PostgreSQL holds dataset, job, partition, and task state. NATS prompts workers
to process tasks. A PostgreSQL lease prevents two workers from processing the
same active task. MinIO holds source files, CSV partitions, and JSON results.

## Run locally

Docker Desktop and Docker Compose are required.

~~~bash
docker compose -f deploy/compose.yaml up -d --build --scale worker=3
curl http://localhost:3000/health
~~~

The API listens on port 3000. Jaeger is available at
http://localhost:16686 and the MinIO console at http://localhost:9001.

Stop the stack without deleting local data:

~~~bash
docker compose -f deploy/compose.yaml down
~~~

To delete local Compose volumes as well:

~~~bash
docker compose -f deploy/compose.yaml down -v
~~~

## API flow

Upload a CSV file:

~~~bash
curl -X POST http://localhost:3000/datasets \
  -F "file=@fixtures/distributed-events.csv"
~~~

Create a job with the returned dataset ID:

~~~bash
curl -X POST http://localhost:3000/jobs \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: local-example-1" \
  -d '{
    "dataset_id": "DATASET_ID",
    "sql": "SELECT endpoint, COUNT(*) AS requests FROM events GROUP BY endpoint"
  }'
~~~

Poll the returned job ID:

~~~bash
curl http://localhost:3000/jobs/JOB_ID
curl http://localhost:3000/jobs/JOB_ID/result
~~~

The supported distributed query shape is:

~~~sql
SELECT column_name, COUNT(*) AS count_name
FROM events
GROUP BY column_name
~~~

The API reference is in [docs/api.md](docs/api.md). The OpenAPI document is
served at /openapi.yaml while the API is running.

## Development

Rust checks:

~~~bash
cargo fmt --check
cargo test
~~~

cargo test uses the PostgreSQL connection configured in .env.test.

Python tooling provides a typed client, deterministic CSV generation, Polars
result checks, and Locust scenarios:

~~~bash
cd python
uv sync --group dev
uv run pytest
~~~

## Documentation

- [Architecture](docs/architecture.md)
- [Docker Compose](docs/docker.md)
- [API](docs/api.md)
- [Failure handling](docs/failure-model.md)
- [Storage lifecycle](docs/storage-lifecycle.md)
- [Python tooling](docs/python.md)

## License

See [LICENSE](LICENSE).
