# Python tooling

The Python package is an external client and verification layer. It uploads
CSV data through the public API, generates deterministic telemetry, compares
grouped counts with Polars, and provides a Locust load scenario.

## Setup

~~~bash
cd python
uv sync --group dev
~~~

## Tests

The reference calculation test does not require Docker:

~~~bash
cd python
uv run pytest tests/test_reference.py
~~~

The end-to-end test requires the Compose stack at http://127.0.0.1:3000:

~~~bash
cd python
uv run pytest tests/test_e2e.py
~~~

## Validate a distributed job

This command generates 20,000 rows, uploads the CSV, waits for the job, and
compares the API result with Polars:

~~~bash
cd python
uv run python scripts/validate_results.py --rows 20000
~~~

The default partition size is 10,000 rows, so this run creates multiple
partitions.

## Load testing

Locust repeatedly uploads a small CSV and submits a job. It measures API
upload and job-submission requests. It does not wait for every job result.

~~~bash
cd python
uv run locust -f locustfile.py --host http://127.0.0.1:3000
~~~

## Benchmarking

Scale workers with Compose, then run the benchmark from the Python directory:

~~~bash
docker compose -f deploy/compose.yaml up -d --scale worker=1
cd python && uv run python scripts/benchmark.py --workers 1 --output benchmarks/one-worker.json
~~~

Repeat with two and four worker replicas. Compare the full list of durations
and the mean value. The small default workload is useful for checking the
system, not for measuring linear worker scaling.
