# API

The API listens on http://localhost:3000 when started with Docker Compose.
Every response includes X-Request-Id. Utoipa generates the OpenAPI document
from the Rust transport types and handler annotations at /openapi.yaml.

## Upload a dataset

~~~http
POST /datasets
Content-Type: multipart/form-data
~~~

Send one non-empty CSV file in the file field.

~~~json
{
  "id": "dataset-uuid",
  "filename": "events.csv",
  "format": "csv",
  "status": "stored"
}
~~~

The endpoint returns 201 on success and 400 for an empty, invalid, or
non-CSV upload.

## Create a job

~~~http
POST /jobs
Content-Type: application/json
Idempotency-Key: client-generated-key
~~~

~~~json
{
  "dataset_id": "dataset-uuid",
  "sql": "SELECT endpoint, COUNT(*) AS requests FROM events GROUP BY endpoint"
}
~~~

~~~json
{
  "job_id": "job-uuid",
  "status": "queued"
}
~~~

The endpoint returns 202 when it creates a job. Repeating the same request
with the same Idempotency-Key returns the original job with 200. Reusing a key
with different input returns 409.

The distributed job parser accepts:

~~~sql
SELECT group_column, COUNT(*) AS count_alias
FROM events
GROUP BY group_column
~~~

Both names must be simple identifiers. Filters, joins, sorting, and other
aggregate functions are rejected.

## Read job status

~~~http
GET /jobs/{job_id}
~~~

~~~json
{
  "job_id": "job-uuid",
  "status": "queued"
}
~~~

Possible states are queued, running, completed, failed, and cancelled.

## Read a result

~~~http
GET /jobs/{job_id}/result
~~~

~~~json
{
  "job_id": "job-uuid",
  "result": {
    "row_count": 2,
    "rows": [
      {"endpoint": "/login", "requests": 10},
      {"endpoint": "/payments", "requests": 7}
    ]
  }
}
~~~

The endpoint returns 409 until a result is ready. It also returns 409 for a
failed or cancelled job.

## Cancel a job

~~~http
POST /jobs/{job_id}/cancel
~~~

Queued and running jobs can be cancelled. Terminal jobs return 409.

## Metrics

~~~http
GET /metrics
~~~

The endpoint returns Prometheus text metrics for jobs, tasks, worker activity,
queue depth, and scanned rows.

## Errors

Errors use this shape:

~~~json
{
  "code": "not_found",
  "message": "Job was not found"
}
~~~
