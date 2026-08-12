# Architecture

RustFlow has an API process and one or more worker processes. The API accepts
CSV uploads and asynchronous query requests. Workers execute partition tasks.

| Component | Responsibility |
| --- | --- |
| Axum API | Upload datasets, create jobs, expose status and results |
| PostgreSQL | Dataset metadata, jobs, partitions, task state, and leases |
| NATS JetStream | Durable task delivery to workers |
| Rust workers | Claim tasks, run DataFusion, persist results |
| MinIO | Dataset sources, partitions, task results, and merged results |
| DataFusion | Executes the supported query against one CSV partition |
| Jaeger | Receives OpenTelemetry traces from the API and workers |

## Job execution

1. The client uploads a CSV file through POST /datasets.
2. The client creates a query job through POST /jobs.
3. The API creates partitions and one task per partition in PostgreSQL.
4. The API publishes task IDs to JetStream.
5. A worker claims each task with a database lease.
6. The worker runs DataFusion against the partition and writes its JSON result.
7. The coordinator merges partition counts and marks the parent job completed.

Workers use explicit JetStream acknowledgements. PostgreSQL remains the source
of truth for terminal job and task state.

## Query scope

Distributed jobs support one grouped count:

~~~sql
SELECT endpoint, COUNT(*) AS requests
FROM events
GROUP BY endpoint
~~~

The parser only accepts simple identifiers for the group column and count
alias. It does not support filters, sorting, joins, or other aggregate
functions.

## Delivery and storage

Task messages are delivered at least once. A redelivery is safe when the task
is terminal. A running task can be reclaimed only after its lease expires.

Objects use UUID-based immutable keys. The API and workers write an object
before recording its key in PostgreSQL, so a completed record never refers to
an object that was not written.
