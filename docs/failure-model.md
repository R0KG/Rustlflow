# Failure handling

RustFlow uses at-least-once task delivery. PostgreSQL is the source of truth
for jobs and tasks. JetStream delivery prompts a worker to attempt work; it
does not confirm that work finished.

| Failure | Stored state | Result |
| --- | --- | --- |
| Worker stops during a task | Task remains running until its lease expires | JetStream can redeliver and another worker can claim the expired lease |
| PostgreSQL is unavailable | The worker cannot record a terminal state | The message is left unacknowledged |
| Object storage is unavailable | No result key is recorded | The task does not report success |
| Invalid SQL or result conversion | Task is failed | The parent job is failed and the message is acknowledged |
| Task timeout | Task is failed with a timeout error | The parent job is failed and the lease is cleared |
| Duplicate job request | The idempotency key identifies the existing job | The API returns the original job for matching input |

## Retry and lease settings

JetStream uses explicit acknowledgements and allows five deliveries by default.
Its retry delays are 1, 5, 15, 30, and 60 seconds.

The related environment variables are:

- RUSTFLOW_MAX_DELIVERIES, default 5
- RUSTFLOW_LEASE_SECONDS, default 60
- RUSTFLOW_TASK_TIMEOUT_SECONDS, default 45

The database lease is separate from the JetStream acknowledgement timer. A
redelivered message does not run while another worker owns an active lease.

## Operational boundary

If JetStream reaches its delivery limit while PostgreSQL is unavailable, the
system cannot safely write a final failure state. The message remains in
JetStream for operator inspection after PostgreSQL recovers.
