# 004: Partitioned grouped counts

## Context

Sending one complete job to several workers duplicates work. Concatenating
partial aggregate rows gives incorrect totals when a group appears in more than
one partition.

## Decision

RustFlow creates one persistent task for each dataset partition. A task has its
own state, lease, worker identity, result, and attempt count. The coordinator
completes the parent job only after every task reaches a terminal state.

The distributed query grammar supports a grouped count:

~~~sql
SELECT endpoint, COUNT(*) AS requests
FROM events
GROUP BY endpoint
~~~

Workers return partial counts. The coordinator sums rows with the same group
key before it writes the parent result.

## Consequences

- A dataset has partition metadata before its tasks run.
- Task state is recoverable independently of the parent job.
- Averages need partial sums and counts, then a weighted final calculation.
- Filters, joins, window functions, sorting, and arbitrary SQL are outside the
  current distributed grammar.
