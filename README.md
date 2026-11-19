
# Python Request Scheduling Engine

A dependency-free, in-memory request scheduler demonstrating:

- FIFO scheduling
- Priority scheduling
- Stable priority ordering
- Dictionary-based request tracking
- Heap-based priority queues
- Request lifecycle management
- Batching
- Token budgets
- Request expiration
- Queue limits
- Scheduling fairness
- Thread safety
- Unit testing
- Performance benchmarking

## Requirements

Python 3.10+

No external dependencies.

## Project Structure

request_scheduler_engine/

    request_scheduler.py

    tests/
        test_scheduler.py

    benchmarks/
        benchmark.py

    README.md

    pyproject.toml

## Quick Start

```python
from request_scheduler import RequestScheduler

scheduler = RequestScheduler(
    policy="priority",
    max_queue_size=10000,
    max_batch_size=8,
    token_budget=2000,
    default_ttl=60,
    max_priority_streak=5
)

request = scheduler.create_request(
    payload={
        "prompt": "Summarize this report"
    },
    priority=10,
    token_cost=450,
    metadata={
        "tenant": "acme",
        "model": "example-model"
    }
)

batch = scheduler.dequeue_batch()

for item in batch:

    # Execute request here.

    scheduler.complete_request(
        item.request_id
    )

print(scheduler.stats())
```

## API

### Create request

create_request(
    payload,
    priority=0,
    token_cost=1,
    metadata=None,
    request_id=None,
    ttl=None
)

### Retrieve request

get_request(request_id)

### Schedule a batch

dequeue_batch(
    max_batch_size=None,
    token_budget=None
)

### Complete request

complete_request(request_id)

### Cancel request

cancel_request(request_id)

### View pending requests

pending_requests()

### View statistics

stats()

## Data Structures

| Operation | Data Structure | Complexity |
|---|---|---|
| Request lookup | Dictionary | O(1) average |
| FIFO enqueue | Deque | O(1) |
| FIFO dequeue | Deque | O(1) |
| Priority enqueue | Binary heap | O(log n) |
| Priority dequeue | Binary heap | O(log n) |
| Stable ordering | Sequence counter | O(1) |
| Cancellation | Lazy invalidation | O(1) |
| Fairness fallback | Registry scan and heap rebuild | O(n) |

## Priority Scheduling

Higher priority values are scheduled first.

The priority heap uses:

(-priority, sequence, request_id)

The negative priority converts Python's min-heap into
a highest-priority-first queue.

The sequence number preserves FIFO ordering when
multiple requests have equal priority.

## Token Budget

Requests that exceed the remaining token budget
are skipped for the current batch.

Smaller requests can still be scheduled.

Skipped requests remain queued.

## Request Lifecycle

queued -> dispatched -> completed

queued -> cancelled

queued -> expired

dispatched -> cancelled

Dispatched requests are reserved for workers and
are no longer eligible for another scheduling batch.

## Fairness

After a configured number of consecutive priority
selections, the oldest queued request is selected.

This prevents starvation of older low-priority work.

## Run Tests

python -m unittest discover -s tests -v

## Run Benchmark

python benchmarks/benchmark.py 100000

## Production Considerations

This is an in-memory educational implementation.

A production system should additionally consider:

- Persistent storage
- Distributed worker coordination
- Worker leases and visibility timeouts
- Retry policies
- Dead-letter queues
- Idempotency
- Monitoring and metrics
- Crash recovery
