"""
Request scheduler performance benchmark.

Usage:
    python benchmarks/benchmark.py
    python benchmarks/benchmark.py 100000
"""

import sys
import time
from pathlib import Path

package_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(package_root))

from request_scheduler import RequestScheduler


count = (
    int(sys.argv[1])
    if len(sys.argv) > 1
    else 100_000
)

if count <= 0:
    raise ValueError("Request count must be positive")


for policy in ("fifo", "priority"):

    scheduler = RequestScheduler(
        policy=policy,
        max_batch_size=256
    )

    start = time.perf_counter()

    # Request creation benchmark.
    for i in range(count):

        scheduler.create_request(
            payload={"index": i},
            priority=i % 10
        )

    created = time.perf_counter()

    # Scheduling benchmark.
    dequeued = 0

    while True:

        batch = scheduler.dequeue_batch()

        if not batch:
            break

        dequeued += len(batch)

    end = time.perf_counter()

    total_time = end - start

    throughput = (
        dequeued / total_time
        if total_time > 0
        else 0
    )

    print(
        f"{policy:8} "
        f"requests={dequeued:,} "
        f"create={created - start:.3f}s "
        f"total={total_time:.3f}s "
        f"throughput={throughput:,.0f} req/s"
    )
