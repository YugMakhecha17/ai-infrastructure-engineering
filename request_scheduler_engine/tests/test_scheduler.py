import unittest

try:
    from ..request_scheduler import (
        QueueFullError,
        RequestScheduler,
        RequestState,
        ValidationError,
    )
except ImportError:
    from request_scheduler import (
        QueueFullError,
        RequestScheduler,
        RequestState,
        ValidationError,
    )


class FakeClock:

    def __init__(self):
        self.now = 100.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


class RequestSchedulerTests(unittest.TestCase):

    def test_fifo_order_and_generated_ids(self):

        scheduler = RequestScheduler()

        a = scheduler.create_request("A")
        b = scheduler.create_request("B")

        self.assertNotEqual(
            a.request_id,
            b.request_id
        )

        batch = scheduler.dequeue_batch(
            max_batch_size=2
        )

        self.assertEqual(
            [r.payload for r in batch],
            ["A", "B"]
        )

    def test_priority_order_and_stable_ties(self):

        scheduler = RequestScheduler("priority")

        scheduler.create_request("low", priority=1)
        scheduler.create_request("high-1", priority=9)
        scheduler.create_request("high-2", priority=9)

        batch = scheduler.dequeue_batch(
            max_batch_size=3
        )

        self.assertEqual(
            [r.payload for r in batch],
            ["high-1", "high-2", "low"]
        )

    def test_completion_and_cancellation(self):

        scheduler = RequestScheduler()

        a = scheduler.create_request("A")
        b = scheduler.create_request("B")

        scheduler.complete_request(a.request_id)
        scheduler.cancel_request(b.request_id)

        stats = scheduler.stats()

        self.assertEqual(stats.completed, 1)
        self.assertEqual(stats.cancelled, 1)
        self.assertEqual(stats.queued, 0)

    def test_batch_token_budget(self):

        scheduler = RequestScheduler(
            max_batch_size=4,
            token_budget=5
        )

        scheduler.create_request(
            "large",
            token_cost=8
        )

        scheduler.create_request(
            "small-1",
            token_cost=2
        )

        scheduler.create_request(
            "small-2",
            token_cost=3
        )

        batch = scheduler.dequeue_batch()

        self.assertEqual(
            [r.payload for r in batch],
            ["small-1", "small-2"]
        )

        self.assertEqual(
            [r.payload for r in scheduler.pending_requests()],
            ["large"]
        )

        self.assertEqual(
            scheduler.stats().in_progress,
            2
        )

        for request in batch:
            scheduler.complete_request(
                request.request_id
            )

        self.assertEqual(
            scheduler.stats().completed,
            2
        )

    def test_expiration(self):

        clock = FakeClock()

        scheduler = RequestScheduler(
            default_ttl=2,
            clock=clock
        )

        scheduler.create_request("short-lived")

        clock.advance(3)

        self.assertEqual(
            scheduler.stats().expired,
            1
        )

        self.assertEqual(
            scheduler.pending_requests(),
            []
        )

    def test_queue_limit(self):

        scheduler = RequestScheduler(
            max_queue_size=1
        )

        scheduler.create_request("one")

        with self.assertRaises(QueueFullError):
            scheduler.create_request("two")

    def test_validation_and_duplicate_ids(self):

        scheduler = RequestScheduler()

        with self.assertRaises(ValidationError):
            scheduler.create_request(
                "bad",
                token_cost=0
            )

        scheduler.create_request(
            "one",
            request_id="fixed"
        )

        with self.assertRaises(ValidationError):
            scheduler.create_request(
                "two",
                request_id="fixed"
            )

        with self.assertRaises(ValidationError):
            RequestScheduler(max_batch_size=0)

    def test_fairness_after_priority_streak(self):

        scheduler = RequestScheduler(
            "priority",
            max_priority_streak=2
        )

        scheduler.create_request(
            "old-low",
            priority=0
        )

        scheduler.create_request(
            "high-1",
            priority=10
        )

        scheduler.create_request(
            "high-2",
            priority=10
        )

        scheduler.create_request(
            "high-3",
            priority=10
        )

        batch = scheduler.dequeue_batch(
            max_batch_size=3
        )

        self.assertEqual(
            [r.payload for r in batch],
            ["high-1", "high-2", "old-low"]
        )

    def test_terminal_request_cannot_be_completed_twice(self):

        scheduler = RequestScheduler()

        request = scheduler.create_request("x")

        scheduler.cancel_request(
            request.request_id
        )

        with self.assertRaises(ValidationError):
            scheduler.complete_request(
                request.request_id
            )


if __name__ == "__main__":
    unittest.main()
