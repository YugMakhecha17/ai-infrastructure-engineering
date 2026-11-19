
"""Thread-safe in-memory request scheduling engine."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from enum import Enum
import heapq
import itertools
import threading
import time
from typing import Any


class RequestState(str, Enum):
    QUEUED = "queued"
    DISPATCHED = "dispatched"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    EXPIRED = "expired"


class SchedulingPolicy(str, Enum):
    FIFO = "fifo"
    PRIORITY = "priority"



class SchedulerError(Exception):
    pass


class ValidationError(SchedulerError, ValueError):
    pass


class QueueFullError(SchedulerError):
    pass


class RequestNotFoundError(SchedulerError, KeyError):
    pass



@dataclass(slots=True)
class Request:
    request_id: str
    payload: Any
    priority: int
    token_cost: int
    metadata: dict[str, Any]
    created_at: float
    expires_at: float | None
    sequence: int

    state: RequestState = RequestState.QUEUED

    completed_at: float | None = None
    cancelled_at: float | None = None
    expired_at: float | None = None

    @property
    def is_terminal(self) -> bool:
        return self.state in (
            RequestState.COMPLETED,
            RequestState.CANCELLED,
            RequestState.EXPIRED,
        )


@dataclass(frozen=True, slots=True)
class SchedulerStats:
    queued: int
    in_progress: int
    completed: int
    cancelled: int
    expired: int
    total_created: int


# -------------------- SCHEDULER --------------------

class RequestScheduler:

    def __init__(
        self,
        policy: SchedulingPolicy | str = SchedulingPolicy.FIFO,
        *,
        max_queue_size: int | None = None,
        max_batch_size: int = 1,
        token_budget: int | None = None,
        default_ttl: float | None = None,
        max_priority_streak: int | None = 5,
        clock=time.monotonic,
    ):

        try:
            self.policy = SchedulingPolicy(policy)
        except ValueError as exc:
            raise ValidationError(
                "Policy must be 'fifo' or 'priority'"
            ) from exc

        self._validate_optional_positive_int(
            max_queue_size, "max_queue_size"
        )
        self._validate_positive_int(
            max_batch_size, "max_batch_size"
        )
        self._validate_optional_positive_int(
            token_budget, "token_budget"
        )
        self._validate_optional_positive_number(
            default_ttl, "default_ttl"
        )
        self._validate_optional_positive_int(
            max_priority_streak, "max_priority_streak"
        )

        self.max_queue_size = max_queue_size
        self.max_batch_size = max_batch_size
        self.token_budget = token_budget
        self.default_ttl = default_ttl
        self.max_priority_streak = max_priority_streak

        self._clock = clock

        # Authoritative request registry.
        self._requests: dict[str, Request] = {}

        # FIFO queue.
        self._fifo = deque()

        # Priority heap:
        # (-priority, sequence, request_id)
        self._priority_heap = []

        # Unique sequence generator.
        self._sequence = itertools.count()

        # Thread safety.
        self._lock = threading.RLock()

        self._priority_streak = 0

        self._counts = {
            state: 0 for state in RequestState
        }

        self._total_created = 0

    # -------------------- VALIDATION --------------------

    @staticmethod
    def _validate_positive_int(value, name):

        if (
            isinstance(value, bool)
            or not isinstance(value, int)
            or value <= 0
        ):
            raise ValidationError(
                f"{name} must be a positive integer"
            )

    @staticmethod
    def _validate_optional_positive_int(value, name):

        if value is not None:
            RequestScheduler._validate_positive_int(
                value, name
            )

    @staticmethod
    def _validate_optional_positive_number(value, name):

        if value is not None and (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or value <= 0
        ):
            raise ValidationError(
                f"{name} must be a positive number"
            )


    def create_request(
        self,
        payload,
        *,
        priority=0,
        token_cost=1,
        metadata=None,
        request_id=None,
        ttl=None,
    ):

        if (
            isinstance(priority, bool)
            or not isinstance(priority, int)
        ):
            raise ValidationError(
                "priority must be an integer"
            )

        self._validate_positive_int(
            token_cost, "token_cost"
        )

        self._validate_optional_positive_number(
            ttl, "ttl"
        )

        if metadata is not None and not isinstance(metadata, dict):
            raise ValidationError(
                "metadata must be a dictionary"
            )

        if request_id is not None and (
            not isinstance(request_id, str)
            or not request_id.strip()
        ):
            raise ValidationError(
                "request_id must be a non-empty string"
            )

        with self._lock:

            self._expire_due_locked()

            if (
                self.max_queue_size is not None
                and self._counts[RequestState.QUEUED]
                >= self.max_queue_size
            ):
                raise QueueFullError(
                    "Queue capacity reached"
                )

            sequence = next(self._sequence)

            rid = request_id or f"req_{sequence:08d}"

            if rid in self._requests:
                raise ValidationError(
                    f"Duplicate request ID: {rid}"
                )

            now = self._clock()

            effective_ttl = (
                self.default_ttl if ttl is None else ttl
            )

            request = Request(
                request_id=rid,
                payload=payload,
                priority=priority,
                token_cost=token_cost,
                metadata=dict(metadata or {}),
                created_at=now,
                expires_at=(
                    now + effective_ttl
                    if effective_ttl is not None
                    else None
                ),
                sequence=sequence,
            )

            self._requests[rid] = request

            if self.policy is SchedulingPolicy.FIFO:

                self._fifo.append((sequence, rid))

            else:

                heapq.heappush(
                    self._priority_heap,
                    (-priority, sequence, rid),
                )

            self._counts[RequestState.QUEUED] += 1
            self._total_created += 1

            return request


    def get_request(self, request_id):

        with self._lock:

            request = self._requests.get(request_id)

            if request is None:
                raise RequestNotFoundError(request_id)

            return request

    # -------------------- REQUEST LIFECYCLE --------------------

    def _get_active_locked(self, request_id):

        request = self._requests.get(request_id)

        if request is None:
            raise RequestNotFoundError(request_id)

        if request.state not in (
            RequestState.QUEUED,
            RequestState.DISPATCHED,
        ):
            raise ValidationError(
                f"Request is already {request.state.value}"
            )

        if (
            request.state is RequestState.QUEUED
            and request.expires_at is not None
            and request.expires_at <= self._clock()
        ):
            self._expire_one_locked(request)

            raise ValidationError(
                f"Request {request_id} has expired"
            )

        return request

    def complete_request(self, request_id):

        with self._lock:

            request = self._get_active_locked(request_id)

            previous_state = request.state

            request.state = RequestState.COMPLETED
            request.completed_at = self._clock()

            self._counts[previous_state] -= 1
            self._counts[RequestState.COMPLETED] += 1

            return request

    def cancel_request(self, request_id):

        with self._lock:

            request = self._get_active_locked(request_id)

            previous_state = request.state

            request.state = RequestState.CANCELLED
            request.cancelled_at = self._clock()

            self._counts[previous_state] -= 1
            self._counts[RequestState.CANCELLED] += 1

            return request

    # -------------------- EXPIRATION --------------------

    def _expire_one_locked(self, request):

        if request.state is RequestState.QUEUED:

            request.state = RequestState.EXPIRED
            request.expired_at = self._clock()

            self._counts[RequestState.QUEUED] -= 1
            self._counts[RequestState.EXPIRED] += 1

    def _expire_due_locked(self):

        now = self._clock()

        for request in self._requests.values():

            if (
                request.state is RequestState.QUEUED
                and request.expires_at is not None
                and request.expires_at <= now
            ):
                self._expire_one_locked(request)


    def _remove_heap_entry_locked(self, request_id):

        self._priority_heap = [
            entry
            for entry in self._priority_heap
            if entry[2] != request_id
        ]

        heapq.heapify(self._priority_heap)

    # -------------------- SCHEDULING --------------------

    def _pop_next_locked(self):

        # FIFO scheduling.
        if self.policy is SchedulingPolicy.FIFO:

            while self._fifo:

                _, rid = self._fifo.popleft()

                request = self._requests[rid]

                if request.state is RequestState.QUEUED:
                    return request

            return None

        # Priority fairness.
        if (
            self.max_priority_streak is not None
            and self._priority_streak
            >= self.max_priority_streak
        ):

            queued = (
                request
                for request in self._requests.values()
                if request.state is RequestState.QUEUED
            )

            oldest = min(
                queued,
                key=lambda r: r.sequence,
                default=None,
            )

            if oldest is not None:

                self._remove_heap_entry_locked(
                    oldest.request_id
                )

                self._priority_streak = 0

                return oldest

        # Normal priority scheduling.
        while self._priority_heap:

            _, _, rid = heapq.heappop(
                self._priority_heap
            )

            request = self._requests[rid]

            if request.state is RequestState.QUEUED:

                self._priority_streak += 1

                return request

        return None


    def dequeue_batch(
        self,
        *,
        max_batch_size=None,
        token_budget=None,
    ):

        if max_batch_size is not None:
            self._validate_positive_int(
                max_batch_size, "max_batch_size"
            )

        if token_budget is not None:
            self._validate_positive_int(
                token_budget, "token_budget"
            )

        batch_limit = (
            max_batch_size or self.max_batch_size
        )

        budget = (
            self.token_budget
            if token_budget is None
            else token_budget
        )

        with self._lock:

            self._expire_due_locked()

            batch = []
            deferred = []
            used_tokens = 0

            while len(batch) < batch_limit:

                request = self._pop_next_locked()

                if request is None:
                    break

                if (
                    budget is not None
                    and used_tokens + request.token_cost > budget
                ):

                    deferred.append(request)
                    continue

                # Reserve request for a worker.
                request.state = RequestState.DISPATCHED

                self._counts[RequestState.QUEUED] -= 1
                self._counts[RequestState.DISPATCHED] += 1

                batch.append(request)
                used_tokens += request.token_cost

            # Restore skipped requests.
            for request in reversed(deferred):

                if self.policy is SchedulingPolicy.FIFO:

                    self._fifo.appendleft(
                        (request.sequence, request.request_id)
                    )

                else:

                    heapq.heappush(
                        self._priority_heap,
                        (
                            -request.priority,
                            request.sequence,
                            request.request_id,
                        ),
                    )

            return batch


    def pending_requests(self):

        with self._lock:

            self._expire_due_locked()

            return sorted(
                (
                    request
                    for request in self._requests.values()
                    if request.state is RequestState.QUEUED
                ),
                key=lambda r: r.sequence,
            )


    def stats(self):

        with self._lock:

            self._expire_due_locked()

            return SchedulerStats(
                queued=self._counts[RequestState.QUEUED],
                in_progress=self._counts[RequestState.DISPATCHED],
                completed=self._counts[RequestState.COMPLETED],
                cancelled=self._counts[RequestState.CANCELLED],
                expired=self._counts[RequestState.EXPIRED],
                total_created=self._total_created,
            )
