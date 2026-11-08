import json
import threading

import psycopg
import pytest

from claims import metrics
from claims.consumer import Processor, Shutdown, backoff_delay
from claims.producer import generate
from claims.schemas import ClaimEvent, parse_event
from claims.state_machine import next_status
from tests.unit.conftest import raw_event


class MemorySink:
    """Same contract as db.Store: dedup on event_id, then guarded transition."""

    def __init__(self):
        self.seen: set = set()
        self.status: dict = {}

    def apply(self, event: ClaimEvent):
        if event.event_id in self.seen:
            return "duplicate"
        new = next_status(self.status.get(event.claim_id), event)  # raises before we record
        self.seen.add(event.event_id)
        self.status[event.claim_id] = new
        return "applied"


class Flaky:
    def __init__(self, errors):
        self.errors, self.calls = list(errors), 0

    def apply(self, event):
        self.calls += 1
        if self.errors:
            raise self.errors.pop(0)
        return "applied"


def make(sink, stop=None, **kw):
    dead = []
    p = Processor(
        sink,
        lambda raw, reason, err, origin: dead.append(reason),
        stop or threading.Event(),
        backoff_base=0,
        backoff_cap=0,
        **kw,
    )
    return p, dead


def test_replaying_a_stream_is_idempotent():
    stream = [v for _, v in generate(30, seed=7, duplicate_rate=0.3)]
    sink = MemorySink()
    p, dead = make(sink)
    for raw in stream:
        p.handle(raw)
    once = dict(sink.status)
    for raw in stream:  # full redelivery, as after a crash before commit
        p.handle(raw)
    assert sink.status == once
    assert dead == []


def test_duplicate_is_counted_not_applied():
    raw = json.dumps(raw_event()).encode()
    p, _ = make(MemorySink())
    before = metrics.DUPLICATES._value.get()
    p.handle(raw)
    p.handle(raw)
    assert metrics.DUPLICATES._value.get() == before + 1


def test_invalid_and_illegal_events_go_to_dlq():
    p, dead = make(MemorySink())
    p.handle(b"{not json")
    p.handle(json.dumps(raw_event("claim_paid", claim_id="ghost")).encode())
    assert dead == ["invalid_event", "illegal_transition"]


def test_transient_errors_are_retried_then_applied():
    sink = Flaky([ValueError("x"), ValueError("y")])
    p, dead = make(sink, max_attempts=5)
    p.handle(json.dumps(raw_event()).encode())
    assert sink.calls == 3
    assert dead == []


def test_poison_message_is_dead_lettered_after_max_attempts():
    sink = Flaky([ValueError("boom")] * 10)
    p, dead = make(sink, max_attempts=3)
    p.handle(json.dumps(raw_event()).encode())
    assert sink.calls == 3
    assert dead == ["retries_exhausted"]


def test_database_outage_is_retried_beyond_max_attempts():
    sink = Flaky([psycopg.OperationalError("down")] * 6)
    p, dead = make(sink, max_attempts=3)
    p.handle(json.dumps(raw_event()).encode())
    assert sink.calls == 7
    assert dead == []


def test_shutdown_during_retry_leaves_message_uncommitted():
    stop = threading.Event()
    stop.set()
    p, dead = make(Flaky([psycopg.OperationalError("down")]), stop=stop)
    with pytest.raises(Shutdown):
        p.handle(json.dumps(raw_event()).encode())
    assert dead == []


def test_backoff_grows_and_is_capped():
    def full():
        return 1.0

    assert [backoff_delay(n, 0.5, 4, full) for n in (1, 2, 3, 4, 5)] == [0.5, 1, 2, 4, 4]
    assert backoff_delay(1, 1, 10, lambda: 0.0) == 0.5


def test_generated_events_are_valid_and_ordered_per_claim():
    seen: dict = {}
    for key, raw in generate(20, seed=3):
        e = parse_event(raw)
        assert e.claim_id == key
        seen.setdefault(key, []).append(e.event_type.value)
    assert all(types[0] == "claim_opened" for types in seen.values())
