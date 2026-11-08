"""End-to-end tests against a real Kafka-compatible broker and PostgreSQL.

Run with ``make up && make test-integration`` (or see .github/workflows/ci.yml).
"""

import json
import os
import threading
import time
import uuid

import psycopg
import pytest
from confluent_kafka import Consumer, Producer, TopicPartition
from fastapi.testclient import TestClient

from claims.config import Settings
from claims.consumer import ClaimsConsumer, DeadLetterPublisher, Processor
from claims.db import Store, init_schema
from claims.producer import generate
from claims.topics import ensure_topics
from tests.unit.conftest import raw_event

pytestmark = pytest.mark.integration

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
DSN = os.environ.get("DATABASE_URL", "postgresql://claims:claims@localhost:5432/claims")


def wait_for(predicate, timeout=60.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.25)
    raise AssertionError("condition not met in time")


def query(sql, params=()):
    with psycopg.connect(DSN, autocommit=True) as conn:
        return conn.execute(sql, params).fetchall()


class Pipeline:
    def __init__(self):
        suffix = uuid.uuid4().hex[:8]
        self.settings = Settings(BOOTSTRAP, DSN, f"it.{suffix}", f"it.{suffix}.dlq",
                                 f"it-{suffix}", 0, 3, 0.05, 0.2)  # fmt: skip
        ensure_topics(BOOTSTRAP, [self.settings.topic, self.settings.dlq_topic], partitions=1)
        self.producer = Producer({"bootstrap.servers": BOOTSTRAP})
        self._workers: list[tuple[threading.Event, threading.Thread]] = []

    def send(self, *messages: bytes | dict) -> None:
        for m in messages:
            raw = m if isinstance(m, bytes) else json.dumps(m).encode()
            self.producer.produce(self.settings.topic, raw, key=b"k")
        self.producer.flush(30)

    def start_consumer(self, group_id: str | None = None) -> None:
        s = self.settings if group_id is None else Settings(**{**self.settings.__dict__,
                                                                "group_id": group_id})  # fmt: skip
        stop = threading.Event()
        processor = Processor(Store(DSN), DeadLetterPublisher(BOOTSTRAP, s.dlq_topic), stop,
                              s.max_attempts, s.backoff_base, s.backoff_cap)  # fmt: skip
        thread = threading.Thread(target=ClaimsConsumer(s, processor).run, args=(stop,))
        thread.start()
        self._workers.append((stop, thread))

    def stop_all(self) -> None:
        for stop, thread in self._workers:
            stop.set()
            thread.join(30)
            assert not thread.is_alive(), "consumer did not shut down gracefully"
        self._workers.clear()

    def dlq(self, expected: int) -> list:
        c = Consumer(
            {
                "bootstrap.servers": BOOTSTRAP,
                "group.id": f"dlq-{uuid.uuid4().hex}",
                "auto.offset.reset": "earliest",
            }
        )
        c.subscribe([self.settings.dlq_topic])
        found: list = []

        def poll_until_enough() -> bool:
            m = c.poll(0.5)
            if m is not None and not m.error():
                found.append(m)
            return len(found) >= expected

        try:
            wait_for(poll_until_enough)
        finally:
            c.close()
        return found

    def committed(self, group_id: str | None = None) -> int:
        c = Consumer(
            {"bootstrap.servers": BOOTSTRAP, "group.id": group_id or self.settings.group_id}
        )
        try:
            return c.committed([TopicPartition(self.settings.topic, 0)], timeout=10)[0].offset
        finally:
            c.close()


@pytest.fixture
def pipeline(monkeypatch):
    with psycopg.connect(DSN, autocommit=True) as conn:
        init_schema(conn)
        conn.execute("TRUNCATE processed_events, claim_status")
    monkeypatch.setenv("DATABASE_URL", DSN)
    p = Pipeline()
    p.start_consumer()
    yield p
    p.stop_all()


def lifecycle(claim_id):
    return [raw_event(t, claim_id=claim_id) for t in
            ("claim_opened", "document_uploaded", "claim_assessed", "claim_paid")]  # fmt: skip


def test_lifecycle_is_materialised_and_served_by_the_api(pipeline):
    pipeline.send(*lifecycle("CLM-A"), *lifecycle("CLM-B")[:2])
    wait_for(lambda: pipeline.committed() == 6)

    from claims.api import app

    with TestClient(app) as api:
        assert api.get("/health").json() == {"status": "ok"}
        a = api.get("/claims/CLM-A").json()
        assert (a["status"], a["paid_amount"], a["event_count"]) == ("PAID", "100.00", 4)
        assert api.get("/claims/CLM-B").json()["status"] == "DOCUMENTS_RECEIVED"
        assert api.get("/claims/nope").status_code == 404
        listed = api.get("/claims", params={"status": "PAID"}).json()
        assert [c["claim_id"] for c in listed["items"]] == ["CLM-A"]
        assert api.get("/claims", params={"status": "bogus"}).status_code == 422


def test_duplicates_are_applied_once(pipeline):
    events = lifecycle("CLM-D")[:2]
    pipeline.send(events[0], events[0], events[1], events[0], events[1])
    wait_for(lambda: pipeline.committed() == 5)
    assert query("SELECT event_count, status FROM claim_status WHERE claim_id = 'CLM-D'") == [
        (2, "DOCUMENTS_RECEIVED")
    ]
    assert query("SELECT count(*) FROM processed_events")[0][0] == 2


def test_bad_events_are_dead_lettered_and_do_not_block_the_stream(pipeline):
    ghost_payment = raw_event("claim_paid", claim_id="GHOST")
    pipeline.send(b"{not json", ghost_payment, *lifecycle("CLM-OK"))
    wait_for(
        lambda: query("SELECT status FROM claim_status WHERE claim_id = 'CLM-OK'") == [("PAID",)]
    )

    dead = pipeline.dlq(2)
    reasons = sorted(dict(m.headers())["dlq.reason"].decode() for m in dead)
    assert reasons == ["illegal_transition", "invalid_event"]
    assert query("SELECT count(*) FROM claim_status WHERE claim_id = 'GHOST'")[0][0] == 0
    wait_for(lambda: pipeline.committed() == 6)  # every message, bad ones included, is committed


def test_replay_from_the_beginning_changes_nothing(pipeline):
    total = 0
    for _, raw in generate(15, seed=11, duplicate_rate=0.2):
        pipeline.send(raw)
        total += 1
    wait_for(lambda: pipeline.committed() == total)
    pipeline.stop_all()
    sql = "SELECT claim_id, status, event_count, last_event_id FROM claim_status ORDER BY 1"
    before = query(sql)
    assert before

    replay = f"replay-{uuid.uuid4().hex[:6]}"  # a new group re-reads the whole topic
    pipeline.start_consumer(group_id=replay)
    wait_for(lambda: pipeline.committed(replay) == total)
    assert query(sql) == before
