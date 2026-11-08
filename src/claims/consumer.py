"""At-least-once consumer: poll -> process -> commit, one message at a time.

The offset is committed only after the message has been fully handled, meaning applied,
recognised as a duplicate, or published to the dead-letter topic. A crash anywhere before
that point redelivers the message, and the idempotent sink absorbs the repeat.
"""

import logging
import random
import signal
import threading
import time
from collections.abc import Callable
from typing import Protocol

import psycopg
from confluent_kafka import Consumer, KafkaException, Producer
from prometheus_client import start_http_server
from pydantic import ValidationError

from claims import metrics
from claims.config import Settings, load_settings
from claims.db import Outcome, Store
from claims.schemas import ClaimEvent, parse_event
from claims.state_machine import IllegalTransition
from claims.topics import ensure_topics

log = logging.getLogger("claims.consumer")


class Shutdown(Exception):
    """Raised mid-retry when a stop was requested; the message is left uncommitted."""


class Sink(Protocol):
    def apply(self, event: ClaimEvent) -> Outcome: ...


DeadLetter = Callable[[bytes, str, str, str], None]  # raw, reason, error, origin


def backoff_delay(attempt: int, base: float, cap: float, rand: Callable[[], float] = random.random):
    """Exponential backoff with 50-100% jitter. ``attempt`` starts at 1."""
    return min(cap, base * 2 ** (attempt - 1)) * (0.5 + rand() / 2)


class Processor:
    """Handles one message end to end. Returns normally only when it is safe to commit."""

    def __init__(
        self,
        sink: Sink,
        dead_letter: DeadLetter,
        stop: threading.Event,
        max_attempts: int = 5,
        backoff_base: float = 0.5,
        backoff_cap: float = 10.0,
    ):
        self.sink, self.dead_letter, self.stop = sink, dead_letter, stop
        self.max_attempts, self.backoff_base, self.backoff_cap = (
            max_attempts,
            backoff_base,
            backoff_cap,
        )

    def handle(self, raw: bytes, origin: str = "") -> None:
        started = time.perf_counter()
        metrics.CONSUMED.inc()
        try:
            self._handle(raw, origin)
        finally:
            metrics.LATENCY.observe(time.perf_counter() - started)

    def _dead(self, raw: bytes, reason: str, error: Exception, origin: str) -> None:
        self.dead_letter(raw, reason, str(error), origin)  # raises if the DLQ write fails
        metrics.DLQ.labels(reason).inc()

    def _handle(self, raw: bytes, origin: str) -> None:
        try:
            event = parse_event(raw)
        except ValidationError as exc:
            metrics.FAILED.labels("invalid_event").inc()
            return self._dead(raw, "invalid_event", exc, origin)

        attempt = 0
        while True:
            attempt += 1
            try:
                outcome = self.sink.apply(event)
            except IllegalTransition as exc:  # permanent: retrying cannot help
                metrics.FAILED.labels("illegal_transition").inc()
                return self._dead(raw, "illegal_transition", exc, origin)
            except Exception as exc:
                # Connection-level trouble is the database's problem, not the message's:
                # keep retrying (capped backoff) rather than dead-lettering a healthy event.
                # Anything else (bad data, constraint violation) is poison after max_attempts.
                infra = isinstance(exc, psycopg.OperationalError)
                metrics.FAILED.labels("infrastructure" if infra else "processing_error").inc()
                if not infra and attempt >= self.max_attempts:
                    return self._dead(raw, "retries_exhausted", exc, origin)
                delay = backoff_delay(attempt, self.backoff_base, self.backoff_cap)
                log.warning("attempt %d failed (%s); retrying in %.2fs", attempt, exc, delay)
                if self.stop.wait(delay):
                    raise Shutdown from exc
            else:
                (metrics.DUPLICATES if outcome == "duplicate" else metrics.APPLIED).inc()
                return


class DeadLetterPublisher:
    def __init__(self, bootstrap: str, topic: str):
        self._producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True})
        self._topic = topic

    def __call__(self, raw: bytes, reason: str, error: str, origin: str) -> None:
        errors: list = []
        self._producer.produce(
            self._topic,
            raw,
            headers={"dlq.reason": reason, "dlq.error": error[:500], "dlq.origin": origin},
            on_delivery=lambda err, _msg: err and errors.append(err),
        )
        if self._producer.flush(30) or errors:
            raise RuntimeError(f"DLQ publish failed: {errors or 'timeout'}")


class ClaimsConsumer:
    LAG_INTERVAL = 5.0

    def __init__(self, settings: Settings, processor: Processor):
        self.settings, self.processor = settings, processor
        self._next_lag = 0.0

    def run(self, stop: threading.Event) -> None:
        consumer = Consumer(
            {
                "bootstrap.servers": self.settings.bootstrap,
                "group.id": self.settings.group_id,
                "enable.auto.commit": False,  # commit manually, after the sink has the event
                "auto.offset.reset": "earliest",
            }
        )
        consumer.subscribe([self.settings.topic])
        try:
            while not stop.is_set():
                msg = consumer.poll(1.0)
                self._update_lag(consumer)
                if msg is None:
                    continue
                if msg.error():
                    log.error("consumer error: %s", msg.error())
                    continue
                origin = f"{msg.topic()}[{msg.partition()}]@{msg.offset()}"
                try:
                    self.processor.handle(msg.value() or b"", origin)
                except Shutdown:
                    break
                consumer.commit(message=msg, asynchronous=False)
        finally:
            consumer.close()  # leaves the group cleanly so partitions rebalance at once
            log.info("consumer closed")

    def _update_lag(self, consumer: Consumer) -> None:
        if time.monotonic() < self._next_lag:
            return
        self._next_lag = time.monotonic() + self.LAG_INTERVAL
        try:
            for tp in consumer.assignment():
                low, high = consumer.get_watermark_offsets(tp, timeout=2)
                committed = consumer.committed([tp], timeout=2)[0].offset
                position = committed if committed >= 0 else low
                metrics.LAG.labels(str(tp.partition)).set(max(high - position, 0))
        except KafkaException as exc:
            log.warning("lag refresh failed: %s", exc)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = load_settings()
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())

    ensure_topics(settings.bootstrap, [settings.topic, settings.dlq_topic])
    start_http_server(settings.metrics_port)
    store = Store(settings.database_url)
    processor = Processor(
        store,
        DeadLetterPublisher(settings.bootstrap, settings.dlq_topic),
        stop,
        settings.max_attempts,
        settings.backoff_base,
        settings.backoff_cap,
    )
    log.info("consuming %s, metrics on :%d", settings.topic, settings.metrics_port)
    try:
        ClaimsConsumer(settings, processor).run(stop)
    finally:
        store.close()


if __name__ == "__main__":
    main()
