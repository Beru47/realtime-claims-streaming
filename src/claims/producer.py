"""Synthetic claim event generator.

Produces realistic lifecycles, interleaved across claims, keyed by claim_id so every
claim's events stay ordered within one partition. Optionally injects duplicates and
bad events to exercise the idempotency and dead-letter paths.
"""

import argparse
import json
import random
import time
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime

from confluent_kafka import Producer

from claims.config import load_settings
from claims.topics import ensure_topics

CLAIM_TYPES = ["auto_collision", "home_water_damage", "theft", "medical", "travel"]


def _event(claim_id: str, event_type: str, payload: dict, version: int = 2) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "claim_id": claim_id,
        "event_type": event_type,
        "schema_version": version,
        "occurred_at": datetime.now(UTC).isoformat(),
        "payload": payload,
    }


def lifecycle(claim_id: str, rng: random.Random) -> list[dict]:
    """A valid event sequence that stops at a random point (some claims stay in flight)."""
    events = [
        _event(
            claim_id,
            "claim_opened",
            {
                "policy_id": f"POL-{rng.randint(10000, 99999)}",
                "claimant": f"claimant-{rng.randint(1, 500)}",
                "claim_type": rng.choice(CLAIM_TYPES),
            },
        )
    ]
    stop_at = rng.choices(["opened", "docs", "assessed", "paid"], weights=[1, 2, 2, 5])[0]
    if stop_at == "opened":
        return events
    for i in range(rng.randint(1, 3)):
        events.append(
            _event(
                claim_id,
                "document_uploaded",
                {
                    "document_type": rng.choice(["photo", "invoice", "report"]),
                    "filename": f"{i}.pdf",
                },
            )
        )
    if stop_at == "docs":
        return events
    amount = f"{rng.uniform(100, 9000):.2f}"
    approved = rng.random() > 0.15
    events.append(
        _event(
            claim_id,
            "claim_assessed",
            {
                "approved": approved,
                "assessed_amount": amount if approved else "0.00",
                "currency": "EUR",
            },
        )
    )
    if approved and stop_at == "paid":
        events.append(_event(claim_id, "claim_paid", {"paid_amount": amount, "currency": "EUR"}))
    return events


def generate(
    claims: int, seed: int = 1, invalid_rate: float = 0.0, duplicate_rate: float = 0.0
) -> Iterator[tuple[str, bytes]]:
    """Yield (key, value) pairs. Per-claim order is preserved; claims are interleaved."""
    rng = random.Random(seed)
    queues = [lifecycle(f"CLM-{seed}-{i:05d}", rng) for i in range(claims)]
    while queues:
        queue = rng.choice(queues)
        event = queue.pop(0)
        if not queue:
            queues.remove(queue)
        raw = json.dumps(event).encode()
        yield event["claim_id"], raw
        if rng.random() < duplicate_rate:
            yield event["claim_id"], raw  # redelivery: same event_id
        if rng.random() < invalid_rate:
            yield (
                event["claim_id"],
                rng.choice(
                    [
                        b"{not json",
                        json.dumps({**event, "event_id": "nope"}).encode(),
                        json.dumps({**event, "schema_version": 99}).encode(),
                        # valid event, illegal in any state: pay a claim that never existed
                        json.dumps(
                            _event(
                                f"GHOST-{rng.randint(1, 9999)}",
                                "claim_paid",
                                {"paid_amount": "10.00"},
                            )
                        ).encode(),
                    ]
                ),
            )


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--claims", type=int, default=25)
    ap.add_argument("--seed", type=int, default=int(time.time()))
    ap.add_argument("--invalid-rate", type=float, default=0.05)
    ap.add_argument("--duplicate-rate", type=float, default=0.1)
    ap.add_argument("--delay", type=float, default=0.0, help="seconds between messages")
    args = ap.parse_args()

    settings = load_settings()
    ensure_topics(settings.bootstrap, [settings.topic, settings.dlq_topic])
    producer = Producer({"bootstrap.servers": settings.bootstrap, "enable.idempotence": True})
    sent = 0
    for key, value in generate(args.claims, args.seed, args.invalid_rate, args.duplicate_rate):
        producer.produce(settings.topic, value, key=key)
        producer.poll(0)
        sent += 1
        if args.delay:
            time.sleep(args.delay)
    producer.flush()
    print(
        f"produced {sent} messages for {args.claims} claims to {settings.topic} (seed={args.seed})"
    )


if __name__ == "__main__":
    main()
