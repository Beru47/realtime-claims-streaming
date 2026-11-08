"""PostgreSQL sink: idempotent, state-machine-guarded upserts."""

from typing import Literal

import psycopg
from psycopg.rows import dict_row

from claims.schemas import ClaimEvent, EventType
from claims.state_machine import Status, next_status

SCHEMA = """
CREATE TABLE IF NOT EXISTS processed_events (
    event_id     UUID PRIMARY KEY,
    claim_id     TEXT NOT NULL,
    event_type   TEXT NOT NULL,
    occurred_at  TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS claim_status (
    claim_id        TEXT PRIMARY KEY,
    status          TEXT NOT NULL,
    policy_id       TEXT,
    claimant        TEXT,
    claim_type      TEXT,
    assessed_amount NUMERIC(14, 2),
    paid_amount     NUMERIC(14, 2),
    currency        TEXT,
    event_count     INTEGER NOT NULL DEFAULT 0,
    last_event_id   UUID NOT NULL,
    opened_at       TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS claim_status_status_idx ON claim_status (status);
"""

Outcome = Literal["applied", "duplicate"]


def init_schema(conn: psycopg.Connection) -> None:
    with conn.transaction():
        conn.execute("SELECT pg_advisory_xact_lock(47)")  # serialize concurrent first starts
        conn.execute(SCHEMA)


class Store:
    """One connection, reconnected lazily after a failure (the caller's retry drives it)."""

    def __init__(self, dsn: str):
        self._dsn = dsn
        self._conn: psycopg.Connection | None = None

    def _connection(self) -> psycopg.Connection:
        if self._conn is None or self._conn.closed:
            self._conn = psycopg.connect(self._dsn, autocommit=True, row_factory=dict_row)
            init_schema(self._conn)
        return self._conn

    def apply(self, event: ClaimEvent) -> Outcome:
        """Atomically: dedup on event_id, validate the transition, upsert claim_status.

        Raises IllegalTransition (permanent) or psycopg errors (transient). Any exception
        rolls back the whole transaction, including the dedup row, so a failed event can
        be retried or replayed later.
        """
        try:
            return self._apply(self._connection(), event)
        except psycopg.OperationalError:
            self.close()
            raise

    @staticmethod
    def _apply(conn: psycopg.Connection, event: ClaimEvent) -> Outcome:
        with conn.transaction():
            inserted = conn.execute(
                """INSERT INTO processed_events (event_id, claim_id, event_type, occurred_at)
                   VALUES (%s, %s, %s, %s) ON CONFLICT (event_id) DO NOTHING RETURNING 1""",
                (event.event_id, event.claim_id, event.event_type.value, event.occurred_at),
            ).fetchone()
            if inserted is None:
                return "duplicate"

            row = conn.execute(
                "SELECT status FROM claim_status WHERE claim_id = %s FOR UPDATE",
                (event.claim_id,),
            ).fetchone()
            new = next_status(Status(row["status"]) if row else None, event)
            p = event.payload

            if event.event_type is EventType.CLAIM_OPENED:
                conn.execute(
                    """INSERT INTO claim_status (claim_id, status, policy_id, claimant, claim_type,
                           event_count, last_event_id, opened_at, updated_at)
                       VALUES (%s, %s, %s, %s, %s, 1, %s, %s, %s)""",
                    (
                        event.claim_id,
                        new.value,
                        p["policy_id"],
                        p["claimant"],
                        p["claim_type"],
                        event.event_id,
                        event.occurred_at,
                        event.occurred_at,
                    ),
                )
            else:
                conn.execute(
                    """UPDATE claim_status SET status = %s,
                           assessed_amount = COALESCE(%s, assessed_amount),
                           paid_amount = COALESCE(%s, paid_amount),
                           currency = COALESCE(%s, currency),
                           event_count = event_count + 1,
                           last_event_id = %s, updated_at = %s
                       WHERE claim_id = %s""",
                    (
                        new.value,
                        p.get("assessed_amount"),
                        p.get("paid_amount"),
                        p.get("currency"),
                        event.event_id,
                        event.occurred_at,
                        event.claim_id,
                    ),
                )
            return "applied"

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
