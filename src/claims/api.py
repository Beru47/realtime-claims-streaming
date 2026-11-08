"""Read API over the materialized claim_status table."""

from collections.abc import Iterator
from contextlib import asynccontextmanager
from typing import Any

import psycopg
from fastapi import Depends, FastAPI, HTTPException, Query
from prometheus_client import make_asgi_app
from psycopg.rows import dict_row

from claims.config import load_settings
from claims.db import init_schema
from claims.state_machine import Status


@asynccontextmanager
async def lifespan(_: FastAPI):
    with psycopg.connect(load_settings().database_url) as conn:
        init_schema(conn)
    yield


app = FastAPI(title="Claims read API", version="0.1.0", lifespan=lifespan)
app.mount("/metrics", make_asgi_app())


def get_conn() -> Iterator[psycopg.Connection]:
    # ponytail: connection per request; add psycopg_pool if request rate ever matters
    with psycopg.connect(load_settings().database_url, row_factory=dict_row) as conn:
        yield conn


def _serialise(row: dict[str, Any]) -> dict[str, Any]:
    return {k: (str(v) if k.endswith("_amount") and v is not None else v) for k, v in row.items()}


@app.get("/health")
def health(conn: psycopg.Connection = Depends(get_conn)):
    try:
        conn.execute("SELECT 1")
    except psycopg.Error as exc:
        raise HTTPException(503, "database unavailable") from exc
    return {"status": "ok"}


@app.get("/claims/{claim_id}")
def get_claim(claim_id: str, conn: psycopg.Connection = Depends(get_conn)):
    row = conn.execute("SELECT * FROM claim_status WHERE claim_id = %s", (claim_id,)).fetchone()
    if row is None:
        raise HTTPException(404, "claim not found")
    return _serialise(row)


@app.get("/claims")
def list_claims(
    status: Status | None = None,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    conn: psycopg.Connection = Depends(get_conn),
):
    rows = conn.execute(
        """SELECT * FROM claim_status WHERE (%(s)s::text IS NULL OR status = %(s)s)
           ORDER BY updated_at DESC, claim_id LIMIT %(l)s OFFSET %(o)s""",
        {"s": status.value if status else None, "l": limit, "o": offset},
    ).fetchall()
    return {"count": len(rows), "items": [_serialise(r) for r in rows]}
