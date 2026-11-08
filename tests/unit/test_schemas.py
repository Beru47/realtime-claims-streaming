import json

import pytest
from pydantic import ValidationError

from claims.schemas import CURRENT_VERSION, parse_event
from tests.unit.conftest import raw_event


def test_valid_event_parses():
    e = parse_event(json.dumps(raw_event("claim_assessed")))
    assert e.payload["assessed_amount"] == "100.00"


def test_v1_is_upcast_with_default_currency():
    e = parse_event(json.dumps(raw_event("claim_paid", schema_version=1)))
    assert e.schema_version == CURRENT_VERSION
    assert e.payload["currency"] == "EUR"


@pytest.mark.parametrize(
    "mutation",
    [
        {"schema_version": 99},
        {"event_id": "not-a-uuid"},
        {"occurred_at": "2026-01-01T10:00:00"},  # naive timestamp
        {"claim_id": ""},
        {"event_type": "claim_teleported"},
        {"unexpected": 1},
    ],
)
def test_contract_violations_rejected(mutation):
    with pytest.raises(ValidationError):
        parse_event(json.dumps(raw_event(**mutation)))


@pytest.mark.parametrize(
    ("event_type", "payload"),
    [
        ("claim_paid", {"paid_amount": "-5"}),
        ("claim_paid", {"paid_amount": "10", "currency": "euro"}),
        ("claim_assessed", {"approved": True}),
        ("claim_opened", {"policy_id": "P1", "claimant": "C1", "claim_type": "x", "extra": 1}),
    ],
)
def test_bad_payloads_rejected(event_type, payload):
    with pytest.raises(ValidationError):
        parse_event(json.dumps(raw_event(event_type, payload=payload)))


def test_garbage_is_a_validation_error():
    with pytest.raises(ValidationError):
        parse_event(b"{not json")
