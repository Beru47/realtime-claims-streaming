import uuid
from datetime import UTC, datetime

from claims.schemas import ClaimEvent

PAYLOADS = {
    "claim_opened": {"policy_id": "P1", "claimant": "C1", "claim_type": "theft"},
    "document_uploaded": {"document_type": "photo", "filename": "a.pdf"},
    "claim_assessed": {"approved": True, "assessed_amount": "100.00"},
    "claim_paid": {"paid_amount": "100.00"},
}


def raw_event(event_type="claim_opened", claim_id="CLM-1", **over) -> dict:
    base = {
        "event_id": str(uuid.uuid4()),
        "claim_id": claim_id,
        "event_type": event_type,
        "schema_version": 2,
        "occurred_at": datetime.now(UTC).isoformat(),
        "payload": dict(PAYLOADS.get(event_type, {})),
    }
    return {**base, **over}


def event(event_type="claim_opened", **over) -> ClaimEvent:
    return ClaimEvent.model_validate(raw_event(event_type, **over))
