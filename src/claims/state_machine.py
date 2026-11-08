"""Claim lifecycle. Pure functions, no I/O.

(none) --claim_opened--> OPENED
OPENED / DOCUMENTS_RECEIVED --document_uploaded--> DOCUMENTS_RECEIVED
DOCUMENTS_RECEIVED --claim_assessed (approved)--> ASSESSED
DOCUMENTS_RECEIVED --claim_assessed (rejected)--> REJECTED   (terminal)
ASSESSED --claim_paid--> PAID                                  (terminal)
"""

from enum import StrEnum

from claims.schemas import ClaimEvent, EventType


class Status(StrEnum):
    OPENED = "OPENED"
    DOCUMENTS_RECEIVED = "DOCUMENTS_RECEIVED"
    ASSESSED = "ASSESSED"
    REJECTED = "REJECTED"
    PAID = "PAID"


class IllegalTransition(Exception):
    """The event is valid but not allowed in the claim's current state."""


_ALLOWED: dict[EventType, dict[Status | None, Status]] = {
    EventType.CLAIM_OPENED: {None: Status.OPENED},
    EventType.DOCUMENT_UPLOADED: {
        Status.OPENED: Status.DOCUMENTS_RECEIVED,
        Status.DOCUMENTS_RECEIVED: Status.DOCUMENTS_RECEIVED,
    },
    EventType.CLAIM_ASSESSED: {Status.DOCUMENTS_RECEIVED: Status.ASSESSED},
    EventType.CLAIM_PAID: {Status.ASSESSED: Status.PAID},
}


def next_status(current: Status | None, event: ClaimEvent) -> Status:
    target = _ALLOWED[event.event_type].get(current)
    if target is None:
        raise IllegalTransition(f"{event.event_type.value} not allowed from {current}")
    if event.event_type is EventType.CLAIM_ASSESSED and not event.payload["approved"]:
        return Status.REJECTED
    return target
