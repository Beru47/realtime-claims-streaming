import pytest

from claims.state_machine import IllegalTransition, Status, next_status
from tests.unit.conftest import event

HAPPY = [
    (None, "claim_opened", Status.OPENED),
    (Status.OPENED, "document_uploaded", Status.DOCUMENTS_RECEIVED),
    (Status.DOCUMENTS_RECEIVED, "document_uploaded", Status.DOCUMENTS_RECEIVED),
    (Status.DOCUMENTS_RECEIVED, "claim_assessed", Status.ASSESSED),
    (Status.ASSESSED, "claim_paid", Status.PAID),
]


@pytest.mark.parametrize(("current", "event_type", "expected"), HAPPY)
def test_legal_transitions(current, event_type, expected):
    assert next_status(current, event(event_type)) is expected


def test_rejected_assessment_is_terminal():
    rejected = event("claim_assessed", payload={"approved": False, "assessed_amount": "0"})
    assert next_status(Status.DOCUMENTS_RECEIVED, rejected) is Status.REJECTED
    for t in ("document_uploaded", "claim_assessed", "claim_paid"):
        with pytest.raises(IllegalTransition):
            next_status(Status.REJECTED, event(t))


def test_every_other_combination_is_illegal():
    legal = {(c, t) for c, t, _ in HAPPY}
    types = ["claim_opened", "document_uploaded", "claim_assessed", "claim_paid"]
    for current in [None, *Status]:
        for t in types:
            if (current, t) in legal:
                continue
            with pytest.raises(IllegalTransition):
                next_status(current, event(t))


def test_cannot_pay_before_assessment_or_twice():
    with pytest.raises(IllegalTransition):
        next_status(Status.DOCUMENTS_RECEIVED, event("claim_paid"))
    with pytest.raises(IllegalTransition):
        next_status(Status.PAID, event("claim_paid"))
