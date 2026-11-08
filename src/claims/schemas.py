"""Versioned event contract.

Every message is a ``ClaimEvent`` envelope. The ``payload`` is validated against a
model chosen by ``event_type``. Older schema versions are upcast to the latest one
at the edge, so the rest of the code only ever sees ``CURRENT_VERSION``.

    v1: original payloads
    v2: adds ``currency`` (ISO-4217 code) to money-carrying payloads; v1 implies EUR
"""

from decimal import Decimal
from enum import StrEnum
from typing import Any
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

CURRENT_VERSION = 2
SUPPORTED_VERSIONS = {1, 2}


class EventType(StrEnum):
    CLAIM_OPENED = "claim_opened"
    DOCUMENT_UPLOADED = "document_uploaded"
    CLAIM_ASSESSED = "claim_assessed"
    CLAIM_PAID = "claim_paid"


class _Payload(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ClaimOpened(_Payload):
    policy_id: str = Field(min_length=1)
    claimant: str = Field(min_length=1)
    claim_type: str = Field(min_length=1)


class DocumentUploaded(_Payload):
    document_type: str = Field(min_length=1)
    filename: str = Field(min_length=1)


class ClaimAssessed(_Payload):
    approved: bool
    assessed_amount: Decimal = Field(ge=0, max_digits=12, decimal_places=2)
    currency: str = Field(default="EUR", pattern=r"^[A-Z]{3}$")


class ClaimPaid(_Payload):
    paid_amount: Decimal = Field(gt=0, max_digits=12, decimal_places=2)
    currency: str = Field(default="EUR", pattern=r"^[A-Z]{3}$")


PAYLOADS: dict[EventType, type[_Payload]] = {
    EventType.CLAIM_OPENED: ClaimOpened,
    EventType.DOCUMENT_UPLOADED: DocumentUploaded,
    EventType.CLAIM_ASSESSED: ClaimAssessed,
    EventType.CLAIM_PAID: ClaimPaid,
}


class ClaimEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    claim_id: str = Field(min_length=1, max_length=64)
    event_type: EventType
    schema_version: int
    occurred_at: AwareDatetime
    payload: dict[str, Any]

    @model_validator(mode="after")
    def _validate_and_upcast(self) -> "ClaimEvent":
        if self.schema_version not in SUPPORTED_VERSIONS:
            raise ValueError(f"unsupported schema_version {self.schema_version}")
        model = PAYLOADS[self.event_type].model_validate(self.payload)
        self.payload = model.model_dump(mode="json")
        self.schema_version = CURRENT_VERSION  # v1 -> v2: defaults fill in currency=EUR
        return self


def parse_event(raw: bytes | str) -> ClaimEvent:
    """Parse a raw message. Raises pydantic.ValidationError on any contract violation."""
    return ClaimEvent.model_validate_json(raw)
