"""Helper functions to validate payloads using Pydantic schemas."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional

from pydantic import BaseModel, HttpUrl, ValidationError, field_validator

from .schemas import (
    AdUpdate,
    CampaignCreate,
    CampaignUpdate,
    RegistrationCreate,
    RegistrationUpdate,
)


class AdCreate(BaseModel):
    ad_id: str
    title: str
    creative_url: Optional[HttpUrl] = None
    status: str = "active"
    tags: List[str] = []
    business_id: str
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    @field_validator("title")
    @classmethod
    def title_required(cls, value: str) -> str:
        value = (value or "").strip()
        if not value:
            raise ValueError("Title is required.")
        return value

    @field_validator("creative_url", mode="before")
    @classmethod
    def empty_url_to_none(cls, value):
        if value is None:
            return None
        if isinstance(value, str) and value.strip() == "":
            return None
        return value


class PayloadValidationError(ValueError):
    """Raised when payload validation fails."""


def _model_dump(model) -> Dict[str, Any]:
    return model.model_dump(exclude_none=True)


def validate_ad(data: Mapping[str, Any]) -> Dict[str, Any]:
    try:
        model = AdCreate.model_validate(dict(data))
    except ValidationError as exc:
        raise PayloadValidationError(str(exc)) from exc
    return model.dict()


def validate_ad_update(data: Mapping[str, Any]) -> Dict[str, Any]:
    try:
        model = AdUpdate.model_validate(dict(data))
    except ValidationError as exc:
        raise PayloadValidationError(str(exc)) from exc
    return _model_dump(model)


def validate_campaign(data: Mapping[str, Any]) -> Dict[str, Any]:
    try:
        model = CampaignCreate.model_validate(dict(data))
    except ValidationError as exc:
        raise PayloadValidationError(str(exc)) from exc
    return _model_dump(model)


def validate_campaign_update(data: Mapping[str, Any]) -> Dict[str, Any]:
    try:
        model = CampaignUpdate.model_validate(dict(data))
    except ValidationError as exc:
        raise PayloadValidationError(str(exc)) from exc
    return _model_dump(model)


def validate_registration(data: Mapping[str, Any]) -> Dict[str, Any]:
    try:
        model = RegistrationCreate.model_validate(dict(data))
    except ValidationError as exc:
        raise PayloadValidationError(str(exc)) from exc
    return _model_dump(model)


def validate_registration_update(data: Mapping[str, Any]) -> Dict[str, Any]:
    try:
        model = RegistrationUpdate.model_validate(dict(data))
    except ValidationError as exc:
        raise PayloadValidationError(str(exc)) from exc
    return _model_dump(model)
