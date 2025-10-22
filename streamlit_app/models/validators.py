"""Helper functions to validate payloads using Pydantic schemas."""

from __future__ import annotations

from typing import Any, Dict, Mapping

from pydantic import ValidationError

from .schemas import (
    AdCreate,
    AdUpdate,
    CampaignCreate,
    CampaignUpdate,
    RegistrationCreate,
    RegistrationUpdate,
)


class PayloadValidationError(ValueError):
    """Raised when payload validation fails."""


def _model_dump(model) -> Dict[str, Any]:
    return model.model_dump(exclude_none=True)


def validate_ad(data: Mapping[str, Any]) -> Dict[str, Any]:
    try:
        model = AdCreate.model_validate(dict(data))
    except ValidationError as exc:
        raise PayloadValidationError(str(exc)) from exc
    return _model_dump(model)


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
