"""Pydantic schemas for MongoDB documents."""

from __future__ import annotations

from datetime import date, datetime
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, ValidationInfo, field_validator, model_validator

from streamlit_app.utils.constants import AD_STATUSES, CAMPAIGN_STATUSES


class TargetingModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    locations: List[str] = Field(default_factory=list)
    interests: List[str] = Field(default_factory=list)
    devices: List[str] = Field(default_factory=list)
    budget_daily: Optional[float] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None

    @model_validator(mode="after")
    def validate_dates(self) -> "TargetingModel":
        if self.start_date and self.end_date and self.start_date > self.end_date:
            raise ValueError("End date must be after start date.")
        return self

    @field_validator("budget_daily")
    @classmethod
    def non_negative_budget(cls, value: Optional[float]) -> Optional[float]:
        if value is not None and value < 0:
            raise ValueError("Budget must be positive.")
        return value


class AdBase(BaseModel):
    model_config = ConfigDict(extra="forbid")

    title: str
    status: str = Field(default="active")
    creative_url: Optional[HttpUrl] = None
    tags: List[str] = Field(default_factory=list)

    @field_validator("title")
    @classmethod
    def title_not_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("Title is required.")
        return value.strip()

    @field_validator("status")
    @classmethod
    def validate_status(cls, value: str) -> str:
        if value not in AD_STATUSES:
            raise ValueError(f"Status must be one of {AD_STATUSES}.")
        return value

    @field_validator("tags", mode="before")
    @classmethod
    def normalize_tags(cls, value):
        if value is None:
            return []
        return [str(tag).strip() for tag in value if str(tag).strip()]


class AdCreate(AdBase):
    ad_id: Optional[str] = None
    business_id: str

    @field_validator("business_id")
    @classmethod
    def business_id_not_blank(cls, value: str) -> str:
        if not value or not value.strip():
            raise ValueError("business_id is required.")
        return value.strip()


class AdUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    title: Optional[str] = None
    status: Optional[str] = None
    creative_url: Optional[HttpUrl] = None
    tags: Optional[List[str]] = None

    @field_validator("status")
    @classmethod
    def validate_status(cls, value: Optional[str]) -> Optional[str]:
        if value is not None and value not in AD_STATUSES:
            raise ValueError(f"Status must be one of {AD_STATUSES}.")
        return value


class CampaignBase(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    status: str = Field(default="draft")
    ad_ids: List[str] = Field(default_factory=list)
    targeting: TargetingModel = Field(default_factory=TargetingModel)
    business_type: str = Field(default="wedding_decor")

    @field_validator("name")
    @classmethod
    def name_not_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("Campaign name is required.")
        return value.strip()

    @field_validator("status")
    @classmethod
    def validate_status(cls, value: str) -> str:
        if value not in CAMPAIGN_STATUSES:
            raise ValueError(f"Status must be one of {CAMPAIGN_STATUSES}.")
        return value

    @field_validator("business_type")
    @classmethod
    def validate_business_type(cls, value: str) -> str:
        if not value:
            raise ValueError("business_type is required.")
        return value.strip()


class CampaignCreate(CampaignBase):
    campaign_id: Optional[str] = None
    business_id: str

    @field_validator("business_id")
    @classmethod
    def business_id_not_blank(cls, value: str) -> str:
        if not value or not value.strip():
            raise ValueError("business_id is required.")
        return value.strip()


class CampaignUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: Optional[str] = None
    status: Optional[str] = None
    ad_ids: Optional[List[str]] = None
    targeting: Optional[TargetingModel] = None
    business_type: Optional[str] = None

    @field_validator("status")
    @classmethod
    def validate_status(cls, value: Optional[str]) -> Optional[str]:
        if value is not None and value not in CAMPAIGN_STATUSES:
            raise ValueError(f"Status must be one of {CAMPAIGN_STATUSES}.")
        return value

    @field_validator("business_type")
    @classmethod
    def validate_business_type(cls, value: Optional[str]) -> Optional[str]:
        if value is not None and not value.strip():
            raise ValueError("business_type cannot be blank.")
        return value.strip() if value else value


class RegistrationBase(BaseModel):
    model_config = ConfigDict(extra="forbid")

    campaign_id: str
    ad_id: Optional[str] = None
    user_id: Optional[str] = None
    source: str
    cost: float = 0.0
    timestamp: datetime
    meta: dict = Field(default_factory=dict)
    messages: Optional[int] = None
    spent: Optional[float] = None
    reach: Optional[int] = None
    impressions: Optional[int] = None
    clicks: Optional[int] = None

    @field_validator("campaign_id", mode="before")
    @classmethod
    def normalize_campaign_id(cls, value: str) -> str:
        if not value:
            raise ValueError("campaign_id is required.")
        return value.strip()

    @field_validator("cost")
    @classmethod
    def positive_cost(cls, value: float) -> float:
        if value < 0:
            raise ValueError("cost must be non-negative.")
        return value

    @field_validator("messages", "reach", "impressions", "clicks")
    @classmethod
    def non_negative_int(cls, value: Optional[int], info: ValidationInfo):
        if value is not None and value < 0:
            raise ValueError(f"{info.field_name} must be non-negative.")
        return value

    @field_validator("spent")
    @classmethod
    def non_negative_float(cls, value: Optional[float]) -> Optional[float]:
        if value is not None and value < 0:
            raise ValueError("spent must be non-negative.")
        return value


class RegistrationCreate(RegistrationBase):
    registration_id: Optional[str] = None
    business_id: str

    @field_validator("business_id")
    @classmethod
    def business_id_not_blank(cls, value: str) -> str:
        if not value or not value.strip():
            raise ValueError("business_id is required.")
        return value.strip()


class RegistrationUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ad_id: Optional[str] = None
    user_id: Optional[str] = None
    source: Optional[str] = None
    cost: Optional[float] = None
    timestamp: Optional[datetime] = None
    meta: Optional[dict] = None
    messages: Optional[int] = None
    spent: Optional[float] = None
    reach: Optional[int] = None
    impressions: Optional[int] = None
    clicks: Optional[int] = None

    @field_validator("messages", "reach", "impressions", "clicks")
    @classmethod
    def non_negative_int(cls, value: Optional[int], info: ValidationInfo):
        if value is not None and value < 0:
            raise ValueError(f"{info.field_name} must be non-negative.")
        return value

    @field_validator("spent")
    @classmethod
    def non_negative_float(cls, value: Optional[float]) -> Optional[float]:
        if value is not None and value < 0:
            raise ValueError("spent must be non-negative.")
        return value


class BusinessCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    business_id: str
    name: str
    password_hash: str
    created_at: datetime

    @field_validator("business_id", "name", "password_hash")
    @classmethod
    def not_blank(cls, value: str) -> str:
        if not value or not value.strip():
            raise ValueError("Field cannot be blank.")
        return value.strip()
