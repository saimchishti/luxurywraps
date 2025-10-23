"""Repository layer encapsulating database CRUD logic."""

from __future__ import annotations

import csv
import io
import math
import random
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import bcrypt
from pymongo import ASCENDING, DESCENDING
from pymongo.collection import Collection
from pymongo.errors import DuplicateKeyError
from ulid import new as ulid_new

from models.validators import (
    PayloadValidationError,
    validate_ad,
    validate_ad_update,
    validate_campaign,
    validate_campaign_update,
    validate_registration,
    validate_registration_update,
)
from services.db import get_db
from utils.constants import CAMPAIGN_STATUSES, DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE


def _sanitize(value):
    if isinstance(value, date) and not isinstance(value, datetime):
        return datetime(value.year, value.month, value.day)
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if isinstance(value, (str, int, bool, float, datetime)) or value is None:
        return value
    if isinstance(value, list):
        return [_sanitize(v) for v in value]
    if isinstance(value, dict):
        out: Dict[str, Any] = {}
        for k, v in value.items():
            ks = str(k).replace(".", "_")
            if ks.startswith("$"):
                ks = "USD_" + ks[1:]
            out[ks] = _sanitize(v)
        return out
    return str(value)


def _sanitize_doc(doc: dict) -> dict:
    d = _sanitize(dict(doc))
    d.setdefault("created_at", datetime.utcnow())
    d["updated_at"] = datetime.utcnow()
    return d


class RepositoryError(RuntimeError):
    """Raised when database operations fail."""


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _ensure_page(page: int) -> int:
    return max(page, 1)


def _ensure_page_size(page_size: int) -> int:
    return min(max(page_size, 1), MAX_PAGE_SIZE)


def _clean(doc: Dict[str, Any]) -> Dict[str, Any]:
    doc = dict(doc)
    doc.pop("_id", None)
    return doc


def _require_business_id(business_id: str) -> str:
    if not business_id or not str(business_id).strip():
        raise ValueError("business_id is required.")
    return str(business_id).strip()


def _with_business(filters: Dict[str, Any], business_id: str) -> Dict[str, Any]:
    scoped = {key: value for key, value in (filters or {}).items() if value is not None}
    scoped["business_id"] = _require_business_id(business_id)
    return scoped


def _paginate(
    collection: Collection,
    filters: Dict[str, Any],
    page: int,
    page_size: int,
    sort: List[Tuple[str, int]],
) -> Dict[str, Any]:
    total = collection.count_documents(filters or {})
    cursor = (
        collection.find(filters or {})
        .sort(sort)
        .skip((page - 1) * page_size)
        .limit(page_size)
    )
    items = [_clean(item) for item in cursor]
    return {"items": items, "total": total, "page": page, "page_size": page_size}


# Collections -----------------------------------------------------------------

def businesses_collection() -> Collection:
    return get_db().businesses


def ads_collection() -> Collection:
    return get_db().ads


def campaigns_collection() -> Collection:
    return get_db().campaigns


def registrations_collection() -> Collection:
    return get_db().registrations


def get_business(business_id: str) -> Optional[Dict[str, Any]]:
    doc = businesses_collection().find_one({"business_id": business_id})
    return _clean(doc) if doc else None


# Ads CRUD --------------------------------------------------------------------

def create_ad(data: Dict[str, Any], business_id: str) -> Dict[str, Any]:
    scoped_id = _require_business_id(business_id)
    payload_data = dict(data)
    payload_data["business_id"] = scoped_id
    payload = validate_ad(payload_data)
    payload.setdefault("ad_id", _new_id())
    payload["business_id"] = scoped_id
    payload = _sanitize_doc(payload)
    try:
        ads_collection().insert_one(payload)
    except DuplicateKeyError as exc:
        raise RepositoryError("Ad with same ad_id already exists.") from exc
    return _clean(payload)


def list_ads(
    business_id: str,
    search: Optional[str] = None,
    status: Optional[str] = None,
    tags: Optional[Iterable[str]] = None,
    page: int = 1,
    page_size: int = DEFAULT_PAGE_SIZE,
    dt_from: Optional[datetime] = None,
    dt_to: Optional[datetime] = None,
) -> Dict[str, Any]:
    scoped_id = _require_business_id(business_id)
    conditions: List[Dict[str, Any]] = [{"business_id": scoped_id}]
    if search:
        conditions.append({"title": {"$regex": search, "$options": "i"}})
    if status:
        conditions.append({"status": status})
    if tags:
        conditions.append({"tags": {"$all": list(tags)}})
    if dt_from:
        range_filter: Dict[str, Any] = {"$gte": dt_from}
        if dt_to:
            range_filter["$lte"] = dt_to
        conditions.append(
            {
                "$or": [
                    {"updated_at": range_filter},
                    {"created_at": range_filter},
                ]
            }
        )
    mongo_filter: Dict[str, Any] = {"$and": conditions} if conditions else {"business_id": scoped_id}
    page = _ensure_page(page)
    page_size = _ensure_page_size(page_size)
    return _paginate(ads_collection(), mongo_filter, page, page_size, [("updated_at", DESCENDING)])


def get_ad(ad_id: str, business_id: str) -> Optional[Dict[str, Any]]:
    doc = ads_collection().find_one({"ad_id": ad_id, "business_id": _require_business_id(business_id)})
    return _clean(doc) if doc else None


def update_ad(ad_id: str, patch: Dict[str, Any], business_id: str) -> Dict[str, Any]:
    scoped_id = _require_business_id(business_id)
    payload = validate_ad_update(patch)
    if not payload:
        raise PayloadValidationError("Nothing to update.")
    payload = _sanitize(dict(payload))
    if not isinstance(payload, dict):
        payload = {}
    payload["updated_at"] = datetime.utcnow()
    result = ads_collection().find_one_and_update(
        {"ad_id": ad_id, "business_id": scoped_id},
        {"$set": payload},
        return_document=True,
    )
    if not result:
        raise RepositoryError("Ad not found.")
    return _clean(result)


def delete_ad(ad_id: str, business_id: str) -> bool:
    result = ads_collection().delete_one({"ad_id": ad_id, "business_id": _require_business_id(business_id)})
    return result.deleted_count > 0


def campaigns_using_ad(ad_id: str, business_id: str) -> List[Dict[str, Any]]:
    cursor = campaigns_collection().find(
        {"business_id": _require_business_id(business_id), "ad_ids": ad_id},
        {"_id": 0},
    )
    return list(cursor)


# Campaigns CRUD --------------------------------------------------------------

def create_campaign(data: Dict[str, Any], business_id: str) -> Dict[str, Any]:
    scoped_id = _require_business_id(business_id)
    payload_data = dict(data)
    payload_data["business_id"] = scoped_id
    payload = validate_campaign(payload_data)
    payload.setdefault("campaign_id", _new_id())
    payload.setdefault("status", "draft")
    payload.setdefault("business_type", "wedding_decor")
    payload["business_id"] = scoped_id
    payload = _sanitize_doc(payload)
    try:
        campaigns_collection().insert_one(payload)
    except DuplicateKeyError as exc:
        raise RepositoryError("Campaign with same campaign_id already exists.") from exc
    return _clean(payload)


def list_campaigns(
    business_id: str,
    q: Optional[str] = None,
    status: Optional[str] = None,
    page: int = 1,
    page_size: int = DEFAULT_PAGE_SIZE,
    dt_from: Optional[datetime] = None,
    dt_to: Optional[datetime] = None,
) -> Dict[str, Any]:
    scoped_id = _require_business_id(business_id)
    conditions: List[Dict[str, Any]] = [{"business_id": scoped_id}]
    if q:
        conditions.append({"name": {"$regex": q, "$options": "i"}})
    if status:
        if status not in CAMPAIGN_STATUSES:
            raise PayloadValidationError(f"Unknown campaign status: {status}")
        conditions.append({"status": status})
    if dt_from:
        range_filter: Dict[str, Any] = {"$gte": dt_from}
        if dt_to:
            range_filter["$lte"] = dt_to
        conditions.append(
            {
                "$or": [
                    {"updated_at": range_filter},
                    {"created_at": range_filter},
                ]
            }
        )
    mongo_filter = {"$and": conditions} if conditions else {"business_id": scoped_id}
    page = _ensure_page(page)
    page_size = _ensure_page_size(page_size)
    return _paginate(
        campaigns_collection(),
        mongo_filter,
        page,
        page_size,
        [("updated_at", DESCENDING)],
    )


def get_campaign(campaign_id: str, business_id: str) -> Optional[Dict[str, Any]]:
    doc = campaigns_collection().find_one(
        {"campaign_id": campaign_id, "business_id": _require_business_id(business_id)}
    )
    return _clean(doc) if doc else None


def update_campaign(campaign_id: str, patch: Dict[str, Any], business_id: str) -> Dict[str, Any]:
    scoped_id = _require_business_id(business_id)
    payload = validate_campaign_update(patch)
    if not payload:
        raise PayloadValidationError("Nothing to update.")
    payload = _sanitize(dict(payload))
    if not isinstance(payload, dict):
        payload = {}
    payload["updated_at"] = datetime.utcnow()
    result = campaigns_collection().find_one_and_update(
        {"campaign_id": campaign_id, "business_id": scoped_id},
        {"$set": payload},
        return_document=True,
    )
    if not result:
        raise RepositoryError("Campaign not found.")
    return _clean(result)


def delete_campaign(campaign_id: str, business_id: str) -> bool:
    result = campaigns_collection().delete_one(
        {"campaign_id": campaign_id, "business_id": _require_business_id(business_id)}
    )
    return result.deleted_count > 0


def attach_ads(campaign_id: str, ad_ids: Iterable[str], business_id: str) -> Dict[str, Any]:
    scoped_id = _require_business_id(business_id)
    ad_ids = [ad_id for ad_id in ad_ids if ad_id]
    if not ad_ids:
        raise PayloadValidationError("No ads selected.")
    missing = ads_collection().count_documents(
        {"business_id": scoped_id, "ad_id": {"$in": ad_ids}}
    )
    if missing != len(ad_ids):
        raise RepositoryError("One or more ads do not belong to this business.")
    result = campaigns_collection().find_one_and_update(
        {"campaign_id": campaign_id, "business_id": scoped_id},
        {"$addToSet": {"ad_ids": {"$each": ad_ids}}, "$set": {"updated_at": datetime.utcnow()}},
        return_document=True,
    )
    if not result:
        raise RepositoryError("Campaign not found.")
    return _clean(result)


def detach_ads(campaign_id: str, ad_ids: Iterable[str], business_id: str) -> Dict[str, Any]:
    scoped_id = _require_business_id(business_id)
    ad_ids = [ad_id for ad_id in ad_ids if ad_id]
    if not ad_ids:
        raise PayloadValidationError("No ads selected.")
    result = campaigns_collection().find_one_and_update(
        {"campaign_id": campaign_id, "business_id": scoped_id},
        {"$pull": {"ad_ids": {"$in": ad_ids}}, "$set": {"updated_at": datetime.utcnow()}},
        return_document=True,
    )
    if not result:
        raise RepositoryError("Campaign not found.")
    return _clean(result)


# Registrations CRUD ----------------------------------------------------------

def create_registration(data: Dict[str, Any], business_id: str) -> Dict[str, Any]:
    scoped_id = _require_business_id(business_id)
    payload_data = dict(data)
    payload_data["business_id"] = scoped_id
    payload = validate_registration(payload_data)
    payload.setdefault("registration_id", _new_id())
    timestamp = payload["timestamp"]
    if timestamp.tzinfo is None:
        payload["timestamp"] = timestamp.replace(tzinfo=timezone.utc)
    payload["business_id"] = scoped_id
    payload = _sanitize_doc(payload)
    try:
        registrations_collection().insert_one(payload)
    except DuplicateKeyError as exc:
        raise RepositoryError("Registration with same registration_id already exists.") from exc
    return _clean(payload)


def list_registrations(
    business_id: str,
    campaign_ids: Optional[Iterable[str]] = None,
    ad_ids: Optional[Iterable[str]] = None,
    sources: Optional[Iterable[str]] = None,
    page: int = 1,
    page_size: int = DEFAULT_PAGE_SIZE,
    dt_from: Optional[datetime] = None,
    dt_to: Optional[datetime] = None,
) -> Dict[str, Any]:
    scoped_id = _require_business_id(business_id)
    filters: Dict[str, Any] = {"business_id": scoped_id}
    if campaign_ids:
        filters["campaign_id"] = {"$in": list(campaign_ids)}
    if ad_ids:
        filters["ad_id"] = {"$in": list(ad_ids)}
    if sources:
        filters["source"] = {"$in": list(sources)}
    if dt_from:
        ts_filter: Dict[str, Any] = {"$gte": dt_from}
        if dt_to:
            ts_filter["$lte"] = dt_to
        filters["timestamp"] = ts_filter
    page = _ensure_page(page)
    page_size = _ensure_page_size(page_size)
    return _paginate(
        registrations_collection(),
        filters,
        page,
        page_size,
        [("timestamp", DESCENDING)],
    )


def update_registration(registration_id: str, patch: Dict[str, Any], business_id: str) -> Dict[str, Any]:
    scoped_id = _require_business_id(business_id)
    payload = validate_registration_update(patch)
    if not payload:
        raise PayloadValidationError("Nothing to update.")
    payload = _sanitize(dict(payload))
    if not isinstance(payload, dict):
        payload = {}
    payload["updated_at"] = datetime.utcnow()
    result = registrations_collection().find_one_and_update(
        {"registration_id": registration_id, "business_id": scoped_id},
        {"$set": payload},
        return_document=True,
    )
    if not result:
        raise RepositoryError("Registration not found.")
    return _clean(result)


def delete_registration(registration_id: str, business_id: str) -> bool:
    result = registrations_collection().delete_one(
        {"registration_id": registration_id, "business_id": _require_business_id(business_id)}
    )
    return result.deleted_count > 0


def export_registrations_csv(query: Dict[str, Any], *, business_id: str) -> io.BytesIO:
    """Return buffered CSV with filtered registrations."""
    db = get_db()
    q = dict(query or {})
    q["business_id"] = _require_business_id(business_id)
    docs = list(db.registrations.find(q, {"_id": 0}))

    desired = [
        "timestamp",
        "campaign_id",
        "ad_id",
        "source",
        "messages",
        "spent",
        "reach",
        "impressions",
        "clicks",
        "user_id",
        "business_id",
        "created_at",
        "updated_at",
    ]

    def _to_iso(value):
        if isinstance(value, datetime):
            return value.isoformat()
        return value

    rows: List[Dict[str, Any]] = []
    for doc in docs:
        row = {}
        for column in desired:
            row[column] = _to_iso(doc.get(column))
        rows.append(row)

    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=desired, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)

    out = io.BytesIO(buffer.getvalue().encode("utf-8"))
    out.seek(0)
    return out


# Seeder ----------------------------------------------------------------------

def seed_demo_data(days: int = 30, registrations: int = 200) -> Dict[str, int]:
    """Populate MongoDB with demo campaigns, ads, and registrations for each business."""
    db = get_db()
    now = _now()
    stats = {"businesses": 0, "ads": 0, "campaigns": 0, "registrations": 0}

    seeds = [
        {
            "business_id": "enchanments",
            "name": "Enchanments Wedding Decor",
            "password": "enchanments_pass",
            "ad_templates": [
                {"title": "Fairy Light Aisle Display", "tags": ["lighting", "aisle"]},
                {"title": "Garden Reception Setup", "tags": ["outdoor", "reception"]},
                {"title": "Luxury Table Centerpieces", "tags": ["centerpiece", "luxury"]},
            ],
            "campaign_templates": [
                {"name": "Spring Garden Weddings", "status": "active"},
                {"name": "Gold Elegance Collection", "status": "active"},
                {"name": "Evergreen Venue Partnerships", "status": "paused"},
            ],
        },
        {
            "business_id": "luxury_floor_wraps",
            "name": "Luxury Floor Wraps",
            "password": "luxury_pass",
            "ad_templates": [
                {"title": "Custom Dance Floor Reveal", "tags": ["dancefloor", "custom"]},
                {"title": "Monogrammed Floor Showcase", "tags": ["monogram", "branding"]},
                {"title": "Event Entry Statement", "tags": ["entry", "branding"]},
            ],
            "campaign_templates": [
                {"name": "Signature Ballroom Series", "status": "active"},
                {"name": "Corporate Launch Highlights", "status": "draft"},
                {"name": "Boutique Venue Partnerships", "status": "active"},
            ],
        },
    ]

    for entry in seeds:
        existing = db.businesses.find_one({"business_id": entry["business_id"]})
        if not existing:
            password_hash = bcrypt.hashpw(entry["password"].encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
            db.businesses.insert_one(
                {
                    "business_id": entry["business_id"],
                    "name": entry["name"],
                    "password_hash": password_hash,
                    "created_at": now,
                }
            )
            stats["businesses"] += 1

    for entry in seeds:
        business_id = entry["business_id"]
        ad_templates = entry["ad_templates"]
        if db.ads.count_documents({"business_id": business_id}) == 0:
            for template in ad_templates:
                create_ad(
                    {**template, "status": "active"},
                    business_id=business_id,
                )
                stats["ads"] += 1

        ad_docs = list(db.ads.find({"business_id": business_id}, {"ad_id": 1}))
        ad_ids = [doc["ad_id"] for doc in ad_docs]

        campaign_templates = entry["campaign_templates"]
        if db.campaigns.count_documents({"business_id": business_id}) == 0:
            for idx, template in enumerate(campaign_templates):
                attached_ads = ad_ids[idx::len(campaign_templates)] if ad_ids else []
                create_campaign(
                    {
                        **template,
                        "ad_ids": attached_ads,
                        "targeting": {
                            "locations": ["NY", "NJ", "CT"],
                            "interests": ["weddings", "events"],
                            "devices": ["mobile", "desktop"],
                            "budget_daily": 150.0,
                        },
                    },
                    business_id=business_id,
                )
                stats["campaigns"] += 1

        campaign_docs = list(db.campaigns.find({"business_id": business_id}, {"campaign_id": 1}))
        if not campaign_docs or not ad_ids:
            continue

        existing_regs = db.registrations.count_documents({"business_id": business_id})
        if existing_regs > 0:
            continue

        per_business_regs = max(1, registrations // len(seeds))
        sources = ["facebook", "google", "organic", "email", "referral"]
        for _ in range(per_business_regs):
            campaign_id = random.choice(campaign_docs)["campaign_id"]
            ad_id = random.choice(ad_ids)
            day_offset = random.randint(10, max(10, min(days, 30)))
            timestamp = now - timedelta(days=day_offset, hours=random.randint(0, 23), minutes=random.randint(0, 59))
            impressions = random.randint(500, 5000)
            clicks = random.randint(0, impressions)
            registrations_count = random.randint(1, 4)
            total_spent = round(random.uniform(50, 400), 2)
            for _ in range(registrations_count):
                per_spend = round(total_spent / registrations_count, 2)
                payload = {
                    "campaign_id": campaign_id,
                    "ad_id": ad_id,
                    "source": random.choice(sources),
                    "cost": per_spend,
                    "timestamp": timestamp,
                    "meta": {"utm_campaign": campaign_id},
                    "messages": random.randint(0, 10),
                    "spent": per_spend,
                    "reach": random.randint(200, impressions),
                    "impressions": impressions,
                    "clicks": clicks,
                }
                create_registration(payload, business_id=business_id)
                stats["registrations"] += 1

    return stats


def _new_id() -> str:
    return ulid_new().str
