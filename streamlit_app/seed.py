"""CLI utility for seeding MongoDB demo data for the Streamlit app."""

from __future__ import annotations

import argparse
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Tuple

import bcrypt
from dotenv import load_dotenv
from pymongo import ASCENDING, MongoClient
from pymongo.database import Database

DEFAULT_MODE = "businesses-only"
REGISTRATION_DAYS = 30

TENANT_DEFINITIONS = [
    {
        "business_id": "enchanments",
        "name": "Enchanments Wedding Decor",
        "password": "enchanments_pass",
        "ads": [
            {"ad_id": "enchanments-ad-1", "title": "Fairy Light Aisle Display", "status": "active", "tags": ["lighting", "aisle"]},
            {"ad_id": "enchanments-ad-2", "title": "Garden Reception Setup", "status": "active", "tags": ["outdoor", "reception"]},
            {"ad_id": "enchanments-ad-3", "title": "Luxury Table Centerpieces", "status": "paused", "tags": ["centerpiece", "luxury"]},
        ],
        "campaigns": [
            {
                "campaign_id": "enchanments-campaign-1",
                "name": "Spring Garden Weddings",
                "status": "active",
                "business_type": "wedding_decor",
                "ad_ids": ["enchanments-ad-1", "enchanments-ad-2"],
                "targeting": {
                    "locations": ["New York", "New Jersey", "Connecticut"],
                    "interests": ["wedding decor", "event planning"],
                    "devices": ["mobile", "desktop"],
                    "budget_daily": 180.0,
                },
            },
            {
                "campaign_id": "enchanments-campaign-2",
                "name": "Golden Evenings Showcase",
                "status": "paused",
                "business_type": "wedding_decor",
                "ad_ids": ["enchanments-ad-3"],
                "targeting": {
                    "locations": ["New York"],
                    "interests": ["luxury weddings", "evening receptions"],
                    "devices": ["desktop"],
                    "budget_daily": 120.0,
                },
            },
        ],
    },
    {
        "business_id": "luxury_floor_wraps",
        "name": "Luxury Floor Wraps",
        "password": "luxury_pass",
        "ads": [
            {"ad_id": "luxury-ad-1", "title": "Custom Dance Floor Reveal", "status": "active", "tags": ["dancefloor", "custom"]},
            {"ad_id": "luxury-ad-2", "title": "Monogrammed Floor Showcase", "status": "active", "tags": ["monogram", "branding"]},
            {"ad_id": "luxury-ad-3", "title": "Event Entry Statement", "status": "archived", "tags": ["entry", "branding"]},
        ],
        "campaigns": [
            {
                "campaign_id": "luxury-campaign-1",
                "name": "Signature Ballroom Series",
                "status": "active",
                "business_type": "event_production",
                "ad_ids": ["luxury-ad-1", "luxury-ad-2"],
                "targeting": {
                    "locations": ["Florida", "Georgia", "Texas"],
                    "interests": ["luxury events", "corporate galas"],
                    "devices": ["mobile", "desktop"],
                    "budget_daily": 200.0,
                },
            },
            {
                "campaign_id": "luxury-campaign-2",
                "name": "Boutique Venue Partnerships",
                "status": "draft",
                "business_type": "event_production",
                "ad_ids": ["luxury-ad-2", "luxury-ad-3"],
                "targeting": {
                    "locations": ["California", "Nevada"],
                    "interests": ["event venues", "wedding planners"],
                    "devices": ["mobile"],
                    "budget_daily": 150.0,
                },
            },
        ],
    },
]

_ENV_LOADED = False
_CLIENT: MongoClient | None = None


def load_environment() -> None:
    """Load environment variables from .env files in current and parent directories."""
    global _ENV_LOADED
    if _ENV_LOADED:
        return

    script_dir = Path(__file__).resolve().parent
    candidates = [
        Path.cwd() / ".env",
        Path.cwd().parent / ".env" if Path.cwd().parent else None,
        script_dir / ".env",
        script_dir.parent / ".env",
    ]

    seen = set()
    for path in candidates:
        if not path or not path.exists():
            continue
        resolved = str(path.resolve())
        if resolved in seen:
            continue
        load_dotenv(resolved, override=False)
        seen.add(resolved)

    _ENV_LOADED = True


def get_db() -> Database:
    """Return a MongoDB database using environment configuration."""
    global _CLIENT
    load_environment()

    uri = os.getenv("MONGODB_URI")
    if not uri:
        raise RuntimeError("MONGODB_URI not set. Configure it in the environment or .env file.")

    db_name = os.getenv("MONGODB_DB")
    if not db_name:
        raise RuntimeError("MONGODB_DB not set. Configure it in the environment or .env file.")

    if _CLIENT is None:
        _CLIENT = MongoClient(uri, serverSelectionTimeoutMS=5000)
    return _CLIENT[db_name]


def ensure_indexes(db: Database) -> None:
    """Ensure required indexes exist prior to seeding."""
    db.businesses.create_index([("business_id", ASCENDING)], name="idx_business_id_unique", unique=True)


def _hash_password(password: str, existing_hash: str | None) -> str:
    if existing_hash and bcrypt.checkpw(password.encode("utf-8"), existing_hash.encode("utf-8")):
        return existing_hash
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def upsert_businesses(db: Database, now: datetime) -> Tuple[int, int]:
    created, updated = 0, 0
    for tenant in TENANT_DEFINITIONS:
        tenant_id = tenant["business_id"]
        existing = db.businesses.find_one({"business_id": tenant_id})
        password_hash = _hash_password(tenant["password"], existing.get("password_hash") if existing else None)
        result = db.businesses.update_one(
            {"business_id": tenant_id},
            {
                "$set": {
                    "business_id": tenant_id,
                    "name": tenant["name"],
                    "password_hash": password_hash,
                    "updated_at": now,
                },
                "$setOnInsert": {"created_at": now},
            },
            upsert=True,
        )
        if result.upserted_id is not None:
            created += 1
        elif result.modified_count:
            updated += 1
    return created, updated


def upsert_ads(db: Database, tenant_id: str, templates: List[Dict[str, object]], now: datetime) -> Tuple[int, int, List[str]]:
    created, updated = 0, 0
    for template in templates:
        ad_id = template["ad_id"]
        payload = {
            "business_id": tenant_id,
            "title": template["title"],
            "status": template.get("status", "active"),
            "tags": template.get("tags", []),
            "updated_at": now,
        }
        if template.get("creative_url"):
            payload["creative_url"] = template["creative_url"]
        result = db.ads.update_one(
            {"business_id": tenant_id, "ad_id": ad_id},
            {
                "$set": payload,
                "$setOnInsert": {
                    "business_id": tenant_id,
                    "ad_id": ad_id,
                    "created_at": now,
                },
            },
            upsert=True,
        )
        if result.upserted_id is not None:
            created += 1
        elif result.modified_count:
            updated += 1
    ad_ids = [template["ad_id"] for template in templates]
    return created, updated, ad_ids


def upsert_campaigns(
    db: Database,
    tenant_id: str,
    templates: List[Dict[str, object]],
    available_ad_ids: List[str],
    now: datetime,
) -> Tuple[int, int, List[str]]:
    created, updated = 0, 0
    campaign_ids: List[str] = []
    for template in templates:
        campaign_id = template["campaign_id"]
        linked_ad_ids = [ad_id for ad_id in template.get("ad_ids", []) if ad_id in available_ad_ids]
        if not linked_ad_ids:
            linked_ad_ids = available_ad_ids[:2]
        targeting = dict(template.get("targeting", {}))
        targeting.setdefault("locations", [])
        targeting.setdefault("interests", [])
        targeting.setdefault("devices", [])
        targeting.setdefault("budget_daily", 0.0)
        start_value = targeting.get("start_date")
        if isinstance(start_value, datetime):
            start_dt = start_value
        elif isinstance(start_value, date):
            start_dt = datetime.combine(start_value, datetime.min.time()).replace(tzinfo=timezone.utc)
        else:
            start_dt = (now - timedelta(days=30)).replace(hour=0, minute=0, second=0, microsecond=0)
        targeting["start_date"] = start_dt

        end_value = targeting.get("end_date")
        if isinstance(end_value, datetime):
            end_dt = end_value
        elif isinstance(end_value, date):
            end_dt = datetime.combine(end_value, datetime.min.time()).replace(tzinfo=timezone.utc)
        else:
            end_dt = (now + timedelta(days=60)).replace(hour=0, minute=0, second=0, microsecond=0)
        targeting["end_date"] = end_dt

        payload = {
            "business_id": tenant_id,
            "name": template["name"],
            "status": template.get("status", "draft"),
            "ad_ids": linked_ad_ids,
            "targeting": targeting,
            "business_type": template.get("business_type", "wedding_decor"),
            "updated_at": now,
        }
        result = db.campaigns.update_one(
            {"business_id": tenant_id, "campaign_id": campaign_id},
            {
                "$set": payload,
                "$setOnInsert": {
                    "business_id": tenant_id,
                    "campaign_id": campaign_id,
                    "created_at": now,
                },
            },
            upsert=True,
        )
        if result.upserted_id is not None:
            created += 1
        elif result.modified_count:
            updated += 1
        campaign_ids.append(campaign_id)
    return created, updated, campaign_ids


def upsert_registrations(
    db: Database,
    tenant_id: str,
    campaign_ids: List[str],
    ad_ids: List[str],
    now: datetime,
) -> Tuple[int, int]:
    created, updated = 0, 0
    if not campaign_ids or not ad_ids:
        return created, updated

    sources = ["facebook", "instagram", "google", "email", "referral"]
    for day in range(REGISTRATION_DAYS):
        timestamp = (now - timedelta(days=day)).replace(hour=15, minute=30, second=0, microsecond=0)
        campaign_id = campaign_ids[day % len(campaign_ids)]
        ad_id = ad_ids[day % len(ad_ids)]
        registration_id = f"{tenant_id}-reg-{timestamp.strftime('%Y%m%d')}"
        cost = round(85.0 + day * 2.5, 2)
        spent = round(cost + 15.0, 2)
        reach = 750 + day * 25
        impressions = reach + 200
        clicks = max(10, impressions // 25)
        payload = {
            "business_id": tenant_id,
            "campaign_id": campaign_id,
            "ad_id": ad_id,
            "source": sources[day % len(sources)],
            "cost": cost,
            "spent": spent,
            "messages": 3 + (day % 5),
            "reach": reach,
            "impressions": impressions,
            "clicks": clicks,
            "timestamp": timestamp,
            "meta": {"note": "Seed registration data"},
        }
        result = db.registrations.update_one(
            {"registration_id": registration_id, "business_id": tenant_id},
            {
                "$set": payload,
                "$setOnInsert": {
                    "registration_id": registration_id,
                    "business_id": tenant_id,
                    "created_at": timestamp,
                },
            },
            upsert=True,
        )
        if result.upserted_id is not None:
            created += 1
        elif result.modified_count:
            updated += 1
    return created, updated


def seed_demo_data(mode: str = DEFAULT_MODE) -> Dict[str, int]:
    """Seed demo data into MongoDB based on the selected mode."""
    normalized_mode = mode.strip().lower()
    if normalized_mode not in {"businesses-only", "full"}:
        raise ValueError("mode must be 'businesses-only' or 'full'.")

    db = get_db()
    ensure_indexes(db)

    now = datetime.now(timezone.utc)
    counts = {
        "businesses_created": 0,
        "businesses_updated": 0,
        "ads_created": 0,
        "ads_updated": 0,
        "campaigns_created": 0,
        "campaigns_updated": 0,
        "registrations_created": 0,
        "registrations_updated": 0,
    }

    created, updated = upsert_businesses(db, now)
    counts["businesses_created"] = created
    counts["businesses_updated"] = updated

    if normalized_mode == "full":
        for tenant in TENANT_DEFINITIONS:
            tenant_id = tenant["business_id"]
            ads_created, ads_updated, ad_ids = upsert_ads(db, tenant_id, tenant.get("ads", []), now)
            counts["ads_created"] += ads_created
            counts["ads_updated"] += ads_updated

            campaigns_created, campaigns_updated, campaign_ids = upsert_campaigns(
                db, tenant_id, tenant.get("campaigns", []), ad_ids, now
            )
            counts["campaigns_created"] += campaigns_created
            counts["campaigns_updated"] += campaigns_updated

            registrations_created, registrations_updated = upsert_registrations(
                db, tenant_id, campaign_ids, ad_ids, now
            )
            counts["registrations_created"] += registrations_created
            counts["registrations_updated"] += registrations_updated

    return counts


def parse_args() -> str:
    parser = argparse.ArgumentParser(description="Seed MongoDB with demo tenants and sample data.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--businesses-only",
        dest="mode",
        action="store_const",
        const="businesses-only",
        default=DEFAULT_MODE,
        help="Seed only tenant accounts (default).",
    )
    group.add_argument(
        "--full",
        dest="mode",
        action="store_const",
        const="full",
        help="Seed tenants plus sample campaigns, ads, and registrations.",
    )
    args = parser.parse_args()
    return args.mode


def main() -> None:
    mode = parse_args()
    counts = seed_demo_data(mode)

    print("Seed complete.")
    print(f"  Mode: {mode}")
    print(f"  Businesses created: {counts['businesses_created']} | updated: {counts['businesses_updated']}")
    print(f"  Ads created: {counts['ads_created']} | updated: {counts['ads_updated']}")
    print(f"  Campaigns created: {counts['campaigns_created']} | updated: {counts['campaigns_updated']}")
    print(
        f"  Registrations created: {counts['registrations_created']} | updated: {counts['registrations_updated']}"
    )


if __name__ == "__main__":
    main()
