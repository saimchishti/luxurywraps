"""MongoDB connection helpers and index management."""

from __future__ import annotations

import os
import re
from functools import lru_cache
from pathlib import Path
from typing import Dict, Set

import streamlit as st
from dotenv import load_dotenv
from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.database import Database

_ENV_READY = False
_DB_CACHE: Dict[str, Database] = {}
_INDEXED_DBS: Set[str] = set()


def _load_env() -> None:
    """Load environment variables from project .env once."""
    global _ENV_READY
    if _ENV_READY:
        return

    root = Path(__file__).resolve().parents[1]
    env_path = root / ".env"
    load_dotenv(env_path if env_path.exists() else None)
    load_dotenv(override=False)  # fall back to environment defaults
    _ENV_READY = True


@lru_cache(maxsize=1)
def get_client() -> MongoClient:
    """Return a cached MongoDB client using the configured URI."""
    _load_env()
    uri = os.getenv("MONGODB_URI")
    if not uri:
        raise RuntimeError("MONGODB_URI not set. Configure it via environment or .env file.")
    return MongoClient(uri, serverSelectionTimeoutMS=5000)


def _db_name_for(business_id: str) -> str:
    """Return a normalized database name for the given business identifier."""
    slug = re.sub(r"[^a-z0-9]+", "_", str(business_id).strip().lower())
    slug = slug.strip("_") or "tenant"
    return f"{slug}_ops"


def get_db() -> Database:
    """Return a tenant-scoped MongoDB database, creating indexes on first use."""
    _load_env()

    default_db = os.getenv("MONGODB_DB")
    if not default_db:
        raise RuntimeError("MONGODB_DB not set. Configure it via environment or .env file.")

    business_id = None
    try:
        session_state = st.session_state
    except Exception:  # pragma: no cover - protects CLI utilities and tests
        session_state = None

    if session_state:
        business_id = session_state.get("business_id")

    db_name = _db_name_for(business_id) if business_id else default_db

    if db_name not in _DB_CACHE:
        db = get_client()[db_name]
        _ensure_indexes(db)
        _DB_CACHE[db_name] = db

    return _DB_CACHE[db_name]


def _ensure_indexes(db: Database) -> None:
    """Create required indexes for a database if not already applied."""
    if db.name in _INDEXED_DBS:
        return

    db.businesses.create_index([("business_id", ASCENDING)], name="idx_business_id", unique=True)

    db.ads.create_index(
        [("business_id", ASCENDING), ("ad_id", ASCENDING)],
        name="idx_ads_business_ad",
        unique=True,
    )
    db.ads.create_index(
        [("business_id", ASCENDING), ("status", ASCENDING), ("updated_at", DESCENDING)],
        name="idx_ads_business_status_updated",
    )

    db.campaigns.create_index(
        [("business_id", ASCENDING), ("campaign_id", ASCENDING)],
        name="idx_campaigns_business_campaign",
        unique=True,
    )
    db.campaigns.create_index(
        [("business_id", ASCENDING), ("status", ASCENDING), ("updated_at", DESCENDING)],
        name="idx_campaigns_business_status_updated",
    )

    db.registrations.create_index(
        [
            ("business_id", ASCENDING),
            ("campaign_id", ASCENDING),
            ("ad_id", ASCENDING),
            ("timestamp", DESCENDING),
        ],
        name="idx_registrations_business_campaign_ad_ts",
    )
    db.registrations.create_index(
        [("business_id", ASCENDING), ("registration_id", ASCENDING)],
        name="idx_registrations_business_reg",
        unique=True,
    )

    _INDEXED_DBS.add(db.name)


def ensure_indexes(force: bool = False) -> None:
    """Public helper retained for compatibility to ensure indexes are created."""
    db = get_db()
    if force:
        _INDEXED_DBS.discard(db.name)
        _ensure_indexes(db)
        return
    _ensure_indexes(db)
