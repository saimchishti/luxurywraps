"""Aggregation utilities for analytics dashboards."""

from __future__ import annotations

from datetime import datetime
from typing import Dict, Iterable, List, Optional

from pymongo.database import Database

from services.db import get_db


def _match_base(
    business_id: str,
    dt_from: Optional[datetime],
    dt_to: Optional[datetime],
    campaign_ids: Optional[Iterable[str]] = None,
    ad_ids: Optional[Iterable[str]] = None,
) -> Dict[str, object]:
    match: Dict[str, object] = {"business_id": business_id}
    if dt_from and dt_to:
        match["timestamp"] = {"$gte": dt_from, "$lte": dt_to}
    if campaign_ids:
        match["campaign_id"] = {"$in": list(campaign_ids)}
    if ad_ids:
        match["ad_id"] = {"$in": list(ad_ids)}
    return match


def kpis(
    db: Optional[Database],
    dt_from: Optional[datetime],
    dt_to: Optional[datetime],
    business_id: str,
    campaign_ids: Optional[Iterable[str]] = None,
    ad_ids: Optional[Iterable[str]] = None,
) -> Dict[str, float]:
    db = db or get_db()
    match = _match_base(business_id, dt_from, dt_to, campaign_ids, ad_ids)
    pipeline = [
        {"$match": match},
        {
            "$group": {
                "_id": None,
                "messages": {"$sum": {"$ifNull": ["$messages", 0]}},
                "spent": {"$sum": {"$ifNull": ["$spent", {"$ifNull": ["$cost", 0]}]}},
                "reach": {"$sum": {"$ifNull": ["$reach", 0]}},
                "impressions": {"$sum": {"$ifNull": ["$impressions", 0]}},
                "clicks": {"$sum": {"$ifNull": ["$clicks", 0]}},
                "registrations": {"$sum": 1},
            }
        },
        {
            "$project": {
                "_id": 0,
                "messages": 1,
                "spent": 1,
                "reach": 1,
                "impressions": 1,
                "clicks": 1,
                "registrations": 1,
                "ctr": {
                    "$cond": [
                        {"$gt": ["$impressions", 0]},
                        {"$divide": ["$clicks", "$impressions"]},
                        0,
                    ]
                },
                "cpm": {
                    "$cond": [
                        {"$gt": ["$impressions", 0]},
                        {
                            "$divide": [
                                "$spent",
                                {"$divide": ["$impressions", 1000]},
                            ]
                        },
                        0,
                    ]
                },
                "cpc": {
                    "$cond": [
                        {"$gt": ["$clicks", 0]},
                        {"$divide": ["$spent", "$clicks"]},
                        0,
                    ]
                },
                "cpr": {
                    "$cond": [
                        {"$gt": ["$registrations", 0]},
                        {"$divide": ["$spent", "$registrations"]},
                        0,
                    ]
                },
            }
        },
    ]
    result = list(db.registrations.aggregate(pipeline))
    if result:
        return result[0]
    return {
        "messages": 0,
        "spent": 0,
        "reach": 0,
        "impressions": 0,
        "clicks": 0,
        "registrations": 0,
        "ctr": 0,
        "cpm": 0,
        "cpc": 0,
        "cpr": 0,
    }


def timeseries_daily(
    db: Optional[Database],
    dt_from: Optional[datetime],
    dt_to: Optional[datetime],
    business_id: str,
    campaign_ids: Optional[Iterable[str]] = None,
    ad_ids: Optional[Iterable[str]] = None,
) -> List[Dict[str, object]]:
    db = db or get_db()
    match = _match_base(business_id, dt_from, dt_to, campaign_ids, ad_ids)
    pipeline = [
        {"$match": match},
        {
            "$group": {
                "_id": {
                    "$dateTrunc": {
                        "date": "$timestamp",
                        "unit": "day",
                    }
                },
                "messages": {"$sum": {"$ifNull": ["$messages", 0]}},
                "spent": {"$sum": {"$ifNull": ["$spent", {"$ifNull": ["$cost", 0]}]}},
                "reach": {"$sum": {"$ifNull": ["$reach", 0]}},
                "impressions": {"$sum": {"$ifNull": ["$impressions", 0]}},
                "clicks": {"$sum": {"$ifNull": ["$clicks", 0]}},
                "registrations": {"$sum": 1},
            }
        },
        {"$sort": {"_id": 1}},
        {
            "$project": {
                "_id": 0,
                "date": "$_id",
                "messages": 1,
                "spent": 1,
                "reach": 1,
                "impressions": 1,
                "clicks": 1,
                "registrations": 1,
                "cpr": {
                    "$cond": [
                        {"$gt": ["$registrations", 0]},
                        {"$divide": ["$spent", "$registrations"]},
                        0,
                    ]
                },
            }
        },
    ]
    return list(db.registrations.aggregate(pipeline))


def campaign_rollup(
    db: Optional[Database],
    dt_from: Optional[datetime],
    dt_to: Optional[datetime],
    business_id: str,
) -> List[Dict[str, object]]:
    db = db or get_db()
    match = _match_base(business_id, dt_from, dt_to)
    pipeline = [
        {"$match": match},
        {
            "$group": {
                "_id": "$campaign_id",
                "messages": {"$sum": {"$ifNull": ["$messages", 0]}},
                "spent": {"$sum": {"$ifNull": ["$spent", {"$ifNull": ["$cost", 0]}]}},
                "reach": {"$sum": {"$ifNull": ["$reach", 0]}},
                "impressions": {"$sum": {"$ifNull": ["$impressions", 0]}},
                "clicks": {"$sum": {"$ifNull": ["$clicks", 0]}},
                "registrations": {"$sum": 1},
            }
        },
        {
            "$lookup": {
                "from": "campaigns",
                "let": {"campaign_id": "$_id"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$campaign_id", "$$campaign_id"]},
                                    {"$eq": ["$business_id", business_id]},
                                ]
                            }
                        }
                    }
                ],
                "as": "campaign",
            }
        },
        {"$unwind": {"path": "$campaign", "preserveNullAndEmptyArrays": True}},
        {
            "$project": {
                "_id": 0,
                "campaign_id": "$_id",
                "name": "$campaign.name",
                "status": "$campaign.status",
                "messages": 1,
                "spent": 1,
                "reach": 1,
                "impressions": 1,
                "clicks": 1,
                "registrations": 1,
                "ctr": {
                    "$cond": [
                        {"$gt": ["$impressions", 0]},
                        {"$divide": ["$clicks", "$impressions"]},
                        0,
                    ]
                },
                "cpr": {
                    "$cond": [
                        {"$gt": ["$registrations", 0]},
                        {"$divide": ["$spent", "$registrations"]},
                        0,
                    ]
                },
            }
        },
        {"$sort": {"registrations": -1}},
    ]
    return list(db.registrations.aggregate(pipeline))


def ad_performance(
    db: Optional[Database],
    dt_from: Optional[datetime],
    dt_to: Optional[datetime],
    business_id: str,
    campaign_id: Optional[str] = None,
) -> List[Dict[str, object]]:
    db = db or get_db()
    match = _match_base(business_id, dt_from, dt_to, campaign_ids=[campaign_id] if campaign_id else None)
    pipeline = [
        {"$match": match},
        {
            "$group": {
                "_id": "$ad_id",
                "messages": {"$sum": {"$ifNull": ["$messages", 0]}},
                "spent": {"$sum": {"$ifNull": ["$spent", {"$ifNull": ["$cost", 0]}]}},
                "reach": {"$sum": {"$ifNull": ["$reach", 0]}},
                "impressions": {"$sum": {"$ifNull": ["$impressions", 0]}},
                "clicks": {"$sum": {"$ifNull": ["$clicks", 0]}},
                "registrations": {"$sum": 1},
            }
        },
        {
            "$lookup": {
                "from": "ads",
                "let": {"ad_id": "$_id"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$ad_id", "$$ad_id"]},
                                    {"$eq": ["$business_id", business_id]},
                                ]
                            }
                        }
                    }
                ],
                "as": "ad",
            }
        },
        {"$unwind": {"path": "$ad", "preserveNullAndEmptyArrays": True}},
        {
            "$project": {
                "_id": 0,
                "ad_id": "$_id",
                "title": "$ad.title",
                "tags": "$ad.tags",
                "messages": 1,
                "spent": 1,
                "reach": 1,
                "impressions": 1,
                "clicks": 1,
                "registrations": 1,
                "ctr": {
                    "$cond": [
                        {"$gt": ["$impressions", 0]},
                        {"$divide": ["$clicks", "$impressions"]},
                        0,
                    ]
                },
                "cpr": {
                    "$cond": [
                        {"$gt": ["$registrations", 0]},
                        {"$divide": ["$spent", "$registrations"]},
                        0,
                    ]
                },
            }
        },
        {"$sort": {"registrations": -1}},
    ]
    return list(db.registrations.aggregate(pipeline))
