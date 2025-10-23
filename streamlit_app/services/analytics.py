"""Aggregation utilities for analytics dashboards."""

from __future__ import annotations

from datetime import datetime
from typing import Dict, Iterable, List, Optional

from pymongo.database import Database

from services.db import get_db


def _match_base(
    business_id: Optional[str],
    dt_from: Optional[datetime],
    campaign_ids: Optional[Iterable[str]] = None,
    ad_ids: Optional[Iterable[str]] = None,
    sources: Optional[Iterable[str]] = None,
) -> Dict[str, object]:
    match: Dict[str, object] = {}
    if business_id:
        match["business_id"] = business_id
    if dt_from:
        match["timestamp"] = {"$gte": dt_from}
    if campaign_ids:
        match["campaign_id"] = {"$in": list(campaign_ids)}
    if ad_ids:
        match["ad_id"] = {"$in": list(ad_ids)}
    if sources:
        match["source"] = {"$in": list(sources)}
    return match


def kpis(
    db: Optional[Database],
    dt_from: Optional[datetime],
    business_id: str,
    campaign_ids: Optional[Iterable[str]] = None,
    ad_ids: Optional[Iterable[str]] = None,
    sources: Optional[Iterable[str]] = None,
) -> Dict[str, float]:
    db = db or get_db()
    match = _match_base(business_id, dt_from, campaign_ids, ad_ids, sources)
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
    business_id: str,
    campaign_ids: Optional[Iterable[str]] = None,
    ad_ids: Optional[Iterable[str]] = None,
    sources: Optional[Iterable[str]] = None,
) -> List[Dict[str, object]]:
    db = db or get_db()
    match = _match_base(business_id, dt_from, campaign_ids, ad_ids, sources)
    pipeline = [
        {"$match": match},
        {
            "$group": {
                "_id": {
                    "$dateTrunc": {
                        "date": "$timestamp",
                        "unit": "day",
                        "timezone": "UTC",
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
            }
        },
    ]
    return list(db.registrations.aggregate(pipeline))


def campaign_rollup(
    db: Optional[Database],
    dt_from: Optional[datetime],
    business_id: str,
    campaign_ids: Optional[Iterable[str]] = None,
    ad_ids: Optional[Iterable[str]] = None,
    sources: Optional[Iterable[str]] = None,
) -> List[Dict[str, object]]:
    db = db or get_db()
    match = _match_base(business_id, dt_from, campaign_ids, ad_ids, sources)
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
    *,
    db: Optional[Database] = None,
    dt_from: datetime,
    business_id: str,
    campaign_id: Optional[str] = None,
    ad_ids: Optional[Iterable[str]] = None,
) -> List[Dict[str, object]]:
    db = db or get_db()

    match: Dict[str, object] = {
        "timestamp": {"$gte": dt_from},
        "business_id": business_id,
    }
    if campaign_id:
        match["campaign_id"] = campaign_id
    if ad_ids:
        match["ad_id"] = {"$in": list(ad_ids)}

    pipeline = [
        {"$match": match},
        {
            "$group": {
                "_id": "$ad_id",
                "registrations": {"$sum": 1},
                "spent": {"$sum": {"$ifNull": ["$spent", 0]}},
                "impressions": {"$sum": {"$ifNull": ["$impressions", 0]}},
                "clicks": {"$sum": {"$ifNull": ["$clicks", 0]}},
                "messages": {"$sum": {"$ifNull": ["$messages", 0]}},
                "reach": {"$sum": {"$ifNull": ["$reach", 0]}},
            }
        },
        {
            "$lookup": {
                "from": "ads",
                "localField": "_id",
                "foreignField": "ad_id",
                "as": "ad",
            }
        },
        {"$unwind": {"path": "$ad", "preserveNullAndEmptyArrays": True}},
        {
            "$project": {
                "_id": 0,
                "ad_id": "$_id",
                "title": "$ad.title",
                "status": "$ad.status",
                "tags": "$ad.tags",
                "registrations": 1,
                "spent": 1,
                "impressions": 1,
                "clicks": 1,
                "messages": 1,
                "reach": 1,
                "ctr": {
                    "$cond": [
                        {"$gt": ["$impressions", 0]},
                        {"$divide": ["$clicks", "$impressions"]},
                        None,
                    ]
                },
                "cpr": {
                    "$cond": [
                        {"$gt": ["$registrations", 0]},
                        {"$divide": ["$spent", "$registrations"]},
                        None,
                    ]
                },
            }
        },
        {"$sort": {"registrations": -1}},
    ]
    return list(db.registrations.aggregate(pipeline))


def _safe_div(a, b) -> float:
    try:
        return (float(a) / float(b)) if b else 0.0
    except Exception:
        return 0.0


def _campaign_ids_for_business(db, business_id: str) -> List[str]:
    """Return campaign IDs scoped to a business."""
    return [
        doc["campaign_id"]
        for doc in db.campaigns.find(
            {"business_id": business_id}, {"_id": 0, "campaign_id": 1}
        )
        if doc.get("campaign_id")
    ]


def kpis_full(
    db,
    *,
    dt_from: datetime,
    dt_to: datetime,
    business_id: str,
) -> Dict[str, float]:
    match = {
        "business_id": business_id,
        "timestamp": {"$gte": dt_from, "$lte": dt_to},
    }
    pipe = [
        {"$match": match},
        {
            "$group": {
                "_id": None,
                "messages": {"$sum": {"$ifNull": ["$messages", 0]}},
                "spent": {"$sum": {"$ifNull": ["$spent", 0]}},
                "reach": {"$sum": {"$ifNull": ["$reach", 0]}},
                "impressions": {"$sum": {"$ifNull": ["$impressions", 0]}},
                "clicks": {"$sum": {"$ifNull": ["$clicks", 0]}},
                "customers_set": {"$addToSet": "$user_id"},
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
                "customers": {
                    "$size": {
                        "$filter": {
                            "input": "$customers_set",
                            "as": "u",
                            "cond": {"$ne": ["$$u", None]},
                        }
                    }
                },
            }
        },
    ]
    agg = list(db.registrations.aggregate(pipe))
    if not agg:
        return {
            k: 0
            for k in [
                "messages",
                "spent",
                "reach",
                "impressions",
                "clicks",
                "customers",
                "ctr_pct",
                "conv_pct",
                "frequency",
                "engagement_pct",
                "cpm",
                "cpc",
                "cost_per_msg",
                "cac",
            ]
        }

    a = agg[0]
    messages = a["messages"]
    spent = a["spent"]
    reach = a["reach"]
    impr = a["impressions"]
    clicks = a["clicks"]
    customers = a["customers"]

    return {
        "messages": messages,
        "spent": spent,
        "reach": reach,
        "impressions": impr,
        "clicks": clicks,
        "customers": customers,
        "ctr_pct": _safe_div(clicks, impr) * 100.0,
        "conv_pct": _safe_div(customers, messages) * 100.0,
        "frequency": _safe_div(impr, reach),
        "engagement_pct": _safe_div(clicks, impr) * 100.0,
        "cpm": _safe_div(spent, (impr / 1000.0) if impr else 0.0),
        "cpc": _safe_div(spent, clicks),
        "cost_per_msg": _safe_div(spent, messages),
        "cac": _safe_div(spent, customers),
    }

def clicks_impressions_by_ad_simple(
    db,
    *,
    dt_from: datetime,
    dt_to: datetime,
    business_id: str,
    limit: int = 10,
):
    """
    Returns [{ad_id, title, clicks, impressions}] for top ads since dt_from.
    """
    campaign_ids = _campaign_ids_for_business(db, business_id)
    if not campaign_ids:
        return []
    match = {
        "business_id": business_id,
        "timestamp": {"$gte": dt_from, "$lte": dt_to},
        "campaign_id": {"$in": campaign_ids},
        "ad_id": {"$ne": None},
    }
    pipeline = [
        {"$match": match},
        {
            "$group": {
                "_id": "$ad_id",
                "impressions": {"$sum": {"$ifNull": ["$impressions", 0]}},
                "clicks": {"$sum": {"$ifNull": ["$clicks", 0]}},
            }
        },
        {
            "$lookup": {
                "from": "ads",
                "localField": "_id",
                "foreignField": "ad_id",
                "as": "ad",
            }
        },
        {"$unwind": {"path": "$ad", "preserveNullAndEmptyArrays": True}},
        {
            "$project": {
                "_id": 0,
                "ad_id": "$_id",
                "title": {
                    "$cond": [
                        {
                            "$and": [
                                {"$ne": ["$ad.title", None]},
                                {"$ne": ["$ad.title", ""]},
                            ]
                        },
                        "$ad.title",
                        {"$ifNull": ["$_id", "(Unlabeled)"]},
                    ]
                },
                "clicks": 1,
                "impressions": 1,
            }
        },
        {"$match": {"impressions": {"$gt": 0}}},
        {"$sort": {"impressions": -1}},
        {"$limit": limit},
    ]
    return list(db.registrations.aggregate(pipeline))


def ad_performance_table_simple(
    db,
    *,
    dt_from: datetime,
    dt_to: datetime,
    business_id: str,
):
    """
    Returns rows per ad with core fields for a simple table.
    """
    campaign_ids = _campaign_ids_for_business(db, business_id)
    if not campaign_ids:
        return []
    match = {
        "business_id": business_id,
        "timestamp": {"$gte": dt_from, "$lte": dt_to},
        "campaign_id": {"$in": campaign_ids},
        "ad_id": {"$ne": None},
    }
    pipeline = [
        {"$match": match},
        {
            "$group": {
                "_id": "$ad_id",
                "spent": {"$sum": {"$ifNull": ["$spent", 0]}},
                "messages": {"$sum": {"$ifNull": ["$messages", 0]}},
                "impressions": {"$sum": {"$ifNull": ["$impressions", 0]}},
                "clicks": {"$sum": {"$ifNull": ["$clicks", 0]}},
                "reach": {"$sum": {"$ifNull": ["$reach", 0]}},
                "customers_set": {"$addToSet": "$user_id"},
            }
        },
        {
            "$project": {
                "_id": 1,
                "spent": 1,
                "messages": 1,
                "impressions": 1,
                "clicks": 1,
                "reach": 1,
                "customers": {
                    "$size": {
                        "$filter": {
                            "input": "$customers_set",
                            "as": "u",
                            "cond": {"$ne": ["$$u", None]},
                        }
                    }
                },
            }
        },
        {
            "$lookup": {
                "from": "ads",
                "localField": "_id",
                "foreignField": "ad_id",
                "as": "ad",
            }
        },
        {"$unwind": {"path": "$ad", "preserveNullAndEmptyArrays": True}},
        {
            "$project": {
                "_id": 0,
                "ad_id": "$_id",
                "ad_name": {
                    "$cond": [
                        {
                            "$and": [
                                {"$ne": ["$ad.title", None]},
                                {"$ne": ["$ad.title", ""]},
                            ]
                        },
                        "$ad.title",
                        {"$ifNull": ["$_id", "(Unlabeled)"]},
                    ]
                },
                "spent": 1,
                "messages": 1,
                "impressions": 1,
                "clicks": 1,
                "reach": 1,
                "customers": 1,
            }
        },
        {
            "$match": {
                "$or": [
                    {"spent": {"$gt": 0}},
                    {"messages": {"$gt": 0}},
                    {"impressions": {"$gt": 0}},
                    {"clicks": {"$gt": 0}},
                    {"reach": {"$gt": 0}},
                ]
            }
        },
        {"$sort": {"spent": -1}},
    ]
    return list(db.registrations.aggregate(pipeline))
