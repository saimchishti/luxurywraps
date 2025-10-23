"""Registrations explorer and uploader."""

from __future__ import annotations

import json
import sys
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import streamlit as st

PAGE_DIR = Path(__file__).resolve().parents[1]
if str(PAGE_DIR) not in sys.path:
    sys.path.append(str(PAGE_DIR))

from services.repositories import (  # noqa: E402
    RepositoryError,
    create_registration,
    export_registrations_csv,
    list_ads,
    list_campaigns,
    list_registrations,
)
from utils.auth import do_rerun  # noqa: E402
from utils.constants import BUSINESS_ID_SESSION_KEY, BUSINESS_NAME_SESSION_KEY  # noqa: E402
from utils.filters import use_start_date  # noqa: E402
from utils.formatting import format_currency, format_datetime  # noqa: E402

start_dt = use_start_date()

FILTER_STATE_KEY = "registration_filters"


def _require_business() -> tuple[str, str]:
    business_id = st.session_state.get(BUSINESS_ID_SESSION_KEY)
    if not business_id:
        st.error("Please log in to manage registrations.")
        st.stop()
    business_name = st.session_state.get(BUSINESS_NAME_SESSION_KEY, business_id)
    return business_id, business_name


def _fetch_options(business_id: str, start_dt: datetime) -> tuple[List[dict], List[dict]]:
    campaigns = list_campaigns(
        business_id=business_id,
        page_size=100,
    )["items"]
    ads_response = list_ads(
        business_id=business_id,
        page_size=100,
    )["items"]
    ads = [ad for ad in ads_response if _is_recent_doc(ad, start_dt)]
    return campaigns, ads


def _init_filter_state() -> Dict[str, list]:
    if FILTER_STATE_KEY not in st.session_state:
        st.session_state[FILTER_STATE_KEY] = {
            "campaign_ids": [],
            "ad_ids": [],
            "sources": [],
        }
    return st.session_state[FILTER_STATE_KEY]


def _is_recent_doc(doc: Dict[str, Any], threshold: datetime) -> bool:
    stamp = doc.get("timestamp") or doc.get("updated_at") or doc.get("created_at")
    if isinstance(stamp, str):
        try:
            stamp = datetime.fromisoformat(stamp)
        except ValueError:
            stamp = None
    if isinstance(stamp, datetime):
        return stamp >= threshold
    return True


def _render_create_registration(business_id: str, campaigns: List[dict], ads: List[dict]) -> None:
    st.subheader("Add Campaign Registration")
    if not campaigns:
        st.info("Create a campaign before adding registrations.")
        return

    campaign_lookup = {c["campaign_id"]: c for c in campaigns}
    ad_lookup = {a["ad_id"]: a for a in ads}

    with st.form("create-registration"):
        campaign_id = st.selectbox(
            "Campaign *",
            options=list(campaign_lookup.keys()),
            format_func=lambda cid: f"{campaign_lookup[cid]['name']} ({cid})",
        )
        ad_id = st.selectbox(
            "Ad (optional)",
            options=[None] + list(ad_lookup.keys()),
            format_func=lambda aid: "None" if aid is None else f"{ad_lookup[aid]['title']} ({aid})",
        )
        source = st.text_input("Source", value="organic")
        cost = st.number_input("Cost", min_value=0.0, value=0.0, step=1.0, format="%.2f")
        date_value = st.date_input("Date", value=datetime.utcnow().date())
        time_value = st.time_input("Time", value=datetime.utcnow().time())
        messages = st.number_input("Messages", min_value=0, value=0, step=1)
        spent = st.number_input("Spent", min_value=0.0, value=float(cost), step=1.0, format="%.2f")
        reach = st.number_input("Reach", min_value=0, value=0, step=1)
        impressions = st.number_input("Impressions", min_value=0, value=0, step=1)
        clicks = st.number_input("Clicks", min_value=0, value=0, step=1)
        meta_notes = st.text_area("Meta (JSON optional)", placeholder='{"note": "Wedding inquiry"}')
        save = st.form_submit_button("Save")
    if save:
        meta_payload = {}
        if meta_notes.strip():
            try:
                meta_payload = json.loads(meta_notes)
            except Exception:
                st.error("Meta must be valid JSON if provided.")
                return
        timestamp = datetime.combine(date_value, time_value)
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        payload = {
            "campaign_id": campaign_id,
            "ad_id": ad_id or None,
            "source": source or "organic",
            "cost": float(cost),
            "timestamp": timestamp,
            "meta": meta_payload,
            "messages": int(messages),
            "spent": float(spent),
            "reach": int(reach) if reach else 0,
            "impressions": int(impressions) if impressions else 0,
            "clicks": int(clicks) if clicks else 0,
        }
        try:
            create_registration(payload, business_id=business_id)
            st.toast("Registration saved.")
            do_rerun()
        except RepositoryError as exc:
            st.error(f"Failed to save registration: {exc}")


def _render_filters(campaigns: List[dict], ads: List[dict]) -> Dict[str, list]:
    st.subheader("Filters")
    filters = _init_filter_state()
    campaign_choices = st.multiselect(
        "Campaigns",
        options=[c["campaign_id"] for c in campaigns],
        default=filters["campaign_ids"],
        format_func=lambda cid: next((c["name"] for c in campaigns if c["campaign_id"] == cid), cid),
    )
    ad_choices = st.multiselect(
        "Ads",
        options=[a["ad_id"] for a in ads],
        default=filters["ad_ids"],
        format_func=lambda aid: next((a["title"] for a in ads if a["ad_id"] == aid), aid),
    )
    sources = st.multiselect(
        "Sources",
        options=["facebook", "google", "organic", "email", "referral"],
        default=filters["sources"],
    )
    filters["campaign_ids"] = campaign_choices
    filters["ad_ids"] = ad_choices
    filters["sources"] = sources
    return filters


def _render_table(payload: dict, filters: Dict[str, list]) -> None:
    items = payload["items"]
    if not items:
        st.info("No registrations for the selected filters yet.")
        return
    df = pd.DataFrame(items)
    if "timestamp" in df.columns:
        df["timestamp"] = df["timestamp"].apply(format_datetime)
    if "cost" in df.columns:
        df["cost"] = df["cost"].apply(format_currency)
    if "spent" in df.columns:
        df["spent"] = df["spent"].apply(format_currency)
    desired = [
        "registration_id",
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
        "cost",
        "meta",
    ]
    for col in desired:
        if col not in df.columns:
            df[col] = None
    df = df[desired]
    st.dataframe(
        df,
        use_container_width=True,
    )
    st.caption(f"Showing {len(df)} of {payload['total']} registrations.")


def _render_pagination(current_page: int, total: int, page_size: int) -> int:
    cols = st.columns(3)
    if cols[0].button("Previous", disabled=current_page <= 1):
        current_page -= 1
    cols[1].write(f"Page {current_page}")
    if cols[2].button("Next", disabled=current_page * page_size >= total):
        current_page += 1
    return current_page


def _render_export(filters: Dict[str, list], business_id: str, start_dt: datetime) -> None:
    query = {
        "campaign_id": {"$in": filters["campaign_ids"]} if filters["campaign_ids"] else None,
        "ad_id": {"$in": filters["ad_ids"]} if filters["ad_ids"] else None,
        "source": {"$in": filters["sources"]} if filters["sources"] else None,
        "timestamp": {"$gte": start_dt},
    }
    buffer = export_registrations_csv(query, business_id=business_id)
    st.download_button(
        "Download CSV",
        data=buffer,
        file_name="registrations.csv",
        mime="text/csv",
    )


def _render_upload(business_id: str) -> None:
    st.subheader("Upload Registrations CSV")
    uploaded = st.file_uploader("Upload CSV to upsert registrations", type="csv")
    if not uploaded:
        st.caption(
            "CSV columns: campaign_id, ad_id, source, cost, spent, messages, reach, impressions, clicks, timestamp, user_id, meta"
        )
        return
    try:
        df = pd.read_csv(uploaded)
    except Exception as exc:
        st.error(f"Failed to parse CSV: {exc}")
        return

    successes, failures = 0, 0
    for _, row in df.iterrows():
        timestamp = pd.to_datetime(row.get("timestamp"), utc=True, errors="coerce")
        if pd.isna(timestamp):
            failures += 1
            continue
        try:
            messages = int(row.get("messages", 0) or 0)
            reach = int(row.get("reach", 0) or 0)
            impressions = int(row.get("impressions", 0) or 0)
            clicks = int(row.get("clicks", 0) or 0)
        except Exception:
            failures += 1
            continue
        meta_value = row.get("meta")
        meta_data = {}
        if isinstance(meta_value, str) and meta_value.strip():
            try:
                meta_data = json.loads(meta_value)
            except Exception:
                meta_data = {}
        payload = {
            "campaign_id": row.get("campaign_id"),
            "ad_id": row.get("ad_id") or None,
            "source": row.get("source") or "organic",
            "cost": float(row.get("cost", 0)),
            "spent": float(row.get("spent", row.get("cost", 0)) or 0),
            "messages": messages,
            "reach": reach,
            "impressions": impressions,
            "clicks": clicks,
            "timestamp": timestamp.to_pydatetime(),
            "user_id": row.get("user_id") or None,
            "meta": meta_data,
        }
        try:
            create_registration(payload, business_id=business_id)
            successes += 1
        except RepositoryError:
            failures += 1
    if successes:
        st.toast(f"Imported {successes} registrations.")
    if failures:
        st.warning(f"{failures} rows failed validation.")
    do_rerun()


def main() -> None:
    business_id, business_name = _require_business()
    st.title(f"Registrations - {business_name}")
    campaigns, ads = _fetch_options(business_id, start_dt)
    _render_create_registration(business_id, campaigns, ads)

    filters = _render_filters(campaigns, ads)
    current_page = st.session_state.get("registrations_page", 1)
    response = list_registrations(
        business_id=business_id,
        campaign_ids=filters["campaign_ids"],
        ad_ids=filters["ad_ids"],
        sources=filters["sources"],
        dt_from=start_dt,
        dt_to=None,
        page=current_page,
        page_size=25,
    )
    st.session_state["registrations_page"] = _render_pagination(
        current_page,
        response["total"],
        response["page_size"],
    )
    _render_table(response, filters)
    _render_export(filters, business_id, start_dt)
    _render_upload(business_id)


if __name__ == "__main__":
    main()
