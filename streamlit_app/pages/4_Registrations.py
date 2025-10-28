"""Registrations explorer and uploader."""

from __future__ import annotations

import json
from datetime import date, datetime, time, timezone
from typing import Any, Dict, List

import pandas as pd
import streamlit as st

from streamlit_app.services.repositories import (
    list_registrations_with_names,
    read_registration,
    update_registration,
    delete_registration,
    list_campaigns,
    list_ads,
    list_registrations,
    export_registrations_csv,
    RepositoryError,
    create_registration,
)

from streamlit_app.utils.auth import do_rerun
from streamlit_app.utils.constants import (
    BUSINESS_ID_SESSION_KEY,
    BUSINESS_NAME_SESSION_KEY,
)
from streamlit_app.utils.filters import use_start_date
from streamlit_app.utils.formatting import format_currency, format_datetime

# --- Pagination helper (registrations page only)
def _render_pagination(current_page: int, total: int, page_size: int) -> int:
    import math

    total_pages = max(1, math.ceil((total or 0) / max(1, page_size or 1)))
    col_prev, col_center, col_next = st.columns([1, 2, 1])

    with col_prev:
        if st.button("Previous", disabled=current_page <= 1, key="reg_prev_btn"):
            current_page = max(1, current_page - 1)

    with col_center:
        st.write(f"Page {current_page} / {total_pages}")

    with col_next:
        last_page = current_page >= total_pages
        if st.button("Next", disabled=last_page, key="reg_next_btn"):
            current_page = min(total_pages, current_page + 1)

    return current_page

# Global start-date filter (left rail)
start_dt = use_start_date()
FILTER_STATE_KEY = "registration_filters"


# ------------------------------
# Helpers
# ------------------------------
def _require_business() -> tuple[str, str]:
    business_id = st.session_state.get(BUSINESS_ID_SESSION_KEY)
    if not business_id:
        st.error("Please log in to manage registrations.")
        st.stop()
    business_name = st.session_state.get(BUSINESS_NAME_SESSION_KEY, business_id)
    return business_id, business_name


def _fetch_options(business_id: str, start_dt: datetime) -> tuple[List[dict], List[dict]]:
    campaigns = list_campaigns(business_id=business_id, limit=1000)
    ads_response = list_ads(business_id=business_id, page_size=100)["items"]
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


# ------------------------------
# Create a single registration
# ------------------------------
def _render_create_registration(
    business_id: str, campaigns: List[dict], ads: List[dict]
) -> None:
    st.subheader("Add Campaign Registration")
    if not campaigns:
        st.info("Create a campaign before adding registrations.")
        return

    campaign_lookup = {c["campaign_id"]: c for c in campaigns if c.get("campaign_id")}
    ad_lookup = {a["ad_id"]: a for a in ads if a.get("ad_id")}

    with st.form("create-registration"):
        campaign_id = st.selectbox(
            "Campaign *",
            options=list(campaign_lookup.keys()),
            format_func=lambda cid: f"{campaign_lookup[cid]['name']} ({cid})",
        )
        ad_id = st.selectbox(
            "Ad (optional)",
            options=[None] + list(ad_lookup.keys()),
            format_func=lambda aid: "None"
            if aid is None
            else f"{ad_lookup[aid].get('title','(untitled)')} ({aid})",
        )
        source = st.text_input("Source", value="organic")
        cost = st.number_input("Cost", min_value=0.0, value=0.0, step=1.0, format="%.2f")
        date_value = st.date_input("Date", value=datetime.utcnow().date())
        time_value = st.time_input("Time", value=datetime.utcnow().time())
        messages = st.number_input("Messages", min_value=0, value=0, step=1)
        spent = st.number_input(
            "Spent", min_value=0.0, value=float(cost), step=1.0, format="%.2f"
        )
        reach = st.number_input("Reach", min_value=0, value=0, step=1)
        impressions = st.number_input("Impressions", min_value=0, value=0, step=1)
        clicks = st.number_input("Clicks", min_value=0, value=0, step=1)
        meta_notes = st.text_area(
            "Meta (JSON optional)", placeholder='{"note": "Wedding inquiry"}'
        )
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


# ------------------------------
# Filters
# ------------------------------
def _render_filters(campaigns: List[dict], ads: List[dict]) -> Dict[str, list]:
    st.subheader("Filters")
    filters = _init_filter_state()

    campaign_choices = st.multiselect(
        "Campaigns",
        options=[c["campaign_id"] for c in campaigns if c.get("campaign_id")],
        default=filters["campaign_ids"],
        format_func=lambda cid: next(
            (c["name"] for c in campaigns if c.get("campaign_id") == cid), cid
        ),
    )
    ad_choices = st.multiselect(
        "Ads",
        options=[a["ad_id"] for a in ads if a.get("ad_id")],
        default=filters["ad_ids"],
        format_func=lambda aid: next(
            (a.get("title", "(untitled)") for a in ads if a.get("ad_id") == aid), aid
        ),
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


# ------------------------------
# Table + single-row editor
# ------------------------------
def _render_table(payload: dict, filters: Dict[str, list], campaigns: List[dict]) -> pd.DataFrame:
    """Render the current page and return the DataFrame that is shown."""
    items = payload["items"]
    if not items:
        st.info("No registrations for the selected filters yet.")
        return pd.DataFrame()

    df = pd.DataFrame(items)

    # map campaign_id -> campaign name for display
    cmap = {
        c.get("campaign_id"): (c.get("name") or "(unnamed)")
        for c in campaigns
        if c.get("campaign_id")
    }
    df["campaign"] = df.get("campaign_id").map(lambda x: cmap.get(x, "(unknown)"))

    # Format some columns
    if "timestamp" in df.columns:
        df["timestamp"] = df["timestamp"].apply(format_datetime)
    if "cost" in df.columns:
        df["cost"] = df["cost"].apply(format_currency)
    if "spent" in df.columns:
        df["spent"] = df["spent"].apply(format_currency)

    # Columns to show (campaign name instead of campaign_id)
    desired = [
        "registration_id",
        "timestamp",
        "campaign",   # human friendly
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
    show = df[desired]

    st.dataframe(show, use_container_width=True, hide_index=True)
    st.caption(f"Showing {len(show)} of {payload['total']} registrations.")
    return df


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


# ------------------------------
# Page entrypoint
# ------------------------------
def main() -> None:
    business_id, business_name = _require_business()
    st.title(f"Registrations - {business_name}")

    # options used by create, filters, and editor
    campaigns, ads = _fetch_options(business_id, start_dt)
    ad_title_by_id = {a.get("ad_id"): (a.get("title") or "(untitled)") for a in ads}

    # create
    _render_create_registration(business_id, campaigns, ads)

    # filters + fetch the current page
    filters = _render_filters(campaigns, ads)

    # current page
    current_page = int(st.session_state.get("registrations_page", 1))

    # fetch data (existing list_registrations call)
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

    # table display
    _render_table(response, filters, campaigns)

    # robust totals (avoid KeyError if API shape changes)
    total = int(response.get("total", 0)) if isinstance(response, dict) else 0
    page_size = int(response.get("page_size", 25)) if isinstance(response, dict) else 25

    # draw controls & store back
    current_page = _render_pagination(current_page, total, page_size)
    st.session_state["registrations_page"] = current_page

    # ---- Edit a registration (friendly names)
    st.subheader("Edit a registration")

    # Pull recent rows with campaign names for readable labels.
    # Keep the same start date filter you already use elsewhere.
    rows_for_picker = list_registrations_with_names(
        business_id=business_id,
        query={"timestamp": {"$gte": start_dt}},
        skip=0,
        limit=500,
        sort=[("timestamp", -1)],
    )

    def _row_label(d: dict) -> str:
        ts = d.get("timestamp")
        try:
            ts_str = format_datetime(ts) if ts else ""
        except Exception:
            ts_str = str(ts or "")
        camp = d.get("campaign_name") or "(unknown campaign)"
        adt = ad_title_by_id.get(d.get("ad_id"), "—")
        src = d.get("source") or "—"
        rid = d.get("registration_id", "")
        rid_short = f" · {rid[-6:]}" if rid else ""
        return f"{camp} — {adt} — {src} — {ts_str}{rid_short}"

    selected_row = st.selectbox(
        "Pick a registration to edit",
        options=[None] + rows_for_picker,
        format_func=lambda d: "(select)" if d is None else _row_label(d),
        key="edit_reg_select",
    )

    if selected_row:
        reg_id = selected_row["registration_id"]
        row = read_registration(business_id=business_id, registration_id=reg_id)
        if not row:
            st.warning("Registration not found.")
        else:
            with st.form("edit-registration"):
                # prefill fields from row; keep existing field names/validators
                col1, col2 = st.columns(2)
                with col1:
                    source = st.text_input("Source", value=row.get("source") or "")
                    messages = st.number_input(
                        "Messages", min_value=0, value=int(row.get("messages") or 0), step=1
                    )
                    spent = st.number_input(
                        "Spent",
                        min_value=0.0,
                        value=float(row.get("spent") or 0.0),
                        step=1.0,
                        format="%.2f",
                    )
                    reach = st.number_input(
                        "Reach", min_value=0, value=int(row.get("reach") or 0), step=1
                    )
                with col2:
                    impressions = st.number_input(
                        "Impressions", min_value=0, value=int(row.get("impressions") or 0), step=1
                    )
                    clicks = st.number_input(
                        "Clicks", min_value=0, value=int(row.get("clicks") or 0), step=1
                    )
                    cost = st.number_input(
                        "Cost",
                        min_value=0.0,
                        value=float(row.get("cost") or 0.0),
                        step=1.0,
                        format="%.2f",
                    )
                    user_id = st.text_input("User ID", value=str(row.get("user_id") or ""))

                # timestamp split into date + time for convenient editing
                ts_val = row.get("timestamp")
                if isinstance(ts_val, str):
                    try:
                        from datetime import datetime as _dt

                        ts_val = _dt.fromisoformat(ts_val)
                    except Exception:
                        ts_val = None
                dt_default = ts_val.date() if ts_val else datetime.utcnow().date()
                tm_default = (
                    ts_val.time().replace(microsecond=0)
                    if ts_val
                    else datetime.utcnow().time().replace(microsecond=0)
                )
                dt_part = st.date_input("Date", value=dt_default)
                tm_part = st.time_input("Time", value=tm_default)

                # optional meta field
                raw_meta = row.get("meta") or {}
                meta_text = st.text_area(
                    "Meta (JSON)", value=json.dumps(raw_meta, ensure_ascii=False, indent=2)
                )

                save = st.form_submit_button("Save changes")

            if save:
                # parse meta JSON
                meta_payload = {}
                if meta_text.strip():
                    try:
                        meta_payload = json.loads(meta_text)
                    except Exception:
                        st.error("Meta must be valid JSON.")
                        st.stop()

                ts_combined = datetime.combine(dt_part, tm_part)
                patch = {
                    "source": source or None,
                    "messages": int(messages or 0),
                    "spent": float(spent or 0.0),
                    "reach": int(reach or 0),
                    "impressions": int(impressions or 0),
                    "clicks": int(clicks or 0),
                    "cost": float(cost or 0.0),
                    "user_id": int(user_id) if str(user_id).isdigit() else (user_id or None),
                    "timestamp": ts_combined.isoformat(),
                    "meta": meta_payload,
                }
                try:
                    update_registration(business_id=business_id, registration_id=reg_id, patch=patch)
                    st.toast("Registration updated.")
                    do_rerun()
                except RepositoryError as exc:
                    st.error(f"Failed to update: {exc}")

    # export + upload
    _render_export(filters, business_id, start_dt)
    _render_upload(business_id)


if __name__ == "__main__":
    main()
