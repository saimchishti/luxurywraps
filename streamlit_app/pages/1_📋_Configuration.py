"""Campaign configuration page."""

from __future__ import annotations

import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import streamlit as st

PAGE_DIR = Path(__file__).resolve().parents[1]
if str(PAGE_DIR) not in sys.path:
    sys.path.append(str(PAGE_DIR))

from services.repositories import (  # noqa: E402
    RepositoryError,
    attach_ads,
    campaigns_using_ad,
    create_campaign,
    delete_campaign,
    detach_ads,
    list_ads,
    list_campaigns,
    update_campaign,
)
from utils.auth import do_rerun  # noqa: E402
from utils.constants import BUSINESS_ID_SESSION_KEY, BUSINESS_NAME_SESSION_KEY  # noqa: E402
from utils.filters import use_start_date  # noqa: E402

start_dt = use_start_date()


def _require_business() -> tuple[str, str]:
    business_id = st.session_state.get(BUSINESS_ID_SESSION_KEY)
    if not business_id:
        st.error("Please log in to manage campaigns.")
        st.stop()
    business_name = st.session_state.get(BUSINESS_NAME_SESSION_KEY, business_id)
    return business_id, business_name


def _comma_to_list(value: str) -> List[str]:
    return [part.strip() for part in value.split(",") if part.strip()]


def _is_recent(doc: Dict[str, Any], threshold: datetime) -> bool:
    stamp = doc.get("updated_at") or doc.get("created_at")
    if isinstance(stamp, str):
        try:
            stamp = datetime.fromisoformat(stamp)
        except ValueError:
            stamp = None
    if isinstance(stamp, datetime):
        return stamp >= threshold
    return True


def _render_create_form(ad_options: List[dict], business_id: str) -> None:
    st.subheader("Create Draft Campaign")
    with st.form("create-campaign-form", clear_on_submit=True):
        name = st.text_input("Campaign Name *", placeholder="Q4 Brand Awareness")
        status = st.selectbox("Status", options=["draft", "active", "paused", "completed"], index=0)
        locations = st.text_input("Locations", placeholder="US, CA, UK")
        interests = st.text_input("Interests", placeholder="weddings, events")
        devices = st.text_input("Devices", placeholder="mobile, desktop")
        budget = st.number_input("Daily Budget ($)", min_value=0.0, step=10.0, format="%.2f")
        set_dates = st.checkbox("Set flight dates", value=False)
        date_range: Optional[List[date]] = None
        if set_dates:
            date_range = st.date_input("Flight Dates", value=(date.today(), date.today()))
        selected_ads = st.multiselect(
            "Attach ads from library",
            options=[ad["ad_id"] for ad in ad_options],
            format_func=lambda value: next((ad["title"] for ad in ad_options if ad["ad_id"] == value), value),
        )
        save = st.form_submit_button("Save")
    if save:
        if not name.strip():
            st.error("Campaign name is required.")
            return
        targeting = {
            "locations": _comma_to_list(locations),
            "interests": _comma_to_list(interests),
            "devices": _comma_to_list(devices),
            "budget_daily": budget if budget else None,
        }
        if isinstance(date_range, tuple):
            targeting["start_date"] = date_range[0]
            targeting["end_date"] = date_range[1]
        payload = {
            "name": name,
            "status": status or "draft",
            "ad_ids": selected_ads,
            "targeting": targeting,
        }
        try:
            with st.spinner("Saving campaign..."):
                create_campaign(payload, business_id=business_id)
            st.toast("Campaign created successfully.")
            do_rerun()
        except (RepositoryError, ValueError) as exc:
            st.error(f"Unable to create campaign: {exc}")


def _render_campaigns_list(business_id: str) -> List[dict]:
    st.subheader("Campaigns")
    query = st.text_input("Search by name", placeholder="Search campaigns...")
    status_filter = st.selectbox(
        "Status filter",
        options=[None, "draft", "active", "paused", "completed"],
        format_func=lambda value: "All statuses" if value is None else value.title(),
    )
    response = list_campaigns(
        business_id=business_id,
        q=query or None,
        status=status_filter,
        page_size=100,
    )
    campaigns = response["items"]

    if not campaigns:
        st.info("No saved campaigns yet. Use the form above to create your first campaign.")
        return []

    for campaign in campaigns:
        with st.expander(f"{campaign['name']} ({campaign['status']})", expanded=False):
            st.caption(f"Campaign ID: `{campaign['campaign_id']}`")
            st.caption(f"Attached ads: {len(campaign.get('ad_ids', []))}")
            _render_campaign_update_form(campaign, business_id)
            _render_campaign_actions(campaign, business_id)
    return campaigns


def _render_campaign_update_form(campaign: dict, business_id: str) -> None:
    targeting = campaign.get("targeting", {})
    with st.form(f"update-{campaign['campaign_id']}"):
        name = st.text_input("Name", value=campaign.get("name", ""))
        status = st.selectbox(
            "Status",
            options=["draft", "active", "paused", "completed"],
            index=["draft", "active", "paused", "completed"].index(campaign.get("status", "draft")),
        )
        locations = st.text_input("Locations", value=", ".join(targeting.get("locations", [])))
        interests = st.text_input("Interests", value=", ".join(targeting.get("interests", [])))
        devices = st.text_input("Devices", value=", ".join(targeting.get("devices", [])))
        budget = st.number_input(
            "Daily Budget ($)",
            min_value=0.0,
            step=10.0,
            format="%.2f",
            value=float(targeting.get("budget_daily") or 0),
        )
        start_date = targeting.get("start_date")
        end_date = targeting.get("end_date")
        enable_dates = st.checkbox(
            "Set flight dates",
            value=bool(start_date or end_date),
            key=f"dates-toggle-{campaign['campaign_id']}",
        )
        date_range = None
        if enable_dates:
            default_start = start_date or date.today()
            default_end = end_date or date.today()
            date_range = st.date_input(
                "Flight Dates",
                value=(default_start, default_end),
                key=f"dates-{campaign['campaign_id']}",
            )
        save = st.form_submit_button("Save")
    if save:
        payload = {
            "name": name,
            "status": status,
            "targeting": {
                "locations": _comma_to_list(locations),
                "interests": _comma_to_list(interests),
                "devices": _comma_to_list(devices),
                "budget_daily": budget if budget else None,
            },
        }
        if enable_dates and isinstance(date_range, tuple):
            payload["targeting"]["start_date"] = date_range[0]
            payload["targeting"]["end_date"] = date_range[1]
        else:
            payload["targeting"]["start_date"] = None
            payload["targeting"]["end_date"] = None
        try:
            with st.spinner("Updating campaign..."):
                update_campaign(campaign["campaign_id"], payload, business_id=business_id)
            st.toast("Campaign updated.")
            do_rerun()
        except (RepositoryError, ValueError) as exc:
            st.error(f"Failed to update campaign: {exc}")


def _render_campaign_actions(campaign: dict, business_id: str) -> None:
    cols = st.columns(4)
    for idx, status in enumerate(["draft", "active", "paused", "completed"]):
        if cols[idx].button(
            status.title(),
            key=f"status-{status}-{campaign['campaign_id']}",
            disabled=campaign.get("status") == status,
        ):
            try:
                update_campaign(campaign["campaign_id"], {"status": status}, business_id=business_id)
                st.toast(f"Status updated to {status}.")
                do_rerun()
            except RepositoryError as exc:
                st.error(f"Unable to update status: {exc}")
    if st.button(
        "Delete campaign",
        key=f"delete-{campaign['campaign_id']}",
        type="secondary",
    ):
        try:
            delete_campaign(campaign["campaign_id"], business_id=business_id)
            st.toast("Campaign deleted.")
            do_rerun()
        except RepositoryError as exc:
            st.error(f"Delete failed: {exc}")


def _render_attach_section(ad_options: List[dict], campaigns: List[dict], business_id: str) -> None:
    st.subheader("Manage Campaign Ads")
    if not campaigns:
        st.info("Create a campaign first to attach ads.")
        return
    campaign_lookup = {c["campaign_id"]: c for c in campaigns}
    campaign_choice = st.selectbox(
        "Select campaign",
        options=list(campaign_lookup.keys()),
        format_func=lambda cid: f"{campaign_lookup[cid]['name']} ({cid})",
    )
    selected_campaign = campaign_lookup[campaign_choice]
    available_ads = [ad for ad in ad_options if ad["ad_id"] not in selected_campaign.get("ad_ids", [])]
    current_ads = [ad for ad in ad_options if ad["ad_id"] in selected_campaign.get("ad_ids", [])]

    left, right = st.columns(2)
    with left:
        with st.form(f"attach-{campaign_choice}"):
            ad_ids = st.multiselect(
                "Ads to attach",
                options=[ad["ad_id"] for ad in available_ads],
                format_func=lambda value: next((ad["title"] for ad in ad_options if ad["ad_id"] == value), value),
            )
            save = st.form_submit_button("Save")
        if save:
            try:
                with st.spinner("Attaching ads..."):
                    attach_ads(campaign_choice, ad_ids, business_id=business_id)
                st.toast("Ads attached.")
                do_rerun()
            except (RepositoryError, ValueError) as exc:
                st.error(f"Unable to attach ads: {exc}")

    with right:
        with st.form(f"detach-{campaign_choice}"):
            ad_ids = st.multiselect(
                "Ads to detach",
                options=[ad["ad_id"] for ad in current_ads],
                format_func=lambda value: next((ad["title"] for ad in ad_options if ad["ad_id"] == value), value),
            )
            save = st.form_submit_button("Save")
        if save:
            try:
                with st.spinner("Detaching ads..."):
                    detach_ads(campaign_choice, ad_ids, business_id=business_id)
                st.toast("Ads detached.")
                do_rerun()
            except (RepositoryError, ValueError) as exc:
                st.error(f"Unable to detach ads: {exc}")

    linked_campaigns = campaigns_using_ad(selected_campaign["ad_ids"][0], business_id) if selected_campaign.get("ad_ids") else []
    if linked_campaigns:
        st.caption(
            "This campaign currently shares ads with: "
            + ", ".join(f"{row['name']} ({row['campaign_id']})" for row in linked_campaigns if row["campaign_id"] != campaign_choice)
        )


def main() -> None:
    business_id, business_name = _require_business()
    st.title(f"Configuration - {business_name}")

    ads_response = list_ads(
        business_id=business_id,
        page_size=100,
    )
    ad_options = [
        ad for ad in ads_response["items"]
        if _is_recent(ad, start_dt)
    ]
    _render_create_form(ad_options, business_id)
    campaigns = _render_campaigns_list(business_id)
    _render_attach_section(ad_options, campaigns, business_id)


if __name__ == "__main__":
    main()
