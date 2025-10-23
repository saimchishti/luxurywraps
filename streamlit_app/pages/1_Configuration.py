"""Campaign configuration page."""

from __future__ import annotations

from datetime import datetime, date as _date
from typing import Any, Dict, List

import pandas as pd
import streamlit as st

from streamlit_app.services.db import get_db  # noqa: E402
from streamlit_app.services.repositories import (  # noqa: E402
    RepositoryError,
    attach_ads,
    campaigns_using_ad,
    cleanup_orphans,
    create_or_update_campaign,
    delete_all_campaigns,
    delete_campaign,
    detach_ads,
    backfill_campaign_ids,
    list_ads,
    list_campaigns,
)
from streamlit_app.utils.auth import do_rerun  # noqa: E402
from streamlit_app.utils.constants import BUSINESS_ID_SESSION_KEY, BUSINESS_NAME_SESSION_KEY  # noqa: E402
from streamlit_app.utils.filters import use_start_date  # noqa: E402

start_dt = use_start_date()


def _require_business() -> tuple[str, str]:
    business_id = st.session_state.get(BUSINESS_ID_SESSION_KEY)
    if not business_id:
        st.error("Please log in to manage campaigns.")
        st.stop()
    business_name = st.session_state.get(BUSINESS_NAME_SESSION_KEY, business_id)
    return business_id, business_name


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


def _render_manage_campaigns(db, business_id: str) -> List[Dict[str, Any]]:
    st.header("Manage Campaigns")

    items = list_campaigns(db=db, business_id=business_id, limit=1000)
    REQUIRED = ["campaign_id", "name", "start_date", "status"]
    df = pd.DataFrame(items) if items else pd.DataFrame(columns=REQUIRED)
    for column in REQUIRED:
        if column not in df.columns:
            df[column] = None
    if df["campaign_id"].isna().any() and "_id" in df.columns:
        df["campaign_id"] = df["campaign_id"].fillna(df["_id"].astype(str))
    df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce").dt.date
    df["status"] = df["status"].fillna("active")

    show = df[["campaign_id", "name", "start_date", "status"]].rename(
        columns={
            "campaign_id": "ID",
            "name": "Name",
            "start_date": "Start",
            "status": "Status",
        }
    )
    st.dataframe(show, use_container_width=True, hide_index=True)

    st.divider()
    st.subheader("Create / Edit Campaign")

    id_to_name = {
        (row["campaign_id"] or ""): (row["name"] or "(unnamed)")
        for _, row in df.iterrows()
        if pd.notna(row["campaign_id"]) and str(row["campaign_id"]).strip() != ""
    }
    choice = st.selectbox(
        "Select campaign to edit",
        ["➕ New"] + [f"{value} — {key}" for key, value in id_to_name.items()],
        index=0,
    )
    selected_id = None if choice == "➕ New" else choice.split(" — ")[-1]

    if "camp_form_nonce" not in st.session_state:
        st.session_state.camp_form_nonce = 0

    existing: Dict[str, Any] = {}
    if selected_id:
        match = df.loc[df["campaign_id"] == selected_id]
        if not match.empty:
            row = match.iloc[0]
            existing = {
                "campaign_id": row.get("campaign_id"),
                "name": row.get("name") or "",
                "start_date": row.get("start_date") if pd.notna(row.get("start_date")) else _date.today(),
                "status": row.get("status") or "active",
            }

    with st.form(f"campaign_form_{st.session_state.camp_form_nonce}", clear_on_submit=True):
        name = st.text_input("Name", value=existing.get("name", ""))
        start_default = existing.get("start_date") or _date.today()
        status_options = ["active", "paused"]
        current_status = existing.get("status", "active")
        if current_status not in status_options:
            current_status = "active"
        status = st.selectbox("Status", status_options, index=status_options.index(current_status))
        start = st.date_input("Start date", value=start_default)
        submit = st.form_submit_button("Save campaign")

    if submit:
        errs: List[str] = []
        nm = (name or "").strip()
        if not nm:
            errs.append("Name is required.")

        if errs:
            for e in errs:
                st.error(e)
        else:
            payload = {
                "campaign_id": existing.get("campaign_id"),
                "name": nm,
                "start_date": start,
                "status": status or "active",
            }
            try:
                create_or_update_campaign(payload, business_id=business_id, db=db)
                st.success("Campaign saved.")
                st.session_state.camp_form_nonce += 1
                st.rerun()
            except Exception as ex:
                st.error(f"Could not save campaign: {ex}")

    if selected_id:
        col1, _ = st.columns([1, 1])
        with col1:
            if st.button("Delete selected"):
                deleted = delete_campaign(selected_id, business_id=business_id, db=db)
                if deleted:
                    st.success("Campaign deleted.")
                else:
                    st.warning("Campaign not found.")
                st.rerun()

    st.subheader("Danger zone")
    with st.expander("Backfill missing campaign IDs"):
        if st.button("Backfill now"):
            fixed = backfill_campaign_ids(business_id=business_id, db=db)
            st.success(f"Added campaign_id to {fixed} campaign(s).")
            st.rerun()
    with st.expander("Delete ALL campaigns for this business"):
        st.warning("This will permanently remove every campaign for this business.")
        confirm = st.text_input("Type DELETE to confirm:")
        if st.button("Delete ALL") and confirm == "DELETE":
            removed = delete_all_campaigns(business_id=business_id, db=db)
            st.success(f"Deleted {removed} campaigns.")
            st.rerun()

    with st.expander("Delete ALL ads for this business"):
        st.warning("This removes every ad document for this business. Registrations remain.")
        confirm_ads = st.text_input("Type DELETE ADS to confirm:")
        if st.button("Delete ALL Ads") and confirm_ads == "DELETE ADS":
            deleted_ads = db.ads.delete_many({"business_id": business_id}).deleted_count
            st.success(f"Deleted {deleted_ads} ads.")
            st.rerun()

    with st.expander("Delete ALL registrations for this business"):
        st.warning("This removes every registration record for this business (analytics will be empty).")
        confirm_regs = st.text_input("Type DELETE REGS to confirm:")
        if st.button("Delete ALL Registrations") and confirm_regs == "DELETE REGS":
            deleted_regs = db.registrations.delete_many({"business_id": business_id}).deleted_count
            st.success(f"Deleted {deleted_regs} registrations.")
            st.rerun()

    with st.expander("Cleanup orphan data (no campaign)"):
        if st.button("Run cleanup"):
            stats = cleanup_orphans(business_id=business_id, db=db)
            st.success(
                f"Removed {stats['registrations_deleted']} registrations and {stats['ads_deleted']} ads without campaigns."
            )
            st.rerun()

    return items


def _render_attach_section(ad_options: List[dict], campaigns: List[dict], business_id: str) -> None:
    st.subheader("Manage Campaign Ads")
    if not campaigns:
        st.info("Create a campaign first to attach ads.")
        return

    campaign_lookup = {c["campaign_id"]: c for c in campaigns if c.get("campaign_id")}
    if not campaign_lookup:
        st.info("No campaigns available for selection.")
        return

    campaign_choice = st.selectbox(
        "Select campaign",
        options=list(campaign_lookup.keys()),
        format_func=lambda cid: f"{campaign_lookup[cid].get('name', '(unnamed)')} ({cid})",
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

    linked_campaigns = (
        campaigns_using_ad(selected_campaign["ad_ids"][0], business_id)
        if selected_campaign.get("ad_ids")
        else []
    )
    if linked_campaigns:
        st.caption(
            "This campaign currently shares ads with: "
            + ", ".join(
                f"{row['name']} ({row['campaign_id']})"
                for row in linked_campaigns
                if row["campaign_id"] != campaign_choice
            )
        )


def main() -> None:
    business_id, business_name = _require_business()
    st.title(f"Configuration - {business_name}")

    db = get_db()
    campaigns = _render_manage_campaigns(db, business_id)

    st.divider()

    ads_response = list_ads(
        business_id=business_id,
        page_size=100,
    )
    ad_options = [ad for ad in ads_response["items"] if _is_recent(ad, start_dt)]
    _render_attach_section(ad_options, campaigns, business_id)


if __name__ == "__main__":
    main()
