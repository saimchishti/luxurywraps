"""Ad Library management page."""

from __future__ import annotations

import sys
from datetime import date, datetime, time, timezone
from pathlib import Path

import streamlit as st

PAGE_DIR = Path(__file__).resolve().parents[1]
if str(PAGE_DIR) not in sys.path:
    sys.path.append(str(PAGE_DIR))

from services.repositories import (  # noqa: E402
    RepositoryError,
    campaigns_using_ad,
    create_ad,
    delete_ad,
    list_ads,
    update_ad,
)
from utils.auth import do_rerun  # noqa: E402
from utils.constants import BUSINESS_ID_SESSION_KEY, BUSINESS_NAME_SESSION_KEY  # noqa: E402
from utils.filters import use_date_filters  # noqa: E402

start_date, end_date = use_date_filters()


def _require_business() -> tuple[str, str]:
    business_id = st.session_state.get(BUSINESS_ID_SESSION_KEY)
    if not business_id:
        st.error("Please log in to manage the ad library.")
        st.stop()
    business_name = st.session_state.get(BUSINESS_NAME_SESSION_KEY, business_id)
    return business_id, business_name


def _date_range_to_datetimes(start_date: date, end_date: date) -> tuple[datetime, datetime]:
    start_dt = datetime.combine(start_date, time.min).replace(tzinfo=timezone.utc)
    end_dt = datetime.combine(end_date, time.max).replace(tzinfo=timezone.utc)
    return start_dt, end_dt


def _render_create_ad(business_id: str) -> None:
    st.subheader("Create New Ad")
    with st.form("create-ad", clear_on_submit=True):
        title = st.text_input("Title *", placeholder="Holiday Promo Carousel")
        creative_url = st.text_input("Creative URL", placeholder="https://cdn.example.com/creative.mp4")
        status = st.selectbox("Status", options=["active", "paused", "archived"])
        tags_raw = st.text_input("Tags", placeholder="holiday, carousel, evergreen")
        save = st.form_submit_button("Save")
    if save:
        payload = {
            "title": title,
            "creative_url": creative_url or None,
            "status": status,
            "tags": [tag.strip() for tag in tags_raw.split(",") if tag.strip()],
        }
        try:
            with st.spinner("Saving ad..."):
                create_ad(payload, business_id=business_id)
            st.toast("Ad created.")
            do_rerun()
        except RepositoryError as exc:
            st.error(f"Unable to create ad: {exc}")


def _render_ad_list(business_id: str, dt_from: datetime, dt_to: datetime) -> None:
    st.subheader("Ad Library")
    search = st.text_input("Search ads", placeholder="Search by title")
    status_filter = st.selectbox(
        "Status filter",
        options=[None, "active", "paused", "archived"],
        format_func=lambda value: "All" if value is None else value.title(),
    )
    tag_filter = st.text_input("Filter by tag", placeholder="retargeting")
    response = list_ads(
        business_id=business_id,
        search=search or None,
        status=status_filter,
        tags=[tag_filter] if tag_filter else None,
        page_size=100,
        dt_from=dt_from,
        dt_to=dt_to,
    )
    ads = response["items"]
    if not ads:
        st.info("No ads saved yet. Create an ad to populate the library.")
        return
    for ad in ads:
        with st.expander(f"{ad['title']} ({ad['status']})", expanded=False):
            st.markdown(f"**Ad ID:** `{ad['ad_id']}`")
            if ad.get("creative_url"):
                st.markdown(f"[Preview creative]({ad['creative_url']})")
            st.write("Tags:", ", ".join(ad.get("tags", [])) or "None")
            linked_campaigns = campaigns_using_ad(ad["ad_id"], business_id=business_id)
            if linked_campaigns:
                st.caption(
                    "Used in campaigns: "
                    + ", ".join(f"{campaign['name']} ({campaign['campaign_id']})" for campaign in linked_campaigns)
                )
            else:
                st.caption("Not attached to any campaigns yet.")
            _render_edit_ad(ad, business_id)


def _render_edit_ad(ad: dict, business_id: str) -> None:
    pending_key = f"pending-delete-ad-{ad['ad_id']}"
    with st.form(f"edit-ad-{ad['ad_id']}"):
        title = st.text_input("Title", value=ad.get("title", ""))
        creative_url = st.text_input("Creative URL", value=ad.get("creative_url", "") or "")
        status = st.selectbox(
            "Status",
            options=["active", "paused", "archived"],
            index=["active", "paused", "archived"].index(ad.get("status", "active")),
        )
        tags_raw = st.text_input("Tags", value=", ".join(ad.get("tags", [])))
        save = st.form_submit_button("Save")
    if save:
        payload = {
            "title": title,
            "creative_url": creative_url or None,
            "status": status,
            "tags": [tag.strip() for tag in tags_raw.split(",") if tag.strip()],
        }
        try:
            with st.spinner("Updating ad..."):
                update_ad(ad["ad_id"], payload, business_id=business_id)
            st.toast("Ad updated.")
            do_rerun()
        except RepositoryError as exc:
            st.error(f"Update failed: {exc}")
    if st.button("Delete", key=f"delete-{ad['ad_id']}", type="secondary"):
        st.session_state[pending_key] = True

    if st.session_state.get(pending_key):
        warn_col, confirm_col, cancel_col = st.columns([3, 1, 1])
        warn_col.warning("Confirm delete to remove this ad.")
        if confirm_col.button("Confirm delete", key=f"confirm-{ad['ad_id']}"):
            try:
                with st.spinner("Deleting ad..."):
                    delete_ad(ad["ad_id"], business_id=business_id)
                st.toast("Ad deleted.")
            except RepositoryError as exc:
                st.error(f"Delete failed: {exc}")
            finally:
                st.session_state.pop(pending_key, None)
                do_rerun()
        if cancel_col.button("Cancel", key=f"cancel-{ad['ad_id']}"):
            st.session_state.pop(pending_key, None)
            st.info("Deletion cancelled.")


def main() -> None:
    business_id, business_name = _require_business()
    st.title(f"Ad Library — {business_name}")


    dt_from, dt_to = _date_range_to_datetimes(start_date, end_date)

    _render_create_ad(business_id)
    _render_ad_list(business_id, dt_from, dt_to)


if __name__ == "__main__":
    main()
