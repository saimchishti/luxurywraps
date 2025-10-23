"""Analytics dashboard visuals."""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

PAGE_DIR = Path(__file__).resolve().parents[1]
if str(PAGE_DIR) not in sys.path:
    sys.path.append(str(PAGE_DIR))

from services.analytics import ad_performance, campaign_rollup, kpis, timeseries_daily  # noqa: E402
from services.repositories import list_campaigns  # noqa: E402
from utils.constants import BUSINESS_ID_SESSION_KEY, BUSINESS_NAME_SESSION_KEY  # noqa: E402
from utils.filters import use_start_date  # noqa: E402
from utils.formatting import format_currency  # noqa: E402

start_dt = use_start_date()


def _format_percent(value: float | None) -> str:
    if not value:
        return "0.00%"
    return f"{value:.2%}"


def _require_business() -> tuple[str, str]:
    business_id = st.session_state.get(BUSINESS_ID_SESSION_KEY)
    if not business_id:
        st.error("Please log in to view analytics.")
        st.stop()
    business_name = st.session_state.get(BUSINESS_NAME_SESSION_KEY, business_id)
    return business_id, business_name


def _render_kpis(business_id: str, start_dt: datetime) -> None:
    totals = kpis(None, start_dt, None, business_id)
    columns = st.columns(10)
    columns[0].metric("Registrations", f"{totals['registrations']:,}")
    columns[1].metric("Messages", f"{int(totals['messages']):,}")
    columns[2].metric("Spent", format_currency(totals["spent"]))
    columns[3].metric("Reach", f"{int(totals['reach']):,}")
    columns[4].metric("Impressions", f"{int(totals['impressions']):,}")
    columns[5].metric("Clicks", f"{int(totals['clicks']):,}")
    columns[6].metric("CTR", _format_percent(totals["ctr"]))
    columns[7].metric("CPM", format_currency(totals["cpm"]))
    columns[8].metric("CPC", format_currency(totals["cpc"]))
    columns[9].metric("CPR", format_currency(totals["cpr"]))


def _render_timeseries(business_id: str, start_dt: datetime) -> None:
    st.subheader("Daily Trends")
    data = timeseries_daily(None, start_dt, None, business_id)
    if not data:
        st.info("No activity for the selected date range.")
        return
    df = pd.DataFrame(data)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")

    st.line_chart(data=df.set_index("date")[["registrations"]], height=250, use_container_width=True)
    st.line_chart(
        data=df.set_index("date")[["impressions", "clicks"]],
        height=250,
        use_container_width=True,
    )
    st.line_chart(data=df.set_index("date")[["spent"]], height=250, use_container_width=True)


def _render_top_campaigns(business_id: str, start_dt: datetime) -> pd.DataFrame:
    st.subheader("Top Campaigns")
    data = campaign_rollup(None, start_dt, None, business_id)
    if not data:
        st.info("No campaign performance available.")
        return pd.DataFrame()
    df = pd.DataFrame(data)
    df["Spent"] = df["spent"].apply(format_currency)
    df["CPR"] = df["cpr"].apply(format_currency)
    df["CTR"] = df["ctr"].apply(_format_percent)
    df["Registrations"] = df["registrations"]
    df["Messages"] = df["messages"]
    df["Impressions"] = df["impressions"]
    df["Clicks"] = df["clicks"]
    df["Reach"] = df["reach"]
    display_columns = [
        "name",
        "status",
        "Registrations",
        "Messages",
        "Spent",
        "Reach",
        "Impressions",
        "Clicks",
        "CTR",
        "CPR",
    ]
    st.dataframe(
        df[display_columns].rename(columns={"name": "Campaign", "status": "Status"}),
        use_container_width=True,
    )
    return df


def _render_top_ads(business_id: str, start_dt: datetime) -> None:
    st.subheader("Top Ads")
    campaigns = list_campaigns(
        business_id=business_id,
        page_size=100,
    )["items"]
    campaign_lookup = {item["campaign_id"]: item for item in campaigns}
    selection = st.selectbox(
        "Campaign",
        ["All campaigns"] + list(campaign_lookup.keys()),
        format_func=lambda value: "All campaigns"
        if value == "All campaigns"
        else f"{campaign_lookup[value].get('name', value)} ({value})",
    )
    campaign_id = None if selection == "All campaigns" else selection

    data = ad_performance(
        dt_from=start_dt,
        business_id=business_id,
        campaign_id=campaign_id,
    )
    if not data:
        st.info("No ad performance data available.")
        return
    df = pd.DataFrame(data)
    if "tags" not in df.columns:
        df["tags"] = [[] for _ in range(len(df))]

    def _tags_to_str(value):
        if isinstance(value, (list, tuple)):
            return ", ".join(str(tag).strip() for tag in value if str(tag).strip())
        return ""

    df["Tags"] = df["tags"].apply(_tags_to_str)
    df["Spent"] = df["spent"].apply(format_currency)
    df["CPR"] = df["cpr"].apply(format_currency)
    df["CTR"] = df["ctr"].apply(_format_percent)
    df["Registrations"] = df["registrations"]
    df["Messages"] = df["messages"]
    df["Impressions"] = df["impressions"]
    df["Clicks"] = df["clicks"]
    df["Reach"] = df["reach"]
    display_columns = [
        "title",
        "Tags",
        "Registrations",
        "Spent",
        "Reach",
        "Impressions",
        "Clicks",
        "CTR",
        "CPR",
    ]
    st.dataframe(
        df[display_columns].rename(columns={"title": "Ad"}),
        use_container_width=True,
    )


def main() -> None:
    business_id, business_name = _require_business()
    st.title(f"Analytics - {business_name}")
    _render_kpis(business_id, start_dt)
    _render_timeseries(business_id, start_dt)
    _render_top_campaigns(business_id, start_dt)
    _render_top_ads(business_id, start_dt)


if __name__ == "__main__":
    main()
