"""Analytics dashboard visuals."""

from __future__ import annotations

import sys
from datetime import date, datetime, time, timezone
from pathlib import Path

import pandas as pd
import streamlit as st

PAGE_DIR = Path(__file__).resolve().parents[1]
if str(PAGE_DIR) not in sys.path:
    sys.path.append(str(PAGE_DIR))

from services.analytics import ad_performance, campaign_rollup, kpis, timeseries_daily  # noqa: E402
from services.repositories import list_campaigns  # noqa: E402
from utils.constants import BUSINESS_ID_SESSION_KEY, BUSINESS_NAME_SESSION_KEY  # noqa: E402
from utils.filters import use_date_filters  # noqa: E402
from utils.formatting import format_currency  # noqa: E402

start_date, end_date = use_date_filters()


def _date_range_to_datetimes(start_date: date, end_date: date) -> tuple[datetime, datetime]:
    start_dt = datetime.combine(start_date, time.min).replace(tzinfo=timezone.utc)
    end_dt = datetime.combine(end_date, time.max).replace(tzinfo=timezone.utc)
    return start_dt, end_dt


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


def _render_kpis(business_id: str, dt_from: datetime, dt_to: datetime) -> None:
    totals = kpis(None, dt_from, dt_to, business_id)
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


def _render_timeseries(business_id: str, dt_from: datetime, dt_to: datetime) -> None:
    st.subheader("Daily Trends")
    data = timeseries_daily(None, dt_from, dt_to, business_id)
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


def _render_top_campaigns(business_id: str, dt_from: datetime, dt_to: datetime) -> pd.DataFrame:
    st.subheader("Top Campaigns")
    data = campaign_rollup(None, dt_from, dt_to, business_id)
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


def _render_top_ads(business_id: str, dt_from: datetime, dt_to: datetime) -> None:
    st.subheader("Top Ads")
    campaigns = list_campaigns(
        business_id=business_id,
        page_size=100,
        dt_from=dt_from,
        dt_to=dt_to,
    )["items"]
    campaign_lookup = {item["campaign_id"]: item for item in campaigns}
    selected_campaign = st.selectbox(
        "Filter by campaign",
        options=[None] + list(campaign_lookup.keys()),
        format_func=lambda value: "All campaigns" if value is None else campaign_lookup[value]["name"],
    )

    data = ad_performance(
        None,
        dt_from,
        dt_to,
        business_id,
        campaign_id=selected_campaign,
    )
    if not data:
        st.info("No ad performance data available.")
        return
    df = pd.DataFrame(data)
    df["Spent"] = df["spent"].apply(format_currency)
    df["CPR"] = df["cpr"].apply(format_currency)
    df["CTR"] = df["ctr"].apply(_format_percent)
    df["Registrations"] = df["registrations"]
    df["Messages"] = df["messages"]
    df["Impressions"] = df["impressions"]
    df["Clicks"] = df["clicks"]
    df["Reach"] = df["reach"]
    df["Tags"] = df["tags"].apply(lambda tags: ", ".join(tags or []))
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
    st.title(f"Analytics — {business_name}")


    dt_from, dt_to = _date_range_to_datetimes(start_date, end_date)

    _render_kpis(business_id, dt_from, dt_to)
    _render_timeseries(business_id, dt_from, dt_to)
    _render_top_campaigns(business_id, dt_from, dt_to)
    _render_top_ads(business_id, dt_from, dt_to)


if __name__ == "__main__":
    main()
