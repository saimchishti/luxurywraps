"""Analytics dashboard visuals."""

from __future__ import annotations

from datetime import datetime

import altair as alt
import pandas as pd
import streamlit as st

from streamlit_app.services.analytics import (  # noqa: E402
    ad_performance,
    ad_performance_table_simple,
    campaign_rollup,
    clicks_impressions_by_ad_simple,
    kpis,
    kpis_full,
    timeseries_daily,
)
from streamlit_app.services.db import get_db  # noqa: E402
from streamlit_app.services.repositories import list_campaigns  # noqa: E402
from streamlit_app.utils.constants import BUSINESS_ID_SESSION_KEY, BUSINESS_NAME_SESSION_KEY  # noqa: E402
from streamlit_app.utils.filters import use_start_date  # noqa: E402
from streamlit_app.utils.filters_analytics import use_date_range_for_analytics  # noqa: E402
from streamlit_app.utils.formatting import format_currency  # noqa: E402


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
        limit=100,
    )
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
    db = get_db()
    start_dt = use_start_date()
    dt_from, dt_to = use_date_range_for_analytics()
    st.title(f"Analytics - {business_name}")
    totals = kpis_full(db, dt_from=dt_from, dt_to=dt_to, business_id=business_id)
    r1c1, r1c2, r1c3, r1c4 = st.columns(4)
    r1c1.metric("Total Ad Spend", f"${totals['spent']:,.2f}")
    r1c2.metric("Customer Acquisition Cost", f"${totals['cac']:,.2f}")
    r1c3.metric("Total Messages", f"{int(totals['messages']):,}")
    r1c4.metric("Cost per Message", f"${totals['cost_per_msg']:,.2f}")
    r2c1, r2c2, r2c3, r2c4 = st.columns(4)
    r2c1.metric("Total Customers", f"{int(totals['customers']):,}")
    r2c2.metric("Total Clicks", f"{int(totals['clicks']):,}")
    r2c3.metric("Total Impressions", f"{int(totals['impressions']):,}")
    r2c4.metric("CTR", f"{totals['ctr_pct']:.2f}%")
    r3c1, r3c2, r3c3, r3c4 = st.columns(4)
    r3c1.metric("Conversion Rate", f"{totals['conv_pct']:.2f}%")
    r3c2.metric("Frequency", f"{totals['frequency']:.2f}")
    r3c3.metric("Reach", f"{int(totals['reach']):,}")
    r3c4.metric("Engagement Rate", f"{totals['engagement_pct']:.2f}%")
    _render_kpis(business_id, start_dt)
    _render_timeseries(business_id, start_dt)
    _render_top_campaigns(business_id, start_dt)

    # ---- Clicks vs Impressions (Top 10 Ads) ----
    st.subheader("Clicks vs Impressions")
    top_ads = clicks_impressions_by_ad_simple(
        db,
        dt_from=dt_from,
        dt_to=dt_to,
        business_id=business_id,
        limit=10,
    )
    df_bar = pd.DataFrame(top_ads)
    if not df_bar.empty:
        long = df_bar.melt(
            id_vars=["title"],
            value_vars=["clicks", "impressions"],
            var_name="metric",
            value_name="value",
        )
        chart = (
            alt.Chart(long)
            .mark_bar()
            .encode(
                x=alt.X("title:N", sort="-y", title="Ad"),
                y=alt.Y("value:Q", title="Count"),
                color=alt.Color("metric:N", title=""),
                tooltip=["title", "metric", "value"],
            )
        ).properties(height=360).interactive()
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No data to display for the selected start date.")

    st.divider()

    # ---- Ad Performance Table ----
    st.subheader("Ad Performance")
    rows = ad_performance_table_simple(
        db,
        dt_from=dt_from,
        dt_to=dt_to,
        business_id=business_id,
    )
    df = pd.DataFrame(rows)
    if not df.empty:
        df["CTR %"] = (
            (df["clicks"] / df["impressions"])
            .fillna(0)
            .replace([float("inf"), -float("inf")], 0)
            * 100
        )
        df["CAC"] = (
            (df["spent"] / df["customers"])
            .replace([float("inf"), -float("inf")], 0)
            .fillna(0)
        )
        df["Cost/Msg"] = (
            (df["spent"] / df["messages"])
            .replace([float("inf"), -float("inf")], 0)
            .fillna(0)
        )
        df["Conv %"] = (
            (df["customers"] / df["messages"])
            .replace([float("inf"), -float("inf")], 0)
            .fillna(0)
            * 100
        )
        display = df[
            [
                "ad_name",
                "spent",
                "messages",
                "customers",
                "clicks",
                "impressions",
                "CTR %",
                "Conv %",
                "Cost/Msg",
                "CAC",
                "reach",
            ]
        ].rename(
            columns={
                "ad_name": "Ad Name",
                "spent": "Spend",
                "messages": "Messages",
                "customers": "Customers",
                "clicks": "Clicks",
                "impressions": "Impressions",
                "reach": "Reach",
            }
        )
        st.dataframe(
            display.style.format(
                {
                    "Spend": "${:,.2f}",
                    "Cost/Msg": "${:,.2f}",
                    "CAC": "${:,.2f}",
                    "CTR %": "{:,.2f}",
                    "Conv %": "{:,.2f}",
                }
            ),
            use_container_width=True,
        )
    else:
        st.info("No ad performance yet for the selected start date.")

    st.divider()

    _render_top_ads(business_id, start_dt)


if __name__ == "__main__":
    main()
