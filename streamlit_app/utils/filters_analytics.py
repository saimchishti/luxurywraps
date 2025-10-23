from datetime import datetime, timedelta, time
import streamlit as st

def use_date_range_for_analytics():
    # defaults: last 30 days through today
    if "analytics_start" not in st.session_state:
        st.session_state.analytics_start = (datetime.utcnow().date() - timedelta(days=30))
    if "analytics_end" not in st.session_state:
        st.session_state.analytics_end = datetime.utcnow().date()

    with st.sidebar:
        st.markdown("### Date Range")
        st.session_state.analytics_start = st.date_input("Start", st.session_state.analytics_start, key="analytics_start_input")
        st.session_state.analytics_end   = st.date_input("End",   st.session_state.analytics_end,   key="analytics_end_input")

    s = st.session_state.analytics_start
    e = st.session_state.analytics_end
    # Inclusive end-of-day
    dt_from = datetime(s.year, s.month, s.day, 0, 0, 0)
    dt_to   = datetime(e.year, e.month, e.day, 23, 59, 59)
    return dt_from, dt_to
