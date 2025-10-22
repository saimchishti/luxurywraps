from datetime import datetime, timedelta

import streamlit as st


def use_start_date():
    if "start_date" not in st.session_state:
        st.session_state.start_date = datetime.utcnow().date() - timedelta(days=30)
    with st.sidebar:
        st.markdown("### Start Date")
        st.session_state.start_date = st.date_input("From", st.session_state.start_date)
    sd = st.session_state.start_date
    return datetime(sd.year, sd.month, sd.day)
