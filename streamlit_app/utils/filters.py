from datetime import datetime, timedelta

import streamlit as st


def use_date_filters():
    if "start_date" not in st.session_state:
        st.session_state.start_date = datetime.utcnow().date() - timedelta(days=14)
    if "end_date" not in st.session_state:
        st.session_state.end_date = datetime.utcnow().date()
    with st.sidebar:
        st.markdown("### \\U0001F4C5 Date Filter")
        st.session_state.start_date = st.date_input("Start", st.session_state.start_date)
        st.session_state.end_date = st.date_input("End", st.session_state.end_date)
    return st.session_state.start_date, st.session_state.end_date
