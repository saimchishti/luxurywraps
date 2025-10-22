"""Tenant-aware authentication helpers."""

from __future__ import annotations

import bcrypt
import streamlit as st

from streamlit_app.utils.constants import (
    AUTH_SESSION_KEY,
    BUSINESS_ID_SESSION_KEY,
    BUSINESS_NAME_SESSION_KEY,
)


def do_rerun() -> None:
    if hasattr(st, "rerun"):
        st.rerun()
    else:
        st.experimental_rerun()


def login_form(db) -> bool:
    st.subheader("Business Login")

    opts = list(db.businesses.find({}, {"_id": 0, "business_id": 1, "name": 1}))
    if not opts:
        st.info("No businesses configured. Run the seed script to create demo tenants.")
        return False

    label_by_id = {o["business_id"]: o.get("name") or o["business_id"] for o in opts}
    ids = list(label_by_id.keys())
    labels = [label_by_id[i] for i in ids]

    default_idx = 0
    current_id = st.session_state.get(BUSINESS_ID_SESSION_KEY)
    if current_id in ids:
        default_idx = ids.index(current_id)

    with st.form("business_login", clear_on_submit=False):
        chosen_label = st.selectbox("Business", labels, index=default_idx)
        chosen_id = ids[labels.index(chosen_label)]
        password = st.text_input("Password", type="password")
        submit = st.form_submit_button("Save")

        if submit:
            business = db.businesses.find_one({"business_id": chosen_id})
            if not business or not bcrypt.checkpw(
                password.encode("utf-8"), business["password_hash"].encode("utf-8")
            ):
                st.error("Invalid business or password.")
                return False

            st.session_state[AUTH_SESSION_KEY] = True
            st.session_state[BUSINESS_ID_SESSION_KEY] = chosen_id
            st.session_state[BUSINESS_NAME_SESSION_KEY] = business.get("name", chosen_id)
            st.success("Logged in.")
            do_rerun()
    return False


def require_auth(db) -> bool:
    if st.session_state.get(AUTH_SESSION_KEY):
        return True
    login_form(db)
    st.stop()


def logout_button() -> None:
    if st.sidebar.button("Logout"):
        for key in (AUTH_SESSION_KEY, BUSINESS_ID_SESSION_KEY, BUSINESS_NAME_SESSION_KEY):
            st.session_state.pop(key, None)
        do_rerun()
