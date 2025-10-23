"""Streamlit entrypoint configuring authentication and global layout."""

from __future__ import annotations

# --- path bootstrap so absolute imports like `from streamlit_app.services...` work on Streamlit Cloud
import os
import sys

APP_DIR = os.path.dirname(os.path.abspath(__file__))
if APP_DIR not in sys.path:
    sys.path.insert(0, APP_DIR)
# ---

from pathlib import Path

import streamlit as st

# Ensure local imports resolve when launched from different directories.
BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from streamlit_app.services.db import ensure_indexes, get_db  # noqa: E402
from streamlit_app.utils.auth import logout_button, require_auth  # noqa: E402
from streamlit_app.utils.constants import (  # noqa: E402
    AUTH_SESSION_KEY,
    BUSINESS_ID_SESSION_KEY,
    BUSINESS_NAME_SESSION_KEY,
)


def _render_business_badge() -> None:
    business_id = st.session_state.get(BUSINESS_ID_SESSION_KEY)
    business_name = st.session_state.get(BUSINESS_NAME_SESSION_KEY, business_id or "")
    if business_id:
        with st.sidebar:
            st.markdown(
                f"**Business:** {business_name}  \n"
                f"`{business_id}`"
            )


def main() -> None:
    st.set_page_config(
        page_title="Campaign Configuration Dashboard",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    try:
        ensure_indexes()
        st.session_state["indexes_ready"] = True
    except Exception as exc:  # pragma: no cover
        st.sidebar.error(f"Failed to ensure indexes: {exc}")

    db = get_db()
    require_auth(db)
    if not st.session_state.get(AUTH_SESSION_KEY):
        st.stop()

    logout_button()

    _render_business_badge()

    st.title("Campaign Configuration Center")
    st.markdown(
        """
        Use the navigation sidebar to manage campaigns, ads, registrations, and analytics.
        Select a page from the sidebar to get started.
        """
    )
    st.info("All metrics and data are isolated per business login.")


if __name__ == "__main__":
    main()
