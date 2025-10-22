"""Shared constants for the Streamlit configuration dashboard."""

from datetime import timedelta

AD_STATUSES = ["active", "paused", "archived"]
CAMPAIGN_STATUSES = ["draft", "active", "paused", "completed"]

DEFAULT_PAGE_SIZE = 20
MAX_PAGE_SIZE = 100

FILTER_SESSION_KEY = "global_filters"
AUTH_SESSION_KEY = "authed"
BUSINESS_ID_SESSION_KEY = "business_id"
BUSINESS_NAME_SESSION_KEY = "business_name"

DEFAULT_DATE_RANGE_DAYS = 30

DEFAULT_TIMEDELTA = timedelta(days=DEFAULT_DATE_RANGE_DAYS)

CSV_DATE_FORMAT = "%Y-%m-%d"
CSV_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
