# Enchantments Configuration Dashboard

Streamlit dashboard for managing advertising campaigns, ads, and registrations backed by MongoDB.

## Features
- Campaign configuration center with drafts, status management, and ad attachments.
- Ad Library CRUD with tag management and campaign references.
- Registrations explorer with filters, pagination, and CSV export.
- Analytics with KPI cards, rollups, and time-series charts.
- Multi-business login with isolated tenant data and analytics.

## Prerequisites
- Python 3.10+
- MongoDB database (Atlas or local)

## Setup
1. Clone the repository and navigate to `streamlit_app/`.
2. Create a virtual environment and activate it.
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Copy `.env.example` to `.env` and set:
   - `MONGODB_URI`
   - `MONGODB_DB`

## Running the App
```bash
streamlit run app.py
```

The Streamlit sidebar lets you navigate between configuration, analytics, ads, and registrations pages.

## Seeding Demo Data
A helper in `services/repositories.py` inserts demo ads, campaigns, and registrations. Run:
```bash
python seed.py
```
to populate with sample data (requires valid MongoDB credentials).

## Multi-Business Login
Use the seeded demo credentials to explore tenant isolation:

- Business: `enchanments` | Password: `enchanments_pass`
- Business: `luxury_floor_wraps` | Password: `luxury_pass`

## Deploying
- Push to a repo and connect to Streamlit Community Cloud.
- Configure the same environment variables in the deployment settings.

## Testing
- Validate MongoDB indexes: `ensure_indexes()` runs on startup.
- Use Streamlit UI flows for CRUD and analytics verification.
