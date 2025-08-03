# Snowflake Weight Tracker

A personal weight tracking pipeline built to simulate a modern data platform using **Snowflake**, **dbt**, and **FastAPI**. 

This project tracks daily weigh-ins recorded in Notion, loads them into Snowflake, models the data using dbt, and exposes a REST API for future dashboard or frontend development.

---

## Tech Stack

- **Notion API** – data source leveraging Notion databases for tracking weight data
- **Pandas** – initial data loading and transformation
- **Snowflake** – cloud data warehouse
- **dbt** – data modeling (staging → intermediate → marts)
- **FastAPI** – lightweight REST API to serve data

---

## Data Model

```ascii
+--------------------------+
|     Notion (weight)     |
+-----------+--------------+
            |
            v
+--------------------------+
|  RAW: weight_logs_raw    |  ← inserted via Python
+--------------------------+
            |
            v
+--------------------------+
| STAGING: stg_weight_logs |  ← renamed + typed
+--------------------------+
            |
            v
+--------------------------+
| INTERMEDIATE: int_weight_logs  |  ← enriched
+--------------------------+
            |
            v
+--------------------------+
| MARTS: fct_weight_logs, agg_weight_trends |
+--------------------------+
```

---

## Current API Endpoints
Once the FastAPI server is running:

`GET /weight-logs`
→ Returns all records from the fct_weight_logs mart (used for frontend integration).

More endpoints (e.g. trend summaries, daily insights) will be made in the future.

## Project Goals
This project was built to:

- Practice building ELT pipelines using Snowflake and dbt
- Create a real-world, personal use-case for data modeling
- Serve data via a clean REST API for future frontend use
- Simulate a deployable modern data platform from ingestion → mart → API

## Next Steps
- Add Airflow DAG to orchestrate ingestion + transformation
- Add aggregation endpoints (/daily-summary, /weight-trends)
- Automate Notion ingestion (currently manual/notebook)
- Deploy API (AWS EC2?)
- Add tests + monitoring to the API layer