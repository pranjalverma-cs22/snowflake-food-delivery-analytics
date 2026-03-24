# Food Delivery End‑to‑End Data Engineering Project

## Overview
This project implements a production‑style end‑to‑end data engineering and analytics platform using Snowflake and Streamlit.  
It ingests raw CSV data, processes it incrementally, applies SCD Type‑2 modeling, builds fact tables, and exposes business KPIs through analytical views and a Streamlit dashboard.

---

## Tech Stack
- Snowflake (Snowpipe, Streams, Tasks)
- Snowflake SQL
- Python
- Streamlit
- Pandas
- Altair

---

## Architecture
![Data Flow](architecture/architecture_overview.png)


## Key Features
- Incremental ingestion using Snowpipe
- Change Data Capture using Streams
- Task‑driven transformations
- SCD Type‑2 dimensions
- Order‑item level fact table
- KPI views for analytics
- Role‑based PII masking
- Interactive Streamlit dashboard

---

## How to Run

### Snowflake
1. Create database and schemas
2. Execute SQL scripts in the following order:
   - stage
   - clean
   - consumption
   - tasks
   - security

### Streamlit
```bash
pip install -r requirements.txt
streamlit run app.py
