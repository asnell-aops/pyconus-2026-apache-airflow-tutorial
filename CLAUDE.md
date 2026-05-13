# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## About This Repository

A hands-on workshop repository for teaching Apache Airflow 3.2 through a bookstore data platform. Five progressive DAGs introduce one batch of Airflow concepts at a time, from basic anatomy to asset-driven pipelines.

## Rules to follow
- Never ever use em-dash

## Development Environment Setup

### Option A: Local Python Environment
```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
./scripts/start_airflow_standalone.sh
```

Before running DAGs: set up the `bookshop_postgres` connection in Airflow UI (Admin > Connections):
- Conn ID: `bookshop_postgres`
- Conn Type: `Postgres`
- Host: `localhost`, Database: `bookops`, Login: `airflow`, Password: `airflow`, Port: `5432`

### Option B: Docker Compose
```bash
docker compose up --build
```

## Core Commands

- **Start Airflow**: `./scripts/start_airflow_standalone.sh` (local) or `docker compose up --build` (Docker)
- **Start slides**: `cd slides && npm install && npm run dev`
- **Access Airflow UI**: http://localhost:8080 (credentials: airflow/airflow)
- **Analytics app**: http://localhost:8501 (Docker only)

## Repository Structure

```
dags/                   Starter DAGs for participants (TODO blocks)
dags/solutions/         Complete solution DAGs
data/books.csv          25-row catalog with 4 intentional error rows
data/sales/             7 daily JSON files (2026-05-01 to 2026-05-07)
sql/schema.sql          All table DDL (CREATE TABLE IF NOT EXISTS)
sql/reset.sql           DROP all tables for clean restarts
analytics-app/          Streamlit dashboard (3 tabs mirroring DAGs 01-04)
slides/                 Slidev presentation workspace
```

## DAG Progression

| DAG | Airflow Topics | DE Topics |
|-----|---------------|-----------|
| `00_hello_world` | Anatomy, `@task`, `>>`, Variables, branching | None |
| `01_ingest_books` | Connections, PostgresHook, idempotency | CSV ingestion, ON CONFLICT upsert |
| `02_daily_sales` | schedule, catchup, `{{ ds }}`, XCom | Incremental loads, backfill |
| `03b_validate_sales` | `@task.branch`, trigger_rule | Data quality, quarantine pattern |
| `04_genre_report` | Dynamic task mapping, Assets | Parallel aggregation, reporting mart |

## Database Schema

**`books`**: `isbn TEXT PK, title TEXT NOT NULL, author TEXT, genre TEXT, price NUMERIC(6,2)`
**`raw_sales`**: `sale_id SERIAL PK, isbn TEXT, sale_date DATE, quantity INT, total NUMERIC(8,2)` (written by DAG 02, pre-validation)
**`daily_sales`**: `sale_id SERIAL PK, isbn TEXT, sale_date DATE, quantity INT, total NUMERIC(8,2)` (written by DAG 03b, post-validation)
**`sales_quarantine`**: `raw JSONB, reason TEXT, quarantined_at TIMESTAMP`
**`daily_report`**: `report_date DATE, genre TEXT, books_sold INT, revenue NUMERIC(10,2), PK(report_date, genre)`

## Connection ID

All DAGs use `bookshop_postgres`. Environment variable: `AIRFLOW_CONN_BOOKSHOP_POSTGRES`.
