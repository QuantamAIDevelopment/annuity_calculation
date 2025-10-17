# sipl_annuity_agent — Architecture

This document describes the architecture, components, dataflow, error handling, and extension points for the `sipl_annuity_agent` project.

## High-level overview

The agent is a small orchestrated pipeline that:

- Connects to a SQL Server database (`SIPL_3TABLES`) using `pyodbc`.
- Reads all rows from `dbo.LPS_FINAL` into a Pandas DataFrame.
- Computes annuity values per record using the provided business rules.
- Writes computed summaries to `dbo.LPS_ANNUITY_SUMMARY` (creating the table if it doesn't exist).
- Exposes a REST API (`POST /execute`) implemented with FastAPI to trigger the workflow.

The codebase is intentionally modular to allow easy replacement of components (for example, swap `pyodbc` for SQLAlchemy, or replace the API runner).

## Project layout

sipl_annuity_agent/

- `main.py` — FastAPI application entrypoint. Starts the ASGI server when run directly.
- `api/routes.py` — Defines the `/execute` endpoint which orchestrates the workflow.
- `agents/` — Individual agents for distinct responsibilities:
  - `db_agent.py` — Connects to database and fetches source table into a DataFrame.
  - `annuity_agent.py` — Implements annuity computation logic for each record.
  - `writer_agent.py` — Creates the summary table (if needed) and inserts computed rows in batches with sanitization.
- `core/` — Core utilities and helpers:
  - `database.py` — Contains `get_connection()` using `pyodbc` and the configured connection string.
  - `utils.py` — Small helpers like `chunked()` used for batch processing.
- `requirements.txt` — Pin required Python packages.
- `README.md` — Run instructions and caveats.

## Dataflow (detailed)

1. POST /execute is called.
2. The API route calls `agents.db_agent.fetch_lps_final()` which opens a `pyodbc` connection and runs `SELECT * FROM dbo.LPS_FINAL`. The result is materialized into a Pandas DataFrame.
3. For each DataFrame row, `agents.annuity_agent.compute_annuity_for_row()` is invoked. This function:
   - Normalizes dates (handles `pandas.Timestamp` and `NaT`) and numeric values.
   - Applies the exact business rule provided for OWNERTYPE (`S` and `M`) and calculates base `Annuity_Clac`.
   - Computes total annuity for 15 years with 10% annual increments for years 1–10 and constant for years 11–15.
   - Sums existing annuity payments from the provided `FIRST_ANNUITY` ... `ELEVENTH_ANNUITY` columns and computes `Difference_Amount`.
4. The computed values are attached to each row.
5. `agents.writer_agent.create_table_if_not_exists()` ensures the destination table exists.
6. `agents.writer_agent.insert_rows()` batches rows (default chunk 500), sanitizes values (converts empty strings/NaN to `NULL`, casts numeric fields to float, converts dates to Python `datetime`) and performs parameterized `executemany` insert with `fast_executemany = True`.

## Error handling

- Database connection errors: surfaced by pyodbc exceptions. The app currently does not swallow these — they surface as HTTP 500 responses. Consider wrapping DB calls with explicit try/except and returning structured API error messages.
- Data conversion errors: the writer sanitizes inputs to avoid TDS RPC errors (invalid float strings) by coercing invalid numerics to `NULL`. The annuity calculation attempts to parse dates and falls back safely.
- Logging: minimal. You should add structured logging for production (e.g., Python `logging` module with file/console handlers) and optionally capture failed rows to a CSV for manual inspection.

## Extension points

- Swap `pyodbc` for SQLAlchemy: change `core/database.py` to create an SQLAlchemy engine and pass that engine into pandas `read_sql` to remove pandas DBAPI warning and gain connection pooling.
- Add unit tests: isolate `compute_annuity_for_row()` and write test vectors for boundary cases (Form_914_Agreement_Date before/after 2015-02-01, ROWWISEEXTENT 0.5 vs >1, OWNER types `S` and `M`).
- Add monitoring: expose Prometheus metrics for rows processed, insert failures, and process duration.
- Add idempotency: add a dedupe step or upsert logic using MERGE keyed on `AadhaarNumber`/`ApplicationNumber` to avoid duplicate summary rows.

## Security considerations

- The app uses Trusted Connection to SQL Server. Ensure the server account has least-privilege needed for SELECT on the source table and INSERT/CREATE on the target table.
- Avoid exposing this endpoint publicly without authentication. Consider adding an API key or OAuth to control who can trigger the workflow.

## Operational notes

- For large source tables, consider streaming rows (chunked SQL reads) rather than loading the entire table into memory with pandas.
- `fast_executemany` helps with MSSQL bulk inserts but requires proper sanitization of types — this implementation converts values to appropriate Python types before insertion.

## Glossary

- TDS — Tabular Data Stream (SQL Server protocol).
- NaT — pandas Not-a-Time (missing datetime value).

## Architecture diagram

Below is a simple diagram that visualizes the primary components and the dataflow. You can paste this Mermaid block into VS Code or a Mermaid renderer to view it graphically.

```mermaid
flowchart TD
    A[Client / Copilot Execute] -->|POST /execute| B[FastAPI (main.py / api/routes.py)]
    B --> C[DB Agent (agents/db_agent.py)]
    C -->|SELECT * FROM dbo.LPS_FINAL| D[SQL Server: SIPL_3TABLES]
    C --> E[Pandas DataFrame]
    E --> F[Annuity Agent (agents/annuity_agent.py)]
    F --> G[Rows with computed fields]
    G --> H[Writer Agent (agents/writer_agent.py)]
    H -->|CREATE/INSERT| D[SQL Server: dbo.LPS_ANNUITY_SUMMARY]
    subgraph Core
      I[core/database.py]
      J[core/utils.py]
    end
    B --- I
    H --- I
    B --- J

    style D fill:#f9f,stroke:#333,stroke-width:1px
    style B fill:#bbf,stroke:#333,stroke-width:1px
    style F fill:#bfb,stroke:#333,stroke-width:1px
```

If you prefer, I can also export a PNG/SVG of this diagram and add it to the repo.
