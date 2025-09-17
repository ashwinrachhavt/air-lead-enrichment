Lead Normalization + Enrichment + Scoring API

- FastAPI service that normalizes, enriches (mock deterministic), and scores leads
- Supports single, bulk, and CSV ingestion with per-lead results and batch summary
- Stateless and deterministic for identical inputs

Quickstart

- Python >= 3.10
- Use uv for dependency management

Run locally

- Install uv: https://docs.astral.sh/uv/
- Create venv and install deps: `uv sync`
- Start API: `uv run uvicorn app.main:app --reload`
- Open docs: http://localhost:8000/docs
- Minimal UI for CSV upload: http://localhost:8000/

Run tests

- `uv run pytest`

Docker

- Build: `docker build -t lead-enrichment .`
- Run: `docker run -p 8000:8000 lead-enrichment`
- Open: http://localhost:8000/docs or http://localhost:8000/

Makefile

- Install deps: `make install`
- Run server: `make run` (PORT=8000)
- Run tests: `make test`
- Build image: `make docker-build` (IMAGE=lead-enrichment)
- Run container: `make docker-run` (maps PORT to 8000)
- Stop container: `make docker-stop`

Endpoints

- `GET /health`
- `GET /config/rules`
- `PUT /config/rules`
- `POST /enrich`
- `POST /bulk`
- `POST /ingest_csv` (multipart file .csv, optional `drop_invalid`, optional `column_map` JSON string)
- `POST /ui/ingest` returns cleaned CSV download
- `POST /salesforce/map` maps to Salesforce fields, add `?format=csv` to download CSV

Notes

- Normalization, enrichment, scoring are deterministic and pure
- Rules are read from `app/rules.json`; updates take effect immediately
- Logging emits JSON-like lines including request_id and latency; header `X-Request-ID` is echoed
