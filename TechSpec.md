Lead Normalization + Enrichment + Scoring — Tech Spec (Brief)
1) Purpose

Build a FastAPI service that:

Normalizes messy lead data (names, emails, phones, country, dates).

Enriches leads (mock, deterministic: company_size, industry, website).

Scores leads via a JSON ruleset.

Supports single, bulk, CSV ingestion; returns per-lead results + batch summary.
Repo also includes quick EDA assets, tests, a Dockerfile, and Makefile helpers. 
GitHub

2) Stack & Layout

Python ≥3.10, FastAPI, Pydantic v2, Pandas (CSV), Uvicorn; optional Docker.

Project structure:

/app
  main.py        # endpoints & wiring
  models.py      # Pydantic models
  enrichment.py  # mock deterministic enrichment
  scoring.py     # score computation from rules.json
  rules.json     # scoring config
/EDA             # notebook/outputs for quick data profiling
/tests           # pytest suite
Dockerfile
Makefile
requirements.txt or pyproject.toml


Run locally (per README): uv run uvicorn app.main:app --reload; docs at /docs.

Docker: docker build -t lead-enrichment . → docker run -p 8000:8000 lead-enrichment.

Make targets: make install|run|test|docker-build|docker-run|docker-stop. 
GitHub

3) Endpoints (behavioral contract)

GET /health → { "ok": true }.

GET /config/rules → returns current rules.json.

PUT /config/rules → replace rules (schema-validated), hot-reloaded.

POST /enrich → single lead in, normalized+enriched+scored lead out.

POST /bulk → { leads: LeadIn[] } → results[], summary{count_in,count_out,dropped,%_enriched,avg_score}.

POST /ingest_csv (multipart file): optional drop_invalid, optional column_map (JSON string).

POST /ui/ingest → accepts CSV from a minimal UI; returns cleaned CSV;

POST /salesforce/map → maps to SFDC fields; ?format=csv to download. 
GitHub

4) Data Models (Pydantic v2)

LeadIn
name?, email?, phone?, title?, company?, country?, created_at?, source?

LeadOut = LeadIn +
first_name?, last_name?, email_valid: bool, phone_norm?, country_norm?, created_at_iso?, company_size?, industry?, website?, status: "ok"|"dropped", warnings: list[str], score: int

BulkRequest { leads: LeadIn[] }
BulkResponse { results: LeadOut[], summary: {...} }

5) Normalization Rules (pure functions)

Name: split on first whitespace → first_name, last_name; Title-Case.

Email: lowercase/trim; regex validity flag.

Phone: digits-only →

11 digits starting “1” → trim to 10;

10 digits → +1##########;

11–15 digits → +{digits}; else empty + warning.

Country: map common variants (US/USA/U.S. → “United States”; UK/England → “United Kingdom”; etc.); otherwise Title-Case.

Date: accept YYYY-MM-DD, MM/DD/YYYY, DD/MM/YYYY, YYYY/MM/DD, else Pandas parse → ISO date or None.

Source: Title-Case; map common aliases.

Drop rule: mark status="dropped" only if email invalid/empty AND phone_norm empty.
Dedup (per request): key = email → else phone_norm → else hash(name|company); keep first, later duplicates status="dropped" with duplicate_in_batch.

6) Mock Enrichment (deterministic)

Seed = (company or "") + (email or ""); bucket = sha256(seed) % 6.

company_size ∈ {25,120,450,2000,5000,60};

industry ∈ {"Software","E-Commerce","FinTech","Media","Manufacturing","Healthcare"};

website from email domain if present.

7) Scoring

Load rules from app/rules.json (hot-reload). Defaults include:

title_includes points (e.g., marketing/growth/vp/chief/director/head),

company_size_points bands,

country_boost, source_boost,

optional penalties (e.g., missing company).

Add +5 if email_valid, +3 if phone_norm non-empty; clamp score ≥ 0. 
GitHub

8) CSV Ingestion

Expect columns: Name, Email, Phone, Title, Company, Country, Created At, Source (or use column_map to remap).

Return same shape as /bulk. drop_invalid=true filters out status="dropped" from results.

9) Ops & DevX

Config: editable rules.json, effective immediately.

Observability: log JSON lines with request_id & latency; echo X-Request-ID. 
GitHub

Tests: unit tests for normalizer/enrichment/scoring and API happy paths; pytest.

Run: uv sync && uv run uvicorn app.main:app --reload or Docker; docs at /docs. 
GitHub

10) Acceptance Criteria

All endpoints serve as above; /docs shows schemas.

Deterministic normalization/enrichment; updating rules.json changes scores without restart.

/bulk summary correct; duplicates flagged; drop rule enforced.

/ingest_csv handles provided messy CSV and returns cleaned CSV via /ui/ingest.

Container builds and runs; Make targets work.