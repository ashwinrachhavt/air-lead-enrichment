import io
import json
import logging
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple
import pandas as pd
from fastapi import Body, FastAPI, File, HTTPException, Request, Response, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from .models import BulkRequest, BulkResponse, LeadIn, LeadOut, Summary
from .normalizer import canonical_email, dedupe_key, normalize_country, normalize_phone, normalize_source, parse_date, split_name, validate_email
from .enrichment import mock_enrich_company
from .scoring import score_one
from .config import load_rules, save_rules


app = FastAPI(title="Lead Normalization + Enrichment + Scoring API")


logger = logging.getLogger("app")
logging.basicConfig(level=logging.INFO, format="%(message)s")


@app.middleware("http")
async def request_id_middleware(request: Request, call_next):
    rid = request.headers.get("X-Request-ID") or str(int(time.time() * 1000))
    start = time.time()
    response: Response
    try:
        response = await call_next(request)
    finally:
        duration_ms = int((time.time() - start) * 1000)
        logger.info(json.dumps({
            "request_id": rid,
            "endpoint": request.url.path,
            "method": request.method,
            "status": getattr(response, "status_code", 0) if "response" in locals() else 500,
            "latency_ms": duration_ms,
        }))
    response.headers["X-Request-ID"] = rid
    return response


@app.get("/health")
def health() -> Dict[str, bool]:
    return {"ok": True}


@app.get("/")
def root() -> HTMLResponse:
    html = """
    <!doctype html>
    <html>
    <head>
      <meta charset=\"utf-8\"/>
      <title>Lead Enrichment</title>
      <style>body{font-family:system-ui,Arial;margin:40px}form{margin-bottom:20px}label{display:block;margin:8px 0}input,select{padding:8px}button{padding:8px 12px} .hint{color:#666;font-size:12px}</style>
    </head>
    <body>
      <h1>Lead CSV Cleaner</h1>
      <form id=\"csvform\" enctype=\"multipart/form-data\" method=\"post\" action=\"/ui/ingest\"> 
        <label>CSV File <input type=\"file\" name=\"file\" accept=\".csv\" required/></label>
        <label>Drop invalid <input type=\"checkbox\" name=\"drop_invalid\" value=\"true\"/></label>
        <div class=\"hint\">Expected columns: Name, Email, Phone, Title, Company, Country, Created At, Source</div>
        <button type=\"submit\">Upload & Clean</button>
      </form>
      <p>Or use the interactive API docs at <a href=\"/docs\">/docs</a>.</p>
    </body>
    </html>
    """
    return HTMLResponse(html)


@app.get("/config/rules")
def get_rules() -> Dict[str, Any]:
    return load_rules()


@app.put("/config/rules")
def put_rules(body: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    try:
        return save_rules(body)
    except Exception as e:
        raise HTTPException(status_code=422, detail=str(e))


def normalize_one(inp: LeadIn) -> Dict[str, Any]:
    name = inp.name
    first_name, last_name = split_name(name)
    email = canonical_email(inp.email)
    email_valid = validate_email(email)
    phone_norm = normalize_phone(inp.phone)
    country_norm = normalize_country(inp.country)
    created_at_iso = parse_date(inp.created_at)
    source_norm = normalize_source(inp.source)
    warnings: List[str] = []
    if inp.phone and not phone_norm:
        warnings.append("unparseable_phone")
    if inp.created_at and created_at_iso is None:
        warnings.append("unparseable_date")
    company_size, industry, website = mock_enrich_company(inp.company, email)
    status = "ok" if email_valid or phone_norm else "dropped"
    out: Dict[str, Any] = {
        "name": name,
        "email": email,
        "phone": inp.phone,
        "title": inp.title,
        "company": inp.company,
        "country": inp.country,
        "created_at": inp.created_at,
        "source": source_norm,
        "first_name": first_name,
        "last_name": last_name,
        "email_valid": email_valid,
        "phone_norm": phone_norm,
        "country_norm": country_norm,
        "created_at_iso": created_at_iso,
        "company_size": company_size,
        "industry": industry,
        "website": website,
        "status": status,
        "warnings": warnings,
    }
    return out


def process_and_score(inp: LeadIn) -> LeadOut:
    data = normalize_one(inp)
    data["score"] = score_one(data)
    return LeadOut.model_validate(data)


@app.post("/enrich", response_model=LeadOut)
def enrich_endpoint(lead: LeadIn) -> LeadOut:
    return process_and_score(lead)


def bulk_process(leads: List[LeadIn]) -> Tuple[List[LeadOut], Dict[str, Any]]:
    results: List[LeadOut] = []
    seen: set = set()
    for l in leads:
        o = normalize_one(l)
        key = dedupe_key(o.get("name"), o.get("email"), o.get("phone_norm", ""), o.get("company"))
        if key is not None:
            if key in seen:
                o["status"] = "dropped"
                o.setdefault("warnings", []).append("duplicate_in_batch")
            else:
                seen.add(key)
        o["score"] = score_one(o)
        results.append(LeadOut.model_validate(o))
    count_in = len(leads)
    count_out = len(results)
    dropped = sum(1 for r in results if r.status == "dropped")
    enriched_count = sum(1 for r in results if r.company_size is not None and r.industry is not None)
    percent_enriched = round((enriched_count / count_out) * 100.0, 2) if count_out else 0.0
    avg_score = round(sum(r.score for r in results) / count_out, 2) if count_out else 0.0
    summary = {
        "count_in": count_in,
        "count_out": count_out,
        "dropped": dropped,
        "%_enriched": percent_enriched,
        "avg_score": avg_score,
    }
    return results, summary


@app.post("/bulk", response_model=BulkResponse)
def bulk_endpoint(req: BulkRequest) -> BulkResponse:
    results, summary = bulk_process(req.leads)
    return BulkResponse(results=results, summary=Summary.model_validate(summary))


def _coerce_lead_rows(df: pd.DataFrame, column_map: Optional[Dict[str, str]] = None) -> List[LeadIn]:
    expected = {
        "name": "Name",
        "email": "Email",
        "phone": "Phone",
        "title": "Title",
        "company": "Company",
        "country": "Country",
        "created_at": "Created At",
        "source": "Source",
    }
    mapping = {k: v for k, v in expected.items()}
    if column_map:
        for k in expected.keys():
            if k in column_map:
                mapping[k] = column_map[k]
    leads: List[LeadIn] = []
    for _, row in df.iterrows():
        payload = {}
        for key, col in mapping.items():
            payload[key] = None if col not in df.columns else (None if pd.isna(row.get(col)) else str(row.get(col)))
        leads.append(LeadIn(**payload))
    return leads


@app.post("/ingest_csv", response_model=BulkResponse)
async def ingest_csv(file: UploadFile = File(...), drop_invalid: bool = False, column_map: Optional[str] = None) -> BulkResponse:
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Expected a CSV file")
    content = await file.read()
    try:
        df = pd.read_csv(io.BytesIO(content))
    except Exception as e:
        raise HTTPException(status_code=400, detail="Invalid CSV")
    cm: Optional[Dict[str, str]] = None
    if column_map:
        try:
            cm = json.loads(column_map)
        except Exception:
            raise HTTPException(status_code=400, detail="column_map must be JSON string")
    leads = _coerce_lead_rows(df, cm)
    results, summary = bulk_process(leads)
    if drop_invalid:
        filtered = [r for r in results if r.status != "dropped"]
        dropped = len(results) - len(filtered)
        count_in = len(leads)
        count_out = len(filtered)
        enriched_count = sum(1 for r in filtered if r.company_size is not None and r.industry is not None)
        percent_enriched = round((enriched_count / count_out) * 100.0, 2) if count_out else 0.0
        avg_score = round(sum(r.score for r in filtered) / count_out, 2) if count_out else 0.0
        summary = {
            "count_in": count_in,
            "count_out": count_out,
            "dropped": dropped,
            "%_enriched": percent_enriched,
            "avg_score": avg_score,
        }
        results = filtered
    return BulkResponse(results=results, summary=Summary.model_validate(summary))


@app.post("/ui/ingest")
async def ui_ingest(file: UploadFile = File(...), drop_invalid: Optional[str] = None) -> StreamingResponse:
    drop = drop_invalid == "true" or drop_invalid == "on"
    res = await ingest_csv(file=file, drop_invalid=drop, column_map=None)
    rows: List[Dict[str, Any]] = [r.model_dump(by_alias=True) for r in res.results]
    if not rows:
        rows = []
    df = pd.DataFrame(rows)
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    buf.seek(0)
    headers = {"Content-Disposition": f"attachment; filename=cleaned_{int(time.time())}.csv"}
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv", headers=headers)


def map_salesforce_row(r: LeadOut) -> Dict[str, Any]:
    return {
        "FirstName": r.first_name,
        "LastName": r.last_name,
        "Email": r.email,
        "Phone": r.phone_norm or "",
        "Title": r.title,
        "Company": r.company,
        "Country": r.country_norm,
        "LeadSource": r.source,
        "CreatedDate__c": r.created_at_iso,
        "Website__c": r.website,
        "Industry__c": r.industry,
        "CompanySize__c": r.company_size,
        "Score__c": r.score,
    }


@app.post("/salesforce/map")
def salesforce_map(req: BulkRequest, format: Optional[str] = None):
    results, _ = bulk_process(req.leads)
    rows = [map_salesforce_row(r) for r in results]
    if format == "csv":
        df = pd.DataFrame(rows)
        buf = io.StringIO()
        df.to_csv(buf, index=False)
        buf.seek(0)
        headers = {"Content-Disposition": f"attachment; filename=salesforce_{int(time.time())}.csv"}
        return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv", headers=headers)
    return JSONResponse(content=rows)
