import io
import base64
import json
import logging
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple
import pandas as pd
from fastapi import Body, FastAPI, File, Form, HTTPException, Request, Response, UploadFile
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


@app.get("/ui")
def ui_page() -> HTMLResponse:
    html = """
    <!doctype html>
    <html lang=\"en\">
    <head>
      <meta charset=\"utf-8\"/>
      <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"/>
      <title>Lead Enrichment</title>
      <script src=\"https://cdn.tailwindcss.com\"></script>
    </head>
    <body class=\"bg-gray-50 min-h-screen flex items-center justify-center p-6\">
      <div class=\"w-full max-w-2xl\">
        <div class=\"bg-white shadow-xl rounded-xl p-8\">
          <h1 class=\"text-2xl font-semibold text-gray-800 mb-2\">Lead CSV Cleaner</h1>
          <p class=\"text-gray-500 mb-6\">Upload a CSV to normalize, enrich, score, and download a cleaned CSV.</p>
          <form id=\"csvform\" class=\"space-y-5\" enctype=\"multipart/form-data\" method=\"post\" action=\"/ui/ingest\"> 
            <input type=\"hidden\" name=\"column_map\" id=\"column_map\"> 
            <div>
              <label class=\"block text-sm font-medium text-gray-700 mb-1\">CSV File</label>
              <div id=\"dropzone\" class=\"flex items-center justify-center w-full h-32 border-2 border-dashed rounded-lg bg-gray-50 hover:bg-gray-100 border-gray-300 cursor-pointer\">
                <span class=\"text-gray-500 text-sm\">Drag & drop CSV here or click to select</span>
                <input id=\"fileInput\" class=\"hidden\" type=\"file\" name=\"file\" accept=\".csv\" required>
              </div>
            </div>
            <div class=\"flex items-center space-x-2\">
              <input id=\"drop_invalid\" name=\"drop_invalid\" type=\"checkbox\" value=\"true\" class=\"h-4 w-4 text-indigo-600 border-gray-300 rounded\">
              <label for=\"drop_invalid\" class=\"text-sm text-gray-700\">Drop invalid rows</label>
            </div>
            <div id=\"mapping\" class=\"space-y-3 hidden\"></div>
            <div id=\"progressWrap\" class=\"hidden\">
              <div class=\"w-full bg-gray-200 rounded-full h-2\">
                <div id=\"progressBar\" class=\"bg-indigo-600 h-2 rounded-full\" style=\"width:0%\"></div>
              </div>
              <div id=\"progressText\" class=\"text-xs text-gray-500 mt-1\">0%</div>
            </div>
            <p class=\"text-xs text-gray-500\">Expected columns: Name, Email, Phone, Title, Company, Country, Created At, Source</p>
            <div class=\"flex items-center space-x-3\">
              <button type=\"submit\" class=\"inline-flex items-center px-4 py-2 bg-indigo-600 border border-transparent rounded-md font-medium text-white hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-indigo-500\">Upload and Clean</button>
              <a href=\"/docs\" class=\"text-sm text-indigo-600 hover:underline\">API Docs</a>
            </div>
          </form>
        </div>
      </div>
      <script>
        const expected = [\"Name\",\"Email\",\"Phone\",\"Title\",\"Company\",\"Country\",\"Created At\",\"Source\"];
        const dropzone = document.getElementById('dropzone');
        const fileInput = document.getElementById('fileInput');
        const mapping = document.getElementById('mapping');
        const form = document.getElementById('csvform');
        const colMapInput = document.getElementById('column_map');
        const progressWrap = document.getElementById('progressWrap');
        const progressBar = document.getElementById('progressBar');
        const progressText = document.getElementById('progressText');
        dropzone.addEventListener('click', () => fileInput.click());
        dropzone.addEventListener('dragover', e => { e.preventDefault(); dropzone.classList.add('bg-gray-100'); });
        dropzone.addEventListener('dragleave', e => { dropzone.classList.remove('bg-gray-100'); });
        dropzone.addEventListener('drop', e => {
          e.preventDefault(); dropzone.classList.remove('bg-gray-100');
          if (e.dataTransfer.files && e.dataTransfer.files.length) {
            fileInput.files = e.dataTransfer.files;
            buildMappingFromFile(fileInput.files[0]);
          }
        });
        fileInput.addEventListener('change', () => { if (fileInput.files[0]) buildMappingFromFile(fileInput.files[0]); });
        function splitCsv(line) {
          const parts = [];
          let cur = '', inQ = false;
          for (let i=0;i<line.length;i++) {
            const ch = line[i];
            if (ch === '"') { inQ = !inQ; continue; }
            if (ch === ',' && !inQ) { parts.push(cur.trim().replace(/^\"|\"$/g,'')); cur=''; continue; }
            cur += ch;
          }
          parts.push(cur.trim().replace(/^\"|\"$/g,''));
          return parts;
        }
        function buildMappingFromFile(file) {
          const reader = new FileReader();
          reader.onload = () => {
            const text = reader.result || '';
            const firstLine = String(text).split(/\r?\n/)[0] || '';
            const headers = splitCsv(firstLine);
            buildMapping(headers);
          };
          reader.readAsText(file.slice(0, 2000));
        }
        function buildMapping(headers) {
          mapping.innerHTML = '';
          mapping.classList.remove('hidden');
          expected.forEach(label => {
            const key = label;
            const wrap = document.createElement('div');
            wrap.className = 'grid grid-cols-2 gap-3 items-center';
            const l = document.createElement('label');
            l.className = 'text-sm text-gray-700';
            l.textContent = label + ' column';
            const sel = document.createElement('select');
            sel.className = 'mt-1 block w-full border border-gray-300 rounded-md p-2 text-sm';
            const empty = document.createElement('option'); empty.value = ''; empty.textContent = '(auto)'; sel.appendChild(empty);
            headers.forEach(h => { const o = document.createElement('option'); o.value = h; o.textContent = h; sel.appendChild(o); });
            const match = headers.find(h => h.toLowerCase() === label.toLowerCase());
            if (match) sel.value = match;
            sel.dataset.key = key;
            wrap.appendChild(l); wrap.appendChild(sel); mapping.appendChild(wrap);
          });
        }
        form.addEventListener('submit', e => {
          e.preventDefault();
          if (!fileInput.files || !fileInput.files.length) { alert('Please choose a CSV'); return; }
          const sels = mapping.querySelectorAll('select');
          const m = {};
          sels.forEach(s => { if (s.value) m[s.dataset.key.toLowerCase()] = s.value; });
          colMapInput.value = Object.keys(m).length ? JSON.stringify(m) : '';
          const xhr = new XMLHttpRequest();
          xhr.open('POST', form.action);
          xhr.upload.onprogress = (e) => {
            if (e.lengthComputable) {
              const pct = Math.round((e.loaded / e.total) * 100);
              progressWrap.classList.remove('hidden');
              progressBar.style.width = pct + '%';
              progressText.textContent = pct + '%';
            }
          };
          xhr.onload = () => {
            document.open(); document.write(xhr.responseText); document.close();
          };
          const fd = new FormData(form);
          xhr.send(fd);
        });
      </script>
    </body>
    </html>
    """
    return HTMLResponse(html)
@app.get("/")
def root() -> HTMLResponse:
    html = """
    <!doctype html>
    <html lang=\"en\">
    <head>
      <meta charset=\"utf-8\"/>
      <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"/>
      <title>Lead Enrichment</title>
      <script src=\"https://cdn.tailwindcss.com\"></script>
    </head>
    <body class=\"bg-gray-50 min-h-screen flex items-center justify-center p-6\">
      <div class=\"w-full max-w-2xl\">
        <div class=\"bg-white shadow-xl rounded-xl p-8\">
          <h1 class=\"text-2xl font-semibold text-gray-800 mb-2\">Lead CSV Cleaner</h1>
          <p class=\"text-gray-500 mb-6\">Upload a CSV to normalize, enrich, score, and download a cleaned CSV.</p>
          <form class=\"space-y-5\" enctype=\"multipart/form-data\" method=\"post\" action=\"/ui/ingest\">
            <div>
              <label class=\"block text-sm font-medium text-gray-700 mb-1\">CSV File</label>
              <input class=\"block w-full text-sm text-gray-900 border border-gray-300 rounded-lg cursor-pointer bg-gray-50 focus:outline-none focus:ring-2 focus:ring-indigo-500\" type=\"file\" name=\"file\" accept=\".csv\" required>
            </div>
            <div class=\"flex items-center space-x-2\">
              <input id=\"drop_invalid\" name=\"drop_invalid\" type=\"checkbox\" value=\"true\" class=\"h-4 w-4 text-indigo-600 border-gray-300 rounded\">
              <label for=\"drop_invalid\" class=\"text-sm text-gray-700\">Drop invalid rows</label>
            </div>
            <p class=\"text-xs text-gray-500\">Expected columns: Name, Email, Phone, Title, Company, Country, Created At, Source</p>
            <button type=\"submit\" class=\"inline-flex items-center px-4 py-2 bg-indigo-600 border border-transparent rounded-md font-medium text-white hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-indigo-500\">Upload and Clean</button>
          </form>
          <div class=\"mt-6 text-sm text-gray-500\">Use the interactive API at <a class=\"text-indigo-600 hover:underline\" href=\"/docs\">/docs</a>.</div>
        </div>
      </div>
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
async def ui_ingest(file: UploadFile = File(...), drop_invalid: Optional[str] = None, column_map: Optional[str] = Form(None)) -> HTMLResponse:
    drop = drop_invalid == "true" or drop_invalid == "on"
    res = await ingest_csv(file=file, drop_invalid=drop, column_map=column_map)
    rows: List[Dict[str, Any]] = [r.model_dump(by_alias=True) for r in res.results]
    df = pd.DataFrame(rows)
    csv_buf = io.StringIO()
    df.to_csv(csv_buf, index=False)
    csv_buf.seek(0)
    csv_data = csv_buf.getvalue().encode()
    href = "data:text/csv;base64," + base64.b64encode(csv_data).decode()
    s = res.summary
    headers_html = ''.join(["<th class='px-3 py-2 text-left font-medium text-gray-700'>" + str(c) + "</th>" for c in df.columns])
    rows_html = ''.join(['<tr>' + ''.join(["<td class='px-3 py-2 text-gray-700'>" + str(row.get(col)) + "</td>" for col in df.columns]) + '</tr>' for _, row in df.head(10).iterrows()])
    html = f"""
    <!doctype html>
    <html lang=\"en\">
    <head>
      <meta charset=\"utf-8\"/>
      <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"/>
      <title>Cleaned CSV</title>
      <script src=\"https://cdn.tailwindcss.com\"></script>
    </head>
    <body class=\"bg-gray-50 min-h-screen flex items-center justify-center p-6\">
      <div class=\"w-full max-w-3xl\">
        <div class=\"bg-white shadow-xl rounded-xl p-8\">
          <h1 class=\"text-2xl font-semibold text-gray-800 mb-4\">Cleaned CSV Ready</h1>
          <div class=\"grid grid-cols-2 gap-4 text-sm text-gray-700\">
            <div class=\"p-3 bg-gray-50 rounded\"><span class=\"font-medium\">Count In:</span> {s.count_in}</div>
            <div class=\"p-3 bg-gray-50 rounded\"><span class=\"font-medium\">Count Out:</span> {s.count_out}</div>
            <div class=\"p-3 bg-gray-50 rounded\"><span class=\"font-medium\">Dropped:</span> {s.dropped}</div>
            <div class=\"p-3 bg-gray-50 rounded\"><span class=\"font-medium\">% Enriched:</span> {s.percent_enriched}</div>
            <div class=\"p-3 bg-gray-50 rounded\"><span class=\"font-medium\">Avg Score:</span> {s.avg_score}</div>
          </div>
          <div class=\"mt-6 flex items-center space-x-3\">
            <a href=\"{href}\" download=\"cleaned.csv\" class=\"inline-flex items-center px-4 py-2 bg-indigo-600 border border-transparent rounded-md font-medium text-white hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-indigo-500\">Download Cleaned CSV</a>
            <a href=\"/\" class=\"inline-flex items-center px-4 py-2 bg-gray-100 border border-gray-300 rounded-md font-medium text-gray-700 hover:bg-gray-200 focus:outline-none focus:ring-2 focus:ring-gray-300\">Back</a>
            <a href=\"/docs\" class=\"inline-flex items-center px-4 py-2 bg-white border border-gray-300 rounded-md font-medium text-gray-700 hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-gray-300\">API Docs</a>
          </div>
          <div class=\"mt-8\">
            <h2 class=\"text-sm font-semibold text-gray-600 mb-2\">Preview</h2>
            <div class=\"overflow-auto max-h-96 border rounded\">
              <table class=\"min-w-full divide-y divide-gray-200 text-xs\">
                <thead class=\"bg-gray-50\">{headers_html}</thead>
                <tbody class=\"divide-y divide-gray-100\">{rows_html}</tbody>
              </table>
            </div>
          </div>
        </div>
      </div>
    </body>
    </html>
    """
    return HTMLResponse(html)


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
