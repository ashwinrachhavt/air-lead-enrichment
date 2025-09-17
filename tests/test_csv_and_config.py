import io
from fastapi.testclient import TestClient
from app.main import app


client = TestClient(app)


def test_bulk_dedupe():
    payload = {"leads": [
        {"name": "Alex Doe", "email": "a@example.com", "phone": "(415) 555-1234", "company": "ACME"},
        {"name": "Alex Doe", "email": "a@example.com", "phone": "(415) 555-1234", "company": "ACME"},
    ]}
    r = client.post("/bulk", json=payload)
    assert r.status_code == 200
    data = r.json()
    dropped = [x for x in data["results"] if x["status"] == "dropped"]
    assert len(dropped) >= 1
    assert any("duplicate_in_batch" in x.get("warnings", []) for x in data["results"])


def test_ingest_csv_drop_invalid():
    csv_data = "Name,Email,Phone,Title,Company,Country,Created At,Source\nAlex Doe,alex@example.com,(415) 555-1234,Head of Growth,ACME,US,08/15/2025,Website\nNo Contact,,abc,,,US,2025-01-01,Event\n"
    files = {"file": ("leads.csv", csv_data, "text/csv")}
    r = client.post("/ingest_csv?drop_invalid=true", files=files)
    assert r.status_code == 200
    data = r.json()
    assert data["summary"]["count_in"] == 2
    assert data["summary"]["count_out"] <= 2


def test_rules_config_roundtrip():
    r1 = client.get("/config/rules")
    assert r1.status_code == 200
    rules = r1.json()
    rules["country_boost"]["United Kingdom"] = 7
    r2 = client.put("/config/rules", json=rules)
    assert r2.status_code == 200
    saved = r2.json()
    assert saved["country_boost"]["United Kingdom"] == 7
