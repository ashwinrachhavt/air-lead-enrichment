from fastapi.testclient import TestClient
from app.main import app


client = TestClient(app)


def test_health():
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"ok": True}


def test_enrich_endpoint():
    payload = {
        "name": "Alex Doe",
        "email": "Alex@SampleCo.com",
        "phone": "(415) 555-1234",
        "title": "Head of Growth",
        "company": "SampleCo",
        "country": "US",
        "created_at": "08/15/2025",
        "source": "Product Signup",
    }
    r = client.post("/enrich", json=payload)
    assert r.status_code == 200
    data = r.json()
    assert data["email"] == "alex@sampleco.com"
    assert data["phone_norm"] == "+14155551234"
    assert data["country_norm"] == "United States"
    assert data["created_at_iso"] == "2025-08-15"
    assert data["status"] == "ok"
    assert isinstance(data["score"], int)


def test_bulk_endpoint():
    payload = {"leads": [{"email": "a@example.com", "phone": ""}, {"email": "a@example.com", "phone": ""}]}
    r = client.post("/bulk", json=payload)
    assert r.status_code == 200
    data = r.json()
    assert data["summary"]["count_in"] == 2
    assert data["summary"]["count_out"] == 2
    assert data["summary"]["dropped"] >= 1


def test_salesforce_map_json():
    payload = {"leads": [{"name": "Alex Doe", "email": "alex@example.com", "phone": "(415) 555-1234", "title": "Head of Growth", "company": "ACME", "country": "US", "created_at": "2025-08-15", "source": "Website"}]}
    r = client.post("/salesforce/map", json=payload)
    assert r.status_code == 200
    rows = r.json()
    assert isinstance(rows, list)
    row = rows[0]
    assert set(["FirstName", "LastName", "Email", "Phone", "Title", "Company", "Country", "LeadSource", "CreatedDate__c", "Website__c", "Industry__c", "CompanySize__c", "Score__c"]).issubset(set(row.keys()))


def test_salesforce_map_csv():
    payload = {"leads": [{"email": "alex@example.com"}]}
    r = client.post("/salesforce/map?format=csv", json=payload)
    assert r.status_code == 200
    assert r.headers.get("content-type").startswith("text/csv")
