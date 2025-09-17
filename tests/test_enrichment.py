from app.enrichment import mock_enrich_company


def test_enrichment_deterministic():
    a = mock_enrich_company("SampleCo", "alex@sampleco.com")
    b = mock_enrich_company("SampleCo", "alex@sampleco.com")
    assert a == b

