from app.scoring import compute_score


def test_scoring_bands_and_keywords():
    rules = {
        "title_includes": {"growth": 10, "vp": 15},
        "company_size_points": [
            {"min": 1, "max": 49, "points": 5},
            {"min": 50, "max": 199, "points": 10},
            {"min": 200, "max": 999, "points": 20},
            {"min": 1000, "max": 1000000, "points": 25},
        ],
        "country_boost": {"United States": 5},
        "source_boost": {"Product Signup": 15},
        "penalties": {"missing_company": -5},
    }
    lead = {
        "title": "VP of Growth",
        "company_size": 450,
        "country_norm": "United States",
        "source": "Product Signup",
        "email_valid": True,
        "phone_norm": "+14155551234",
        "company": "X",
    }
    score = compute_score(lead, rules)
    assert score == 10 + 15 + 20 + 5 + 15 + 5 + 3


def test_scoring_penalty_clamp():
    rules = {"title_includes": {}, "company_size_points": [], "country_boost": {}, "source_boost": {}, "penalties": {"missing_company": -50}}
    lead = {"company": None}
    score = compute_score(lead, rules)
    assert score == 0

