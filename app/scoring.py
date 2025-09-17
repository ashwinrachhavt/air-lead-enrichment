from typing import Any, Dict
from .config import load_rules


def compute_score(lead: Dict[str, Any], rules: Dict[str, Any]) -> int:
    score = 0
    title = (lead.get("title") or "").lower()
    for kw, pts in rules.get("title_includes", {}).items():
        if kw in title:
            score += int(pts)
    size = lead.get("company_size")
    if isinstance(size, int):
        for band in rules.get("company_size_points", []):
            if band["min"] <= size <= band["max"]:
                score += int(band["points"])
                break
    country = lead.get("country_norm")
    if country in rules.get("country_boost", {}):
        score += int(rules["country_boost"][country])
    source = lead.get("source")
    if source in rules.get("source_boost", {}):
        score += int(rules["source_boost"][source])
    if lead.get("email_valid") is True:
        score += 5
    phone_norm = lead.get("phone_norm") or ""
    if phone_norm:
        score += 3
    if not lead.get("company"):
        score += int(rules.get("penalties", {}).get("missing_company", 0))
    return max(0, int(score))


def score_one(lead: Dict[str, Any]) -> int:
    rules = load_rules()
    return compute_score(lead, rules)

