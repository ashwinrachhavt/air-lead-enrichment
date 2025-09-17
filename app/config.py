import json
import os
import time
from typing import Any, Dict, Optional, Tuple
from pydantic import BaseModel, ValidationError
from .models import RulesModel


DEFAULT_RULES = {
    "title_includes": {
        "marketing": 10,
        "growth": 10,
        "demand": 10,
        "vp": 15,
        "chief": 20,
        "head": 12,
        "director": 12,
    },
    "company_size_points": [
        {"min": 1, "max": 49, "points": 5},
        {"min": 50, "max": 199, "points": 10},
        {"min": 200, "max": 999, "points": 20},
        {"min": 1000, "max": 1000000, "points": 25},
    ],
    "country_boost": {"United States": 5},
    "source_boost": {"Product Signup": 15, "Website": 10, "LinkedIn": 8},
    "penalties": {"missing_company": -5},
}


_CACHE: Dict[str, Tuple[float, Dict[str, Any]]] = {}


def _rules_path() -> str:
    base = os.path.dirname(__file__)
    return os.path.join(base, "rules.json")


def ensure_rules_file() -> None:
    path = _rules_path()
    if not os.path.exists(path):
        with open(path, "w", encoding="utf-8") as f:
            json.dump(DEFAULT_RULES, f, indent=2)


def validate_rules(data: Dict[str, Any]) -> RulesModel:
    model = RulesModel.model_validate(data)
    for k, v in model.title_includes.items():
        if not isinstance(v, int):
            raise ValueError("title_includes values must be integers")
    for band in model.company_size_points:
        for key in ("min", "max", "points"):
            if key not in band or not isinstance(band[key], int):
                raise ValueError("company_size_points must contain integer min,max,points")
    return model


def load_rules() -> Dict[str, Any]:
    ensure_rules_file()
    path = _rules_path()
    mtime = os.path.getmtime(path)
    cached = _CACHE.get(path)
    if cached and cached[0] == mtime:
        return cached[1]
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    model = validate_rules(data)
    rules = model.model_dump()
    _CACHE[path] = (mtime, rules)
    return rules


def save_rules(data: Dict[str, Any]) -> Dict[str, Any]:
    model = validate_rules(data)
    path = _rules_path()
    with open(path, "w", encoding="utf-8") as f:
        json.dump(model.model_dump(), f, indent=2)
    mtime = os.path.getmtime(path)
    _CACHE[path] = (mtime, model.model_dump())
    return model.model_dump()

