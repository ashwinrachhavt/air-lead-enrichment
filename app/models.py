from typing import Any, Dict, List, Literal, Optional
from pydantic import BaseModel, Field


class LeadIn(BaseModel):
    name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    title: Optional[str] = None
    company: Optional[str] = None
    country: Optional[str] = None
    created_at: Optional[str] = None
    source: Optional[str] = None


class LeadOut(LeadIn):
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email_valid: bool = False
    phone_norm: Optional[str] = None
    country_norm: Optional[str] = None
    created_at_iso: Optional[str] = None
    company_size: Optional[int] = None
    industry: Optional[str] = None
    website: Optional[str] = None
    status: Literal["ok", "dropped"] = "ok"
    warnings: List[str] = Field(default_factory=list)
    score: int = 0


class BulkRequest(BaseModel):
    leads: List[LeadIn]


class Summary(BaseModel):
    count_in: int
    count_out: int
    dropped: int
    percent_enriched: float = Field(alias="%_enriched")
    avg_score: float

    model_config = {
        "populate_by_name": True,
        "json_encoders": {},
    }


class BulkResponse(BaseModel):
    results: List[LeadOut]
    summary: Summary


class RulesModel(BaseModel):
    title_includes: Dict[str, int]
    company_size_points: List[Dict[str, int]]
    country_boost: Dict[str, int]
    source_boost: Dict[str, int]
    penalties: Dict[str, int]

