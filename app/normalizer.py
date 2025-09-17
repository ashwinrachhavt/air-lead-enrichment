import re
import hashlib
from typing import Optional, Tuple
import pandas as pd


EMAIL_REGEX = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")


def _clean_str(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None


def split_name(name: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    s = _clean_str(name)
    if not s:
        return None, None
    tokens = s.split()
    if not tokens:
        return None, None
    first = tokens[0].title()
    last = " ".join(tokens[1:]).title() if len(tokens) > 1 else None
    return first, last


def canonical_email(email: Optional[str]) -> Optional[str]:
    s = _clean_str(email)
    if not s:
        return None
    return s.lower()


def validate_email(email: Optional[str]) -> bool:
    if not email:
        return False
    return bool(EMAIL_REGEX.match(email))


def normalize_phone(phone: Optional[str]) -> str:
    if phone is None:
        return ""
    digits = re.sub(r"\D", "", str(phone))
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) == 10:
        return "+1" + digits
    if 11 <= len(digits) <= 15:
        return "+" + digits
    return ""


def normalize_country(country: Optional[str]) -> Optional[str]:
    s = _clean_str(country)
    if not s:
        return None
    v = s.lower()
    mapping = {
        "us": "United States",
        "usa": "United States",
        "u.s.": "United States",
        "united states": "United States",
        "uk": "United Kingdom",
        "england": "United Kingdom",
        "gb": "United Kingdom",
        "u.k.": "United Kingdom",
        "great britain": "United Kingdom",
        "de": "Germany",
        "ger": "Germany",
        "uae": "United Arab Emirates",
        "u.a.e": "United Arab Emirates",
        "korea": "South Korea",
        "south korea": "South Korea",
        "republic of korea": "South Korea",
        "viet nam": "Vietnam",
        "ind": "India",
    }
    return mapping.get(v, s.title())


def parse_date(created_at: Optional[str]) -> Optional[str]:
    s = _clean_str(created_at)
    if not s:
        return None
    try:
        dt = pd.to_datetime(s, errors="raise", dayfirst=False, utc=False)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        try:
            dt = pd.to_datetime(s, errors="raise", dayfirst=True, utc=False)
            return dt.strftime("%Y-%m-%d")
        except Exception:
            return None


def normalize_source(source: Optional[str]) -> Optional[str]:
    s = _clean_str(source)
    if not s:
        return None
    title = s.title()
    mapping = {
        "Linkedin": "LinkedIn",
        "Product Signup": "Product Signup",
        "Website": "Website",
        "Event": "Event",
    }
    return mapping.get(title, title)


def dedupe_key(name: Optional[str], email: Optional[str], phone_norm: str, company: Optional[str]) -> Optional[str]:
    if email:
        return f"email:{email}"
    if phone_norm:
        return f"phone:{phone_norm}"
    if name and company:
        token = f"{name}|{company}"
        return hashlib.sha256(token.encode()).hexdigest()
    return None

