import hashlib
from typing import Optional, Tuple


def _domain_from_email(email: Optional[str]) -> Optional[str]:
    if not email or "@" not in email:
        return None
    domain = email.split("@", 1)[1].lower().strip()
    return domain or None


def _is_free_domain(domain: Optional[str]) -> bool:
    if not domain:
        return False
    free = {"gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "icloud.com", "proton.me", "protonmail.com"}
    return domain in free


def mock_enrich_company(company: Optional[str], email: Optional[str]) -> Tuple[Optional[int], Optional[str], Optional[str]]:
    seed = f"{company or ''}{email or ''}"
    h = hashlib.sha256(seed.encode()).hexdigest()
    bucket = int(h, 16) % 6
    sizes = [25, 120, 450, 2000, 5000, 60]
    industries = ["Software", "E-Commerce", "FinTech", "Media", "Manufacturing", "Healthcare"]
    company_size = sizes[bucket]
    industry = industries[bucket]
    domain = _domain_from_email(email)
    website = f"https://{domain}" if domain else None
    return company_size, industry, website


def company_domain(email: Optional[str]) -> Optional[str]:
    domain = _domain_from_email(email)
    if domain and not _is_free_domain(domain):
        return domain
    return None


def is_b2b(email: Optional[str]) -> bool:
    return company_domain(email) is not None

