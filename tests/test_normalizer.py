from app.normalizer import split_name, canonical_email, validate_email, normalize_phone, normalize_country, parse_date, normalize_source


def test_split_name():
    assert split_name("alex doe") == ("Alex", "Doe")
    assert split_name("Alex") == ("Alex", None)
    assert split_name(None) == (None, None)


def test_email_validation():
    e = canonical_email(" Alice@Example.COM ")
    assert e == "alice@example.com"
    assert validate_email(e) is True
    assert validate_email("bad@@ex.com") is False
    assert validate_email(None) is False


def test_phone_normalization():
    assert normalize_phone("(415) 555-1234") == "+14155551234"
    assert normalize_phone("14155551234") == "+14155551234"
    assert normalize_phone("+441234567890") == "+441234567890"
    assert normalize_phone("abc") == ""


def test_country_mapping():
    assert normalize_country("us") == "United States"
    assert normalize_country("U.K.") == "United Kingdom"
    assert normalize_country("viet nam") == "Vietnam"
    assert normalize_country(None) is None


def test_parse_date():
    assert parse_date("2025-08-15") == "2025-08-15"
    assert parse_date("08/15/2025") == "2025-08-15"
    assert parse_date("15/08/2025") == "2025-08-15"
    assert parse_date("bad") is None


def test_source_normalization():
    assert normalize_source("linkedin") == "LinkedIn"
    assert normalize_source("product signup") == "Product Signup"
    assert normalize_source("other channel") == "Other Channel"

