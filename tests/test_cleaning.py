import pytest
from src.cleaning import parse_dollars

def test_parse_dollars_basic():
    assert parse_dollars("$1,234.50") == 1234.50
    assert parse_dollars("99") == 99.0

def test_parse_dollars_none():
    assert parse_dollars("") is None
    assert parse_dollars(None) is None

def test_parse_dollars_bad():
    with pytest.raises(ValueError):
        parse_dollars("$12a")