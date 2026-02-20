import pandas as pd
from dollars_helper import parse_dollars


def test_parse_dollars():
    amounts = pd.Series(["$1,000.00", "$2,500.50", "$3,000"])
    expected = pd.Series([1000.00, 2500.50, 3000.00])
    result = parse_dollars(amounts)
    assert result.equals(expected), f"Expected {expected} but got {result}"

