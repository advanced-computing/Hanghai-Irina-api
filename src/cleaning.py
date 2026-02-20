import re

def parse_dollars(x):
    if x is None:
        return None
    s = str(x).strip()
    if s == "" or s.lower() in {"nan", "none"}:
        return None
    s = s.replace(",", "")
    m = re.fullmatch(r"\$?(-?\d+(\.\d+)?)", s)
    if not m:
        raise ValueError(f"bad dollar amount: {x}")
    return float(m.group(1))