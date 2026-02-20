import pandas as pd

CSV_PATH = "nyc_death_causes.csv"

def load_df():
    return pd.read_csv(CSV_PATH)

def test_required_columns_exist():
    df = load_df()
    required = [
        "Year",
        "Leading Cause",
        "Sex",
        "Race Ethnicity",
        "Deaths",
        "Death Rate",
        "Age Adjusted Death Rate",
    ]
    for c in required:
        assert c in df.columns

def test_year_is_numeric_and_reasonable():
    df = load_df()
    year = pd.to_numeric(df["Year"], errors="coerce")
    assert year.notna().all()
    assert year.between(1900, 2100).all()

def test_deaths_non_negative_when_present():
    df = load_df()
    deaths = pd.to_numeric(df["Deaths"], errors="coerce")
    assert (deaths.dropna() >= 0).all()

def test_deaths_missing_rate_not_too_high():
    df = load_df()
    deaths = pd.to_numeric(df["Deaths"], errors="coerce")
    missing_rate = deaths.isna().mean()
    assert missing_rate <= 0.20


