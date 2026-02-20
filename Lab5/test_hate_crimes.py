# tests/test_hate_crimes.py
import pandas as pd
import pytest
from hate_crimes import clean_hate_crimes, load_hate_crimes, summarize_by_year

CSV_PATH = "Lab5/NYPD_Hate_Crimes_20260220.csv"

def test_load_has_required_columns():
    df = load_hate_crimes(CSV_PATH)
    assert df.shape[0] > 0
    assert "Bias Motive Description" in df.columns
    assert "Complaint Year Number" in df.columns


def test_clean_parses_record_create_date():
    df = load_hate_crimes(CSV_PATH)
    cleaned = clean_hate_crimes(df)
    assert pd.api.types.is_datetime64_any_dtype(cleaned["Record Create Date"])


def test_month_in_range_1_to_12():
    df = clean_hate_crimes(load_hate_crimes(CSV_PATH))
    months = df["Month Number"].dropna()
    assert months.between(1, 12).all()


def test_year_reasonable_range():
    df = clean_hate_crimes(load_hate_crimes(CSV_PATH))
    years = df["Complaint Year Number"].dropna()
    assert (years >= 2000).all()
    assert (years <= 2100).all()


def test_summary_by_year_counts_match_rows():
    df = clean_hate_crimes(load_hate_crimes(CSV_PATH))
    summary = summarize_by_year(df)
    assert int(summary.sum()) == len(df)
