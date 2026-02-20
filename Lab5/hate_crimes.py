# hate_crimes.py
from __future__ import annotations

import pandas as pd

REQUIRED_COLS = {
    "Full Complaint ID",
    "Complaint Year Number",
    "Month Number",
    "Record Create Date",
    "Complaint Precinct Code",
    "Patrol Borough Name",
    "County",
    "Law Code Category Description",
    "Offense Description",
    "PD Code Description",
    "Bias Motive Description",
    "Offense Category",
    "Arrest Date",
    "Arrest Id",
}

def load_hate_crimes(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)

    missing = REQUIRED_COLS - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    return df


def clean_hate_crimes(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    # parse dates
    out["Record Create Date"] = pd.to_datetime(out["Record Create Date"], errors="coerce")

    # numeric columns (coerce keeps weird strings from crashing)
    out["Complaint Year Number"] = pd.to_numeric(out["Complaint Year Number"], errors="coerce")
    out["Month Number"] = pd.to_numeric(out["Month Number"], errors="coerce")
    out["Complaint Precinct Code"] = pd.to_numeric(out["Complaint Precinct Code"], errors="coerce")

    return out


def summarize_by_year(df: pd.DataFrame) -> pd.Series:
    # returns counts per year
    return df.groupby("Complaint Year Number")["Full Complaint ID"].count().sort_index()
