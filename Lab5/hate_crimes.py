from __future__ import annotations

import duckdb
import pandas as pd
from flask import Flask, jsonify, request

DB_PATH = "nypd_hate_crimes.db"
TABLE_NAME = "hate_crimes"

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

app = Flask(__name__)


def load_hate_crimes() -> pd.DataFrame:
    with duckdb.connect(DB_PATH) as con:
        df = con.execute(f"SELECT * FROM {TABLE_NAME}").fetchdf()

    missing = REQUIRED_COLS - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    return df


def clean_hate_crimes(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    out["Record Create Date"] = pd.to_datetime(
        out["Record Create Date"], errors="coerce"
    )

    out["Complaint Year Number"] = pd.to_numeric(
        out["Complaint Year Number"], errors="coerce"
    )
    out["Month Number"] = pd.to_numeric(
        out["Month Number"], errors="coerce"
    )
    out["Complaint Precinct Code"] = pd.to_numeric(
        out["Complaint Precinct Code"], errors="coerce"
    )

    return out


def summarize_by_year(df: pd.DataFrame) -> pd.Series:
    return df.groupby("Complaint Year Number")["Full Complaint ID"].count().sort_index()


@app.route("/", methods=["GET"])
def home():
    return jsonify({"message": "NYPD Hate Crimes API"})


@app.route("/hate-crimes", methods=["GET"])
def get_hate_crimes():
    year = request.args.get("year")
    county = request.args.get("county")
    limit = request.args.get("limit", default=100, type=int)

    query = f"SELECT * FROM {TABLE_NAME}"
    conditions = []
    params = []

    if year is not None:
        conditions.append('"Complaint Year Number" = ?')
        params.append(int(year))

    if county is not None:
        conditions.append('"County" = ?')
        params.append(county)

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    query += " LIMIT ?"
    params.append(limit)

    with duckdb.connect(DB_PATH) as con:
        df = con.execute(query, params).fetchdf()

    return jsonify(df.to_dict(orient="records"))


@app.route("/hate-crimes/summary-by-year", methods=["GET"])
def get_summary_by_year():
    query = f'''
        SELECT
            "Complaint Year Number" AS complaint_year,
            COUNT("Full Complaint ID") AS complaint_count
        FROM {TABLE_NAME}
        GROUP BY "Complaint Year Number"
        ORDER BY "Complaint Year Number"
    '''

    with duckdb.connect(DB_PATH) as con:
        df = con.execute(query).fetchdf()

    return jsonify(df.to_dict(orient="records"))


@app.route("/users", methods=["POST"])
def add_user():
    data = request.get_json()

    if not data:
        return jsonify({"error": "Request body must be JSON"}), 400

    username = data.get("username")
    age = data.get("age")
    country = data.get("country")

    if not username or age is None or not country:
        return jsonify(
            {"error": "username, age, and country are required"}
        ), 400

    with duckdb.connect(DB_PATH) as con:
        con.execute(
            """
            INSERT INTO users (username, age, country)
            VALUES (?, ?, ?)
            """,
            [username, age, country],
        )

    return jsonify({"message": "User added successfully"}), 201


@app.route("/users/stats", methods=["GET"])
def get_user_stats():
    with duckdb.connect(DB_PATH) as con:
        total_users = con.execute(
            "SELECT COUNT(*) FROM users"
        ).fetchone()[0]

        average_age = con.execute(
            "SELECT AVG(age) FROM users"
        ).fetchone()[0]

        top_countries = con.execute(
            """
            SELECT country, COUNT(*) AS user_count
            FROM users
            GROUP BY country
            ORDER BY user_count DESC
            LIMIT 3
            """
        ).fetchdf()

    return jsonify(
        {
            "total_users": total_users,
            "average_age": average_age,
            "top_countries": top_countries.to_dict(orient="records"),
        }
    )


if __name__ == "__main__":
    app.run(debug=True)