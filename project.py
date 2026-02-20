from flask import Flask, request, jsonify, Response
import pandas as pd

app = Flask(__name__)

CSV_PATH = "data/nyc_death_causes.csv"  
ID_COL = "record_id"             


def load_df() -> pd.DataFrame:
    return pd.read_csv(CSV_PATH)


def format_output(df: pd.DataFrame, fmt: str):
    fmt = (fmt or "json").lower()
    if fmt == "csv":
        return Response(df.to_csv(index=False), mimetype="text/csv")
    return jsonify(df.to_dict(orient="records"))


@app.get("/")
def home():
    return jsonify(
        {
            "message": "NYC Leading Causes of Death API",
            "endpoints": ["/records", "/records/<record_id>"],
        }
    )


@app.get("/records")
def list_records():
    df = load_df()

    # filters: any query param that matches a column name (except reserved)
    reserved = {"format", "limit", "offset"}
    for key, value in request.args.items():
        if key in reserved:
            continue
        if key not in df.columns:
            return jsonify({"error": f"unknown filter column: {key}"}), 400
        df = df[df[key].astype(str) == str(value)]

    # pagination
    offset = request.args.get("offset", default=0, type=int)
    limit = request.args.get("limit", default=None, type=int)

    if offset < 0:
        offset = 0
    if limit is not None and limit < 0:
        limit = None

    if limit is None:
        df = df.iloc[offset:]
    else:
        df = df.iloc[offset : offset + limit]

    # output format
    fmt = request.args.get("format", "json")
    return format_output(df, fmt)


@app.get("/records/<record_id>")
def get_record(record_id):
    df = load_df()
    one = df[df[ID_COL].astype(str) == str(record_id)]
    if one.empty:
        return jsonify({"error": "not found"}), 404

    fmt = request.args.get("format", "json")
    if (fmt or "json").lower() == "csv":
        return Response(one.to_csv(index=False), mimetype="text/csv")

    return jsonify(one.iloc[0].to_dict())


if __name__ == "__main__":
    app.run(debug=True)