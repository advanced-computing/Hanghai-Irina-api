import pandas as pd
from flask import Flask, Response, jsonify, request

app = Flask(__name__)

# Load data once at startup
df = pd.read_csv("nyc_death_causes.csv")

# Create a stable unique id (lab wants unique identifiers)
df = df.reset_index(drop=True)
df["id"] = df.index


@app.route("/")
def home():
    return "NYC Death Causes API is running."


@app.route("/records")
def records():
    data = df.copy()

    # --- output format ---
    fmt = request.args.get("format", "json").lower()

    # --- limit & offset (pop them so they won't be treated as filters) ---
    limit = request.args.get("limit", default=None, type=int)
    offset = request.args.get("offset", default=0, type=int)

    # --- filtering by ANY column ---
    # Any query param that matches a column name will be used to filter.
    for key, value in request.args.items():
        if key in ["limit", "offset", "format"]:
            continue
        if key in data.columns:
            data = data[data[key].astype(str) == str(value)]

    # --- apply offset/limit ---
    if offset < 0:
        return jsonify({"error": "offset must be >= 0"}), 400
    if limit is not None and limit < 0:
        return jsonify({"error": "limit must be >= 0"}), 400

    if limit is None:
        data = data.iloc[offset:]
    else:
        data = data.iloc[offset : offset + limit]

    # --- return in requested format ---
    if fmt == "csv":
        return Response(data.to_csv(index=False), mimetype="text/csv")
    else:
        return Response(data.to_json(orient="records"), mimetype="application/json")


@app.route("/records/<int:record_id>")
def record_by_id(record_id):
    row = df[df["id"] == record_id]
    if row.empty:
        return jsonify({"error": "Record not found", "id": record_id}), 404
    return Response(row.to_json(orient="records"), mimetype="application/json")


if __name__ == "__main__":
    app.run(debug=True)
