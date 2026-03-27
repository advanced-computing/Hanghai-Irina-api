from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_DB_PATH = BASE_DIR / "cpi.duckdb"
DEFAULT_SOURCE_DIR = BASE_DIR / "data"

VINTAGE_RELEASES = {
    "2024-01-01": "PCPI24M1.csv",
    "2025-02-01": "PCPI25M2.csv",
}

TABLE_NAMES = {
    "append": "cpi_append",
    "trunc": "cpi_trunc",
    "inc": "cpi_inc",
}


def normalize_cpi_frame(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(col).lstrip("\ufeff").strip().lower() for col in out.columns]
    out["date"] = out["date"].astype(str).str.strip()
    out["cpi"] = pd.to_numeric(out["cpi"], errors="coerce")
    out = out.rename(columns={"date": "dates"})
    return out[["dates", "cpi"]]


def load_vintage_file(csv_path: Path) -> pd.DataFrame:
    if not csv_path.exists():
        raise FileNotFoundError(f"Missing CPI source file: {csv_path}")
    df = pd.read_csv(csv_path)
    return normalize_cpi_frame(df)


def get_latest_data(
    pull_date: str | pd.Timestamp,
    source_dir: Path = DEFAULT_SOURCE_DIR,
) -> tuple[pd.DataFrame, str]:
    pull_ts = pd.Timestamp(pull_date).normalize()

    available = []
    for release_date, file_name in VINTAGE_RELEASES.items():
        release_ts = pd.Timestamp(release_date)
        if release_ts <= pull_ts:
            available.append((release_ts, file_name))

    if not available:
        raise ValueError(f"No CPI vintage is available for pull_date={pull_date!r}")

    release_ts, file_name = max(available, key=lambda item: item[0])
    csv_path = source_dir / file_name
    df = load_vintage_file(csv_path)
    return df, release_ts.strftime("%Y-%m-%d")


def connect(db_path: Path = DEFAULT_DB_PATH) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(db_path))


def create_tables(db_path: Path = DEFAULT_DB_PATH) -> None:
    with connect(db_path) as con:
        for table_name in TABLE_NAMES.values():
            con.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    dates TEXT PRIMARY KEY,
                    cpi DOUBLE,
                    source_vintage TEXT,
                    loaded_at TIMESTAMP
                )
                """
            )


def replace_table(
    table_name: str,
    data: pd.DataFrame,
    vintage_label: str,
    db_path: Path = DEFAULT_DB_PATH,
) -> None:
    payload = data.copy()
    payload["source_vintage"] = vintage_label
    payload["loaded_at"] = pd.Timestamp.now()

    with connect(db_path) as con:
        con.register("incoming_data", payload)
        con.execute(f"DELETE FROM {table_name}")
        con.execute(
            f"""
            INSERT INTO {table_name} (dates, cpi, source_vintage, loaded_at)
            SELECT dates, cpi, source_vintage, loaded_at
            FROM incoming_data
            """
        )


def load_append(
    pull_date: str | pd.Timestamp,
    db_path: Path = DEFAULT_DB_PATH,
    source_dir: Path = DEFAULT_SOURCE_DIR,
) -> str:
    data, vintage_label = get_latest_data(pull_date, source_dir=source_dir)
    payload = data.copy()
    payload["source_vintage"] = vintage_label
    payload["loaded_at"] = pd.Timestamp.now()

    with connect(db_path) as con:
        con.register("incoming_data", payload)
        con.execute(
            f"""
            INSERT INTO {TABLE_NAMES["append"]} (dates, cpi, source_vintage, loaded_at)
            SELECT incoming_data.dates, incoming_data.cpi, incoming_data.source_vintage, incoming_data.loaded_at
            FROM incoming_data
            WHERE NOT EXISTS (
                SELECT 1
                FROM {TABLE_NAMES["append"]} current_rows
                WHERE current_rows.dates = incoming_data.dates
            )
            """
        )

    return vintage_label


def load_trunc(
    pull_date: str | pd.Timestamp,
    db_path: Path = DEFAULT_DB_PATH,
    source_dir: Path = DEFAULT_SOURCE_DIR,
) -> str:
    data, vintage_label = get_latest_data(pull_date, source_dir=source_dir)
    replace_table(TABLE_NAMES["trunc"], data, vintage_label, db_path=db_path)
    return vintage_label


def load_incremental(
    pull_date: str | pd.Timestamp,
    db_path: Path = DEFAULT_DB_PATH,
    source_dir: Path = DEFAULT_SOURCE_DIR,
) -> str:
    data, vintage_label = get_latest_data(pull_date, source_dir=source_dir)
    payload = data.copy()
    payload["source_vintage"] = vintage_label
    payload["loaded_at"] = pd.Timestamp.now()

    with connect(db_path) as con:
        con.register("incoming_data", payload)
        con.execute(
            f"""
            INSERT INTO {TABLE_NAMES["inc"]} (dates, cpi, source_vintage, loaded_at)
            SELECT dates, cpi, source_vintage, loaded_at
            FROM incoming_data
            ON CONFLICT (dates) DO UPDATE SET
                cpi = EXCLUDED.cpi,
                source_vintage = EXCLUDED.source_vintage,
                loaded_at = EXCLUDED.loaded_at
            """
        )

    return vintage_label


def initialize_tables(
    pull_date: str = "2024-01-15",
    db_path: Path = DEFAULT_DB_PATH,
    source_dir: Path = DEFAULT_SOURCE_DIR,
) -> str:
    create_tables(db_path)
    data, vintage_label = get_latest_data(pull_date, source_dir=source_dir)
    for table_name in TABLE_NAMES.values():
        replace_table(table_name, data, vintage_label, db_path=db_path)
    return vintage_label


def fetch_table(table_name: str, db_path: Path = DEFAULT_DB_PATH) -> pd.DataFrame:
    with connect(db_path) as con:
        return con.execute(
            f"SELECT * FROM {table_name} ORDER BY dates"
        ).fetchdf()


def run_method(
    method: str,
    pull_date: str,
    db_path: Path = DEFAULT_DB_PATH,
    source_dir: Path = DEFAULT_SOURCE_DIR,
) -> str:
    if method == "append":
        return load_append(pull_date, db_path=db_path, source_dir=source_dir)
    if method == "trunc":
        return load_trunc(pull_date, db_path=db_path, source_dir=source_dir)
    if method == "inc":
        return load_incremental(pull_date, db_path=db_path, source_dir=source_dir)
    raise ValueError(f"Unknown method: {method}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Lab 8 CPI loading helper")
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser("init", help="Initialize all three tables")
    init_parser.add_argument("--pull-date", default="2024-01-15")
    init_parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH))
    init_parser.add_argument("--source-dir", default=str(DEFAULT_SOURCE_DIR))

    load_parser = subparsers.add_parser("load", help="Run one loading method")
    load_parser.add_argument("--method", choices=["append", "trunc", "inc"], required=True)
    load_parser.add_argument("--pull-date", required=True)
    load_parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH))
    load_parser.add_argument("--source-dir", default=str(DEFAULT_SOURCE_DIR))

    show_parser = subparsers.add_parser("show", help="Print one table")
    show_parser.add_argument("--table", choices=list(TABLE_NAMES.values()), required=True)
    show_parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH))

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "init":
        vintage = initialize_tables(
            pull_date=args.pull_date,
            db_path=Path(args.db_path),
            source_dir=Path(args.source_dir),
        )
        print(f"Initialized all tables from vintage {vintage}")
        return

    if args.command == "load":
        vintage = run_method(
            args.method,
            args.pull_date,
            db_path=Path(args.db_path),
            source_dir=Path(args.source_dir),
        )
        print(f"Loaded method={args.method} from vintage {vintage}")
        return

    if args.command == "show":
        df = fetch_table(args.table, db_path=Path(args.db_path))
        print(df.to_string(index=False))


if __name__ == "__main__":
    main()
