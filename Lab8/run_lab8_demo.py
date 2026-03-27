from __future__ import annotations

from lab8 import DEFAULT_DB_PATH, TABLE_NAMES, fetch_table, initialize_tables, run_method


def summarize_table(table_name: str) -> None:
    df = fetch_table(table_name, db_path=DEFAULT_DB_PATH)
    print(f"\n{table_name}")
    print(f"rows: {len(df)}")
    print("tail:")
    print(df.tail(5).to_string(index=False))


def main() -> None:
    initialize_tables(pull_date="2024-01-15", db_path=DEFAULT_DB_PATH)
    print("Initialized all tables with the January 2024 vintage.")

    for method in ("append", "trunc", "inc"):
        run_method(method, "2025-02-15", db_path=DEFAULT_DB_PATH)

    print("Applied the February 2025 vintage with append, trunc, and incremental loading.")

    for table_name in TABLE_NAMES.values():
        summarize_table(table_name)


if __name__ == "__main__":
    main()
