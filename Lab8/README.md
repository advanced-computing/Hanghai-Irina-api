# Lab 8

This folder contains a complete "simplified version" solution for Lab 8 using DuckDB.

## Files

- `lab8.py`: core loading logic
- `run_lab8_demo.py`: manual demo script
- `data/`: repo-local copies of the two CPI source files
- `cpi.duckdb`: DuckDB database created after running the scripts

## Data source

The scripts use repo-local copies of the course sample files:

- `Lab8/data/PCPI24M1.csv`
- `Lab8/data/PCPI25M2.csv`

These two files behave like two CPI vintages:

- `2024-01-01` -> `PCPI24M1.csv`
- `2025-02-01` -> `PCPI25M2.csv`

`get_latest_data(pull_date)` returns the latest available file up to the requested date.

## How to run

From the repository root:

```bash
python3 Lab8/lab8.py init --pull-date 2024-01-15
python3 Lab8/lab8.py load --method append --pull-date 2025-02-15
python3 Lab8/lab8.py load --method trunc --pull-date 2025-02-15
python3 Lab8/lab8.py load --method inc --pull-date 2025-02-15
```

You can also run the combined demo:

```bash
python3 Lab8/run_lab8_demo.py
```

## Manual testing expectations

After `init --pull-date 2024-01-15`:

- `cpi_append`, `cpi_trunc`, and `cpi_inc` should all contain the January 2024 vintage
- each table should have 924 data rows
- the latest date should be `2023:12`

After loading the February 2025 vintage:

- `cpi_append` should keep older historical values and only append new dates
- `cpi_trunc` should exactly match the new file
- `cpi_inc` should also match the new file because revised rows are updated in place

Expected row counts after the update:

- `cpi_append`: 937 rows
- `cpi_trunc`: 937 rows
- `cpi_inc`: 937 rows

Expected behavior differences:

- `append` adds the 13 new dates from `2024:01` to `2025:01`, but it does not fix historical revisions
- `trunc` deletes everything and reloads the full latest dataset
- `incremental` updates revised rows and inserts new rows without reloading the whole table

Known revision examples between the two files:

- `2019:01` changes from `252.7` to `252.6`
- `2022:06` changes from `294.7` to `295.1`
- `2023:12` changes from `308.9` to `308.7`

## Suggested discussion

`append` is fast and simple, but it misses backfilled corrections.

`trunc` is easy to reason about and guarantees consistency with the latest file, but it rewrites the full table every time.

`incremental` is usually the best warehouse pattern here because it keeps the table current while only changing rows that are new or revised.
