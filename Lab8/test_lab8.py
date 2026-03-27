from __future__ import annotations

from pathlib import Path

import duckdb

from Lab8.lab8 import get_latest_data, initialize_tables, run_method


def test_get_latest_data_chooses_expected_vintage() -> None:
    early_df, early_vintage = get_latest_data("2024-01-15")
    later_df, later_vintage = get_latest_data("2025-02-15")

    assert early_vintage == "2024-01-01"
    assert later_vintage == "2025-02-01"
    assert len(early_df) == 924
    assert len(later_df) == 937


def test_loading_methods_have_expected_behavior(tmp_path: Path) -> None:
    db_path = tmp_path / "cpi.duckdb"

    initialize_tables("2024-01-15", db_path=db_path)
    run_method("append", "2025-02-15", db_path=db_path)
    run_method("trunc", "2025-02-15", db_path=db_path)
    run_method("inc", "2025-02-15", db_path=db_path)

    con = duckdb.connect(str(db_path))

    assert con.execute("SELECT COUNT(*) FROM cpi_append").fetchone()[0] == 937
    assert con.execute("SELECT COUNT(*) FROM cpi_trunc").fetchone()[0] == 937
    assert con.execute("SELECT COUNT(*) FROM cpi_inc").fetchone()[0] == 937

    assert con.execute(
        "SELECT cpi FROM cpi_append WHERE dates = '2023:12'"
    ).fetchone()[0] == 308.9
    assert con.execute(
        "SELECT cpi FROM cpi_trunc WHERE dates = '2023:12'"
    ).fetchone()[0] == 308.7
    assert con.execute(
        "SELECT cpi FROM cpi_inc WHERE dates = '2023:12'"
    ).fetchone()[0] == 308.7

    run_method("append", "2025-02-15", db_path=db_path)
    run_method("inc", "2025-02-15", db_path=db_path)

    assert con.execute("SELECT COUNT(*) FROM cpi_append").fetchone()[0] == 937
    assert con.execute("SELECT COUNT(*) FROM cpi_inc").fetchone()[0] == 937
