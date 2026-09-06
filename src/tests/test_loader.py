import logging
import os
import tempfile
from decimal import Decimal

import pandas as pd
import pytest
from sqlalchemy import create_engine, text, inspect as sa_inspect


@pytest.fixture
def engine():
    return create_engine("sqlite:///:memory:")


def test_load_new_file(engine):
    from loader.loader import load_file
    from loader.db import create_metadata_tables
    create_metadata_tables(engine)
    df = pd.DataFrame({"bill_id": [1, 2], "amount": [100.0, 200.0]})
    stats = load_file(engine, df, "cancellation_bills", "cancel/test.xlsx", logger=None)
    assert stats["loaded"] == 2
    assert stats["skipped"] == 0


def test_load_adds_source_file_column(engine):
    from loader.loader import load_file
    from loader.db import create_metadata_tables
    create_metadata_tables(engine)
    df = pd.DataFrame({"bill_id": [1], "amount": [100.0]})
    load_file(engine, df, "cancellation_bills", "cancel/test.xlsx", logger=None)
    cols = [c["name"] for c in sa_inspect(engine).get_columns("cancellation_bills")]
    assert "source_file" in cols


def test_reload_deletes_old_rows(engine):
    from loader.loader import load_file
    from loader.db import create_metadata_tables
    create_metadata_tables(engine)
    df1 = pd.DataFrame({"bill_id": [1, 2], "amount": [100.0, 200.0]})
    load_file(engine, df1, "cancellation_bills", "cancel/test.xlsx", logger=None)
    df2 = pd.DataFrame({"bill_id": [3], "amount": [300.0]})
    load_file(engine, df2, "cancellation_bills", "cancel/test.xlsx", logger=None)
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM cancellation_bills")).scalar()
    assert count == 1


def test_ensure_schema_creates_all_text_columns(engine):
    """_ensure_table_schema must create columns as TEXT, never infer numeric types."""
    from loader.loader import load_file
    from loader.db import create_metadata_tables
    from sqlalchemy import inspect as sa_inspect
    create_metadata_tables(engine)
    df = pd.DataFrame({"amount": [1.0, 2.0], "note": ["a", "b"]})
    load_file(engine, df, "test_tbl", "f.xlsx", logger=None)
    col_types = {c["name"]: str(c["type"]).upper()
                 for c in sa_inspect(engine).get_columns("test_tbl")}
    assert "TEXT" in col_types["amount"]
    assert "TEXT" in col_types["note"]


def test_skip_row_on_bad_data(engine):
    from loader.loader import load_file
    from loader.db import create_metadata_tables
    create_metadata_tables(engine)
    with engine.connect() as conn:
        conn.execute(text("CREATE TABLE cancellation_bills (bill_id INTEGER, amount FLOAT, source_file TEXT)"))
        conn.commit()
    df = pd.DataFrame({"bill_id": [1, 2], "amount": ["not_a_number", 200.0]})
    stats = load_file(engine, df, "cancellation_bills", "cancel/test.xlsx", logger=None)
    assert stats["skipped"] >= 1


def _make_xlsx(tmp_dir, filename, columns):
    """Helper: create a minimal xlsx with given column headers."""
    path = os.path.join(tmp_dir, filename)
    pd.DataFrame({c: [] for c in columns}).to_excel(path, index=False)
    return path


def test_build_table_schemas_creates_tables(engine):
    from loader.loader import build_table_schemas
    from loader.db import create_metadata_tables, get_table_columns

    create_metadata_tables(engine)

    with tempfile.TemporaryDirectory() as tmp:
        f1 = _make_xlsx(tmp, "a.xlsx", ["Bill ID", "Amount"])
        f2 = _make_xlsx(tmp, "b.xlsx", ["Bill ID", "Note"])

        files = [
            {"file_path": f1, "rel_path": "cancel/a.xlsx", "table_name": "cancel_tbl"},
            {"file_path": f2, "rel_path": "cancel/b.xlsx", "table_name": "cancel_tbl"},
        ]

        class FakeCfg:
            table_header_map = {}

        logger = logging.getLogger("test")
        to_load, n_skipped = build_table_schemas(engine, files, FakeCfg(), logger)

    assert n_skipped == 0
    assert len(to_load) == 2
    cols = get_table_columns(engine, "cancel_tbl")
    assert "bill_id" in cols    # normalized
    assert "amount" in cols
    assert "note" in cols
    assert "source_file" in cols


def test_build_table_schemas_skips_bad_file(engine):
    from loader.loader import build_table_schemas
    from loader.db import create_metadata_tables

    create_metadata_tables(engine)

    files = [
        {"file_path": "/nonexistent/bad.xlsx", "rel_path": "cancel/bad.xlsx",
         "table_name": "cancel_tbl"},
    ]

    class FakeCfg:
        table_header_map = {}

    import logging
    logger = logging.getLogger("test")
    to_load, n_skipped = build_table_schemas(engine, files, FakeCfg(), logger)

    assert n_skipped == 1
    assert len(to_load) == 0


def test_load_large_file_bulk(engine):
    """Files with >500 rows should be inserted via bulk path."""
    from loader.loader import load_file
    from loader.db import create_metadata_tables
    create_metadata_tables(engine)
    df = pd.DataFrame({"bill_id": list(range(600)), "amount": [1.0] * 600})
    stats = load_file(engine, df, "cancellation_bills", "cancel/big.xlsx", logger=None)
    assert stats["loaded"] == 600
    assert stats["skipped"] == 0
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM cancellation_bills")).scalar()
    assert count == 600


def test_load_file_deduplicates_normalized_columns(engine):
    """Two raw columns normalizing to the same name must not cause duplicate-column INSERT error."""
    from loader.loader import load_file
    from loader.db import create_metadata_tables
    create_metadata_tables(engine)
    # "Note" and "Note " both normalize to "note" — first occurrence wins
    df = pd.DataFrame({"ID": [1, 2], "Note": ["a", "b"], "Note ": ["x", "y"]})
    stats = load_file(engine, df, "dedup_tbl", "test/dedup.xlsx", logger=None)
    assert stats["loaded"] == 2
    assert stats["skipped"] == 0


def test_load_file_strips_decimal_suffix_from_identifier_columns(engine):
    from loader.loader import load_file
    from loader.db import create_metadata_tables

    create_metadata_tables(engine)
    df = pd.DataFrame({"ID": [1.0, 2.0], "PID": [10.0, 20.0]})
    stats = load_file(engine, df, "id_tbl", "test/id.xlsx", logger=None)

    assert stats["loaded"] == 2
    with engine.connect() as conn:
        rows = conn.execute(text('SELECT "id", "pid" FROM id_tbl ORDER BY "id"')).fetchall()

    assert rows == [("1", "10"), ("2", "20")]


def test_load_file_normalizes_identifier_strings_and_decimals(engine):
    from loader.loader import load_file
    from loader.db import create_metadata_tables

    create_metadata_tables(engine)
    df = pd.DataFrame({
        "customer_id": [Decimal("5124.0"), "201299717.0"],
        "pid": [".0", " 201161672.0 "],
    })

    stats = load_file(engine, df, "id_tbl2", "test/id2.xlsx", logger=None)

    assert stats["loaded"] == 2
    with engine.connect() as conn:
        rows = conn.execute(text('SELECT "customer_id", "pid" FROM id_tbl2')).fetchall()

    assert set(rows) == {("5124", None), ("201299717", "201161672")}


def test_load_file_strips_decimal_suffix_from_live_identifier_patterns(engine):
    from loader.loader import load_file
    from loader.db import create_metadata_tables

    create_metadata_tables(engine)
    df = pd.DataFrame({
        "id_khach_hang": [1416.0, "8246.0"],
        "pid_kh_gioi_thieu": ["813014845.0", "812016605.0"],
        "ma_nv": ["3513423.0", "0.0"],
        "stt_theo_kh": [3.0, 2.0],
        "so_tien": [100.5, 200.75],
    })

    stats = load_file(engine, df, "identifier_patterns", "test/patterns.xlsx", logger=None)

    assert stats["loaded"] == 2
    with engine.connect() as conn:
        rows = conn.execute(text(
            'SELECT "id_khach_hang", "pid_kh_gioi_thieu", "ma_nv", "stt_theo_kh", "so_tien" '
            'FROM identifier_patterns'
        )).fetchall()

    normalized = {
        str(row[0]): (str(row[1]), str(row[2]), str(row[3]), float(row[4]))
        for row in rows
    }
    assert normalized == {
        "1416": ("813014845", "3513423", "3", 100.5),
        "8246": ("812016605", "0", "2", 200.75),
    }


def test_load_file_normalizes_relative_info_numeric_suffix(engine):
    from loader.loader import load_file
    from loader.db import create_metadata_tables

    create_metadata_tables(engine)
    df = pd.DataFrame({
        "thong_tin_nguoi_than": [
            "837008503.0",
            814031512.0,
            " 12345.000 ",
            "Chong: Nguyen Van A - SDT: 0912",
        ],
    })

    stats = load_file(engine, df, "customer_data_like", "test/relative_info.xlsx", logger=None)
    assert stats["loaded"] == 4

    with engine.connect() as conn:
        rows = conn.execute(text(
            'SELECT "thong_tin_nguoi_than" FROM customer_data_like ORDER BY "thong_tin_nguoi_than"'
        )).fetchall()

    assert set(r[0] for r in rows) == {
        "837008503",
        "814031512",
        "12345",
        "Chong: Nguyen Van A - SDT: 0912",
    }


def test_load_file_fallback_dot_zero_text_to_empty(engine):
    from loader.loader import load_file
    from loader.db import create_metadata_tables

    create_metadata_tables(engine)
    df = pd.DataFrame({
        "ly_do_huy": [".0", "huy", " x "],
    })

    stats = load_file(engine, df, "cancel_like", "test/cancel_like.xlsx", logger=None)
    assert stats["loaded"] == 3

    with engine.connect() as conn:
        rows = conn.execute(text('SELECT "ly_do_huy" FROM cancel_like')).fetchall()

    assert set(r[0] for r in rows) == {"", "huy", " x "}


def test_coerce_excel_serial_into_date_column():
    """After init upgrades ngay_sinh to DATE, daily must not push raw Excel serials at it."""
    from datetime import date
    from sqlalchemy import types as sa_types
    from loader.loader import _coerce_value

    assert _coerce_value(33330, sa_types.DATE(), "ngay_sinh") == date(1991, 4, 2)
    assert _coerce_value("33330", sa_types.DATE(), "ngay_sinh") == date(1991, 4, 2)
    assert _coerce_value("1991-04-02", sa_types.DATE(), "ngay_sinh") == date(1991, 4, 2)
    assert _coerce_value("02/04/1991", sa_types.DATE(), "ngay_sinh") == date(1991, 4, 2)
    assert _coerce_value("19910402", sa_types.DATE(), "ngay_sinh") == date(1991, 4, 2)
    # unparseable and out-of-range values become NULL instead of failing the row
    assert _coerce_value("khong-phai-ngay", sa_types.DATE(), "ngay_sinh") is None
    assert _coerce_value(3_000_000_000, sa_types.DATE(), "ngay_sinh") is None


def test_coerce_timestamp_column_keeps_time():
    from datetime import datetime
    from sqlalchemy import types as sa_types
    from loader.loader import _coerce_value

    got = _coerce_value("2026-09-04 07:50:39.31", sa_types.TIMESTAMP(), "ngay_tao")
    assert got == datetime(2026, 9, 4, 7, 50, 39, 310000)


def test_load_file_into_date_column(engine):
    """Reloading a file whose date column is already typed must still insert rows."""
    from loader.loader import load_file
    from loader.db import create_metadata_tables

    create_metadata_tables(engine)
    with engine.connect() as conn:
        conn.execute(text('CREATE TABLE cust (id INTEGER, ngay_sinh DATE, source_file TEXT)'))
        conn.commit()

    df = pd.DataFrame({"ID": [1, 2], "Ngày sinh": [33330, 33331]})
    stats = load_file(engine, df, "cust", "customer_data/c.xlsx", logger=None)

    assert stats["loaded"] == 2
    assert stats["status"] == "success"
    with engine.connect() as conn:
        rows = conn.execute(text('SELECT "ngay_sinh" FROM cust ORDER BY "id"')).fetchall()
    # str() so the assert holds for both SQLite (text) and PostgreSQL (date objects)
    assert [str(r[0]) for r in rows] == ["1991-04-02", "1991-04-03"]


def test_failed_reload_keeps_previous_rows(engine):
    """If every row of a re-exported file is rejected, the old rows must survive."""
    from loader.loader import load_file
    from loader.db import create_metadata_tables

    create_metadata_tables(engine)
    with engine.connect() as conn:
        conn.execute(text('CREATE TABLE bills (bill_id INTEGER, amount FLOAT, source_file TEXT)'))
        conn.commit()

    good = pd.DataFrame({"bill_id": [1, 2], "amount": [100.0, 200.0]})
    assert load_file(engine, good, "bills", "cancel/x.xlsx", logger=None)["loaded"] == 2

    broken = pd.DataFrame({"bill_id": [3, 4], "amount": ["nope", "nope"]})
    stats = load_file(engine, broken, "bills", "cancel/x.xlsx", logger=None)

    assert stats["loaded"] == 0
    assert stats["status"] == "failed"
    with engine.connect() as conn:
        rows = conn.execute(text('SELECT "bill_id" FROM bills ORDER BY "bill_id"')).fetchall()
    assert [r[0] for r in rows] == [1, 2]


def test_partial_reload_keeps_the_rows_that_work(engine):
    """One bad row must not take the rest of the file down with it."""
    from loader.loader import load_file
    from loader.db import create_metadata_tables

    create_metadata_tables(engine)
    with engine.connect() as conn:
        conn.execute(text('CREATE TABLE bills2 (bill_id INTEGER, amount FLOAT, source_file TEXT)'))
        conn.commit()

    df = pd.DataFrame({"bill_id": [1, 2, 3], "amount": [100.0, "nope", 300.0]})
    stats = load_file(engine, df, "bills2", "cancel/y.xlsx", logger=None)

    assert stats["loaded"] == 2
    assert stats["skipped"] == 1
    assert stats["status"] == "partial"


def test_load_file_logs_insert_operation(engine):
    from loader.loader import load_file
    from loader.db import create_metadata_tables
    create_metadata_tables(engine)
    df = pd.DataFrame({"bill_id": [1, 2], "amount": [100.0, 200.0]})
    load_file(engine, df, "cancellation_bills", "cancel/test.xlsx", logger=None)
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT operation FROM _load_metadata
            WHERE file_path = 'cancel/test.xlsx'
            ORDER BY last_loaded_at DESC LIMIT 1
        """)).fetchone()
    assert row[0] == "INSERT"


def test_load_file_logs_update_operation_on_reload(engine):
    from loader.loader import load_file
    from loader.db import create_metadata_tables
    create_metadata_tables(engine)
    df1 = pd.DataFrame({"bill_id": [1], "amount": [100.0]})
    load_file(engine, df1, "cancellation_bills", "cancel/test.xlsx", logger=None)
    df2 = pd.DataFrame({"bill_id": [2], "amount": [200.0]})
    load_file(engine, df2, "cancellation_bills", "cancel/test.xlsx", logger=None)
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT operation FROM _load_metadata
            WHERE file_path = 'cancel/test.xlsx'
            ORDER BY last_loaded_at ASC
        """)).fetchall()
    ops = [r[0] for r in rows]
    assert ops[0] == "INSERT"
    assert ops[1] == "UPDATE"


