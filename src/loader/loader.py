import re
import unicodedata
from collections import defaultdict
from datetime import date, datetime, timedelta

import pandas as pd
from sqlalchemy import Engine, inspect, text, types as sa_types

from loader.db import (
    add_column,
    create_table_with_columns,
    get_table_columns,
    is_file_loaded,
    insert_load_metadata,
    _uuid_col_def,
)


def normalize_col_name(name: str) -> str:
    name = str(name).replace("Đ", "D").replace("đ", "d")
    name = unicodedata.normalize("NFD", name)
    name = "".join(c for c in name if unicodedata.category(c) != "Mn")
    name = name.lower()
    name = name.replace("%", "per")
    name = name.replace("(", "").replace(")", "")
    name = re.sub(r"[^a-z0-9]+", "_", name)
    name = name.strip("_")
    return name or "col"


def _safe_col(name: str) -> str:
    return name.replace('"', '').replace("'", "").strip()


def _is_identifier_col(col_name: str | None) -> bool:
    if not col_name:
        return False
    lname = col_name.lower()
    return (
        lname == "id"
        or lname.startswith("id_")
        or lname.endswith("_id")
        or lname == "pid"
        or lname.startswith("pid_")
        or lname.endswith("_pid")
        or lname.startswith("ma_")
        or lname.startswith("stt_")
    )


def _is_relative_info_col(col_name: str | None) -> bool:
    return bool(col_name and col_name.lower() == "thong_tin_nguoi_than")


# Excel stores dates as days since 1899-12-30 (the 1900 leap-year bug included).
_EXCEL_EPOCH = date(1899, 12, 30)
_MIN_VALID_DATE = date(1900, 1, 1)

# Columns where a future value is meaningless. Mirrors the CURRENT_DATE guard in
# db._upgrade_birth_date_column so an init-loaded row and a daily-loaded row of
# the same file end up with the same value.
_BIRTH_DATE_COLS = {"ngay_sinh"}

_DATE_TEXT_FORMATS = (
    (re.compile(r"^\d{8}$"), "%Y%m%d"),
    (re.compile(r"^\d{2}/\d{2}/\d{4}$"), "%d/%m/%Y"),
)
_ISO_DATETIME_RE = re.compile(r"^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}:\d{2}(\.\d+)?)?$")
_NUMERIC_TEXT_RE = re.compile(r"^[+-]?\d+(?:\.\d+)?$")


def _excel_serial_to_datetime(value) -> datetime | None:
    try:
        serial = float(value)
    except (TypeError, ValueError):
        return None
    if serial <= 0:
        return None
    whole_days = int(serial)
    seconds = round((serial - whole_days) * 86400)
    try:
        return datetime.combine(_EXCEL_EPOCH + timedelta(days=whole_days), datetime.min.time()) \
            + timedelta(seconds=seconds)
    except (OverflowError, OSError, ValueError):
        # Nonsense serial (out of the representable date range) → NULL, same as
        # the ELSE NULL branch of the SQL upgrade. Never kill the row over it.
        return None


def _to_datetime(v) -> datetime | None:
    """Excel serial / datetime / common text formats → datetime, None if unparseable."""
    if isinstance(v, datetime):          # pandas Timestamp subclasses datetime
        return v
    if isinstance(v, date):              # must come after datetime — datetime is a date
        return datetime.combine(v, datetime.min.time())
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return _excel_serial_to_datetime(v)
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        for pattern, fmt in _DATE_TEXT_FORMATS:
            if pattern.match(s):
                try:
                    return datetime.strptime(s, fmt)
                except ValueError:
                    return None
        if _ISO_DATETIME_RE.match(s):
            try:
                return datetime.fromisoformat(s)
            except ValueError:
                # fromisoformat is strict about fractional-second width before 3.11
                for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S.%f"):
                    try:
                        return datetime.strptime(s, fmt)
                    except ValueError:
                        continue
                return None
        if _NUMERIC_TEXT_RE.match(s):
            return _excel_serial_to_datetime(s)
        return None
    # Decimal and other numeric-ish objects
    return _excel_serial_to_datetime(v)


def _coerce_date_like(v, sa_type, col_name: str | None):
    """Coerce a value into what a DATE / TIMESTAMP column will accept."""
    dt = _to_datetime(v)
    if dt is None:
        return None
    if isinstance(sa_type, sa_types.DateTime):
        return dt
    d = dt.date()
    if col_name in _BIRTH_DATE_COLS and not (_MIN_VALID_DATE <= d <= date.today()):
        return None
    return d


def _coerce_value(v, sa_type, col_name: str | None = None) -> object:
    """Return v coerced to the SQLAlchemy column type, or None for NaN/NA."""
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    if _is_relative_info_col(col_name):
        if isinstance(v, str):
            stripped = v.strip()
            if re.fullmatch(r"\d+\.0+", stripped):
                return stripped.split(".", 1)[0]
            return stripped
        if isinstance(v, bool):
            return v
        try:
            numeric = float(v)
        except (TypeError, ValueError):
            return v
        else:
            if numeric.is_integer():
                return str(int(numeric))
            return str(v)
    if _is_identifier_col(col_name):
        if isinstance(v, str):
            stripped = v.strip()
            if stripped in {"", ".0"}:
                return None
            if re.fullmatch(r"[+-]?\d+(?:\.0+)?", stripped):
                return int(float(stripped))
            return stripped
        if isinstance(v, bool):
            return v
        try:
            numeric = float(v)
        except (TypeError, ValueError):
            return v
        else:
            if numeric.is_integer():
                return int(numeric)
    if isinstance(sa_type, (sa_types.Date, sa_types.DateTime)):
        # After init, upgrade_column_types turns columns such as ngay_sinh into
        # DATE. Raw Excel serials would then be rejected by the driver, so they
        # must be converted here instead of being passed through untouched.
        return _coerce_date_like(v, sa_type, col_name)
    if isinstance(v, str) and v.strip() == ".0":
        return ""
    if isinstance(sa_type, sa_types.Numeric):
        return float(v)
    if isinstance(sa_type, sa_types.Integer):
        return int(v)
    return v


def build_table_schemas(engine, files: list[dict], cfg, logger) -> tuple[list[dict], int]:
    """Phase 1: read column headers only, create all tables with union schema.

    Returns (files_to_load, n_skipped) — files_to_load excludes unreadable files.
    """
    cols_by_table: dict[str, set[str]] = defaultdict(set)
    files_to_load = []
    n_skipped = 0

    for f in files:
        path = f["file_path"]
        rel = f["rel_path"]
        table = f["table_name"]
        header = cfg.table_header_map.get(table, 0)
        try:
            header_df = pd.read_excel(path, header=header, nrows=0, engine="openpyxl")
            norm_cols = [normalize_col_name(c) for c in header_df.columns]
            cols_by_table[table].update(norm_cols)
            files_to_load.append(f)
        except Exception as e:
            if logger:
                logger.warning(f"SKIP_FILE — {rel} — cannot read headers: {e}")
            insert_load_metadata(engine, rel, table, 0, "failed", "INSERT")
            n_skipped += 1

    for table_name, cols in cols_by_table.items():
        create_table_with_columns(engine, table_name, sorted(cols))
        if logger:
            logger.info(f"SCHEMA — {table_name} — {len(cols)} columns")

    return files_to_load, n_skipped


def _ensure_table_schema(engine: Engine, df: pd.DataFrame, table_name: str, logger) -> None:
    existing = get_table_columns(engine, table_name)
    if not existing:
        cols_sql = (
            _uuid_col_def(engine) + ", "
            + ", ".join(f'"{normalize_col_name(c)}" TEXT NULL' for c in df.columns)
            + ', "source_file" TEXT NULL'
        )
        with engine.connect() as conn:
            conn.execute(text(f'CREATE TABLE "{table_name}" ({cols_sql})'))
            conn.commit()
        return

    for col in df.columns:
        norm = normalize_col_name(col)
        if norm not in existing:
            add_column(engine, table_name, norm, "TEXT")
            if logger:
                logger.info(f"NEW_COLUMN — {table_name} — added column: '{norm}'")

    if "source_file" not in existing:
        add_column(engine, table_name, "source_file", "TEXT")


def load_file(engine: Engine, df: pd.DataFrame, table_name: str,
              rel_path: str, logger) -> dict:
    df = df.copy()
    df.columns = [normalize_col_name(c) for c in df.columns]
    df = df.loc[:, ~df.columns.duplicated()]  # keep first when two raw cols normalize to same name

    _ensure_table_schema(engine, df, table_name, logger)

    already_loaded = is_file_loaded(engine, rel_path)
    operation = "UPDATE" if already_loaded else "INSERT"

    df["source_file"] = rel_path

    col_info = inspect(engine).get_columns(table_name)
    col_sa_types = {c["name"]: c["type"] for c in col_info}
    existing_cols = list(col_sa_types)
    df = df[[c for c in df.columns if c in existing_cols]]

    col_keys = list(df.columns)
    cols_sql = ", ".join(f'"{_safe_col(c)}"' for c in col_keys)
    params_sql = ", ".join(f":p{i}" for i in range(len(col_keys)))
    stmt = text(f'INSERT INTO "{table_name}" ({cols_sql}) VALUES ({params_sql})')

    loaded = 0
    skipped = 0
    chunk_size = 500
    total_rows = len(df)

    # The replace of an already-loaded file and the insert of its new rows share
    # one transaction: if every row is rejected, the DELETE is rolled back too and
    # the previously loaded data survives instead of being wiped.
    conn = engine.connect()
    trans = conn.begin()
    try:
        if already_loaded:
            conn.execute(
                text(f'DELETE FROM "{table_name}" WHERE source_file = :fp'),
                {"fp": rel_path},
            )

        for chunk_start in range(0, total_rows, chunk_size):
            chunk = df.iloc[chunk_start : chunk_start + chunk_size]

            # Build param dicts, catching per-row coercion errors immediately
            good_records: list[tuple[int, dict]] = []
            for idx, row in chunk.iterrows():
                try:
                    record = {
                        f"p{i}": _coerce_value(v, col_sa_types.get(k), k)
                        for i, (k, v) in enumerate(row.items())
                    }
                    good_records.append((idx, record))
                except Exception as e:
                    skipped += 1
                    if logger:
                        logger.warning(f"SKIP_ROW — {rel_path} — row {idx} — {e}")

            if not good_records:
                continue

            # Attempt bulk insert for this chunk
            try:
                param_list = [r for _, r in good_records]
                with conn.begin_nested():
                    conn.execute(stmt, param_list)
                loaded += len(good_records)
            except Exception as bulk_exc:
                if logger:
                    logger.warning(
                        f"BULK_FAIL — {rel_path} — chunk starting row {chunk_start} — {bulk_exc}"
                    )
                # Fallback: row-by-row. Each row gets its own SAVEPOINT so one bad
                # row leaves the surrounding transaction usable for the rest.
                for idx, record in good_records:
                    try:
                        with conn.begin_nested():
                            conn.execute(stmt, record)
                        loaded += 1
                    except Exception as e:
                        skipped += 1
                        if logger:
                            logger.warning(f"SKIP_ROW — {rel_path} — row {idx} — {e}")

        if loaded == 0 and total_rows > 0:
            trans.rollback()
            status = "failed"
        else:
            trans.commit()
            status = "partial" if skipped else "success"
    except Exception:
        trans.rollback()
        raise
    finally:
        conn.close()

    insert_load_metadata(engine, rel_path, table_name, loaded, status, operation)
    return {"loaded": loaded, "skipped": skipped, "status": status}
