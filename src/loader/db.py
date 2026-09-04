from datetime import datetime, timezone
from sqlalchemy import (create_engine, text, inspect as sa_inspect,
                         MetaData, Table, Column, Integer, String, DateTime, Engine,
                         insert as sa_insert)

STATUS_SUCCESS = "success"

_ALLOWED_COL_TYPES = {"TEXT", "INTEGER", "FLOAT", "TIMESTAMP"}
_UPGRADE_SKIP_COLS = {"source_file", "uuid"}


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


def get_engine(db_url: str, pool_size: int = 5) -> Engine:
    return create_engine(db_url, pool_size=pool_size, max_overflow=2)


def ensure_database(db_url: str):
    from sqlalchemy.engine import make_url
    from sqlalchemy import event
    u = make_url(db_url)
    db_name = u.database
    admin_url = u.set(database="postgres")
    engine = create_engine(admin_url, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        exists = conn.execute(
            text("SELECT 1 FROM pg_database WHERE datname = :name"), {"name": db_name}
        ).fetchone()
        if not exists:
            conn.execute(text(f'CREATE DATABASE "{db_name}"'))
    engine.dispose()


def create_metadata_tables(engine: Engine):
    meta = MetaData()
    Table("_load_metadata", meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("file_path", String(500), nullable=False),
        Column("table_name", String(100)),
        Column("last_loaded_at", DateTime),
        Column("row_count", Integer),
        Column("status", String(20)),
    )
    Table("_run_log", meta,
        Column("run_id", Integer, primary_key=True, autoincrement=True),
        Column("mode", String(10)),
        Column("started_at", DateTime),
        Column("finished_at", DateTime),
        Column("files_processed", Integer, default=0),
        Column("files_skipped", Integer, default=0),
        Column("errors", Integer, default=0),
    )
    meta.create_all(engine, checkfirst=True)
    if "operation" not in get_table_columns(engine, "_load_metadata"):
        add_column(engine, "_load_metadata", "operation", "TEXT")


def get_table_columns(engine: Engine, table_name: str) -> list[str]:
    insp = sa_inspect(engine)
    if not insp.has_table(table_name):
        return []
    return [col["name"] for col in insp.get_columns(table_name)]


def add_column(engine: Engine, table_name: str, col_name: str, col_type: str = "TEXT"):
    if col_type.upper() not in _ALLOWED_COL_TYPES:
        raise ValueError(f"Unsupported col_type: {col_type!r}")
    if engine.dialect.name == "postgresql":
        ddl = f'ALTER TABLE "{table_name}" ADD COLUMN IF NOT EXISTS "{col_name}" {col_type}'
    else:
        ddl = f'ALTER TABLE "{table_name}" ADD "{col_name}" {col_type}'
    with engine.connect() as conn:
        conn.execute(text(ddl))
        conn.commit()


def drop_table(engine: Engine, table_name: str):
    with engine.connect() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}"'))
        conn.commit()


def reset_metadata_tables(engine: Engine):
    drop_table(engine, "_load_metadata")
    drop_table(engine, "_run_log")
    create_metadata_tables(engine)


def _uuid_col_def(engine: Engine) -> str:
    if engine.dialect.name == "postgresql":
        return '"uuid" UUID PRIMARY KEY DEFAULT gen_random_uuid()'
    return '"uuid" TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16))))'


def create_table_with_columns(engine: Engine, table_name: str, columns: list[str]):
    if not columns:
        raise ValueError("columns must not be empty")
    col_defs = ", ".join(f'"{c}" TEXT NULL' for c in columns)
    ddl = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({_uuid_col_def(engine)}, {col_defs}, "source_file" TEXT NULL)'
    with engine.connect() as conn:
        conn.execute(text(ddl))
        conn.commit()


# _UPGRADE_CANDIDATE_TYPES = ("NUMERIC", "TIMESTAMP")

def _upgrade_birth_date_column(engine: Engine, table_name: str, col: str, logger=None) -> bool:
    """Try to convert Excel-serial/text birthday values to DATE in PostgreSQL."""
    if engine.dialect.name != "postgresql":
        return False

    try:
        inspector = sa_inspect(engine)
        column_info = next(
            (
                c
                for c in inspector.get_columns(table_name)
                if c["name"] == col
            ),
            None,
        )

        if not column_info:
            return False

        current_type = str(column_info["type"]).upper()

        # Đã là DATE thì không cần upgrade lại
        if current_type.startswith("DATE"):
            if logger:
                logger.debug(
                    f"TYPE_KEEP_DATE — {table_name}.{col}"
                )
            return True
    except Exception as e:
        if logger:
            logger.debug(
                f"TYPE_CHECK_SKIP — {table_name}.{col}: {e}"
            )
        return False

    try:
        with engine.connect() as conn:
            sql = f'''
                ALTER TABLE "{table_name}"
                ALTER COLUMN "{col}" TYPE DATE
                USING (
                    CASE
                        WHEN "{col}" IS NULL OR BTRIM("{col}"::text) = '' THEN NULL
                        ELSE
                            CASE
                                WHEN (
                                    CASE
                                        WHEN BTRIM("{col}"::text) ~ '^[0-9]{{8}}$'
                                            THEN to_date(BTRIM("{col}"::text), 'YYYYMMDD')
                                        WHEN BTRIM("{col}"::text) ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$'
                                            THEN ("{col}")::timestamp::date
                                        WHEN BTRIM("{col}"::text) ~ '^[0-9]{{2}}/[0-9]{{2}}/[0-9]{{4}}$'
                                            THEN to_date(BTRIM("{col}"::text), 'DD/MM/YYYY')
                                        WHEN BTRIM("{col}"::text) ~ '^[0-9]+(\\.[0]+)?$'
                                            THEN DATE '1899-12-30' + FLOOR(("{col}")::numeric)::int
                                        ELSE NULL
                                    END
                                ) BETWEEN DATE '1900-01-01' AND CURRENT_DATE
                                THEN (
                                    CASE
                                        WHEN BTRIM("{col}"::text) ~ '^[0-9]{{8}}$'
                                            THEN to_date(BTRIM("{col}"::text), 'YYYYMMDD')
                                        WHEN BTRIM("{col}"::text) ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$'
                                            THEN ("{col}")::timestamp::date
                                        WHEN BTRIM("{col}"::text) ~ '^[0-9]{{2}}/[0-9]{{2}}/[0-9]{{4}}$'
                                            THEN to_date(BTRIM("{col}"::text), 'DD/MM/YYYY')
                                        WHEN BTRIM("{col}"::text) ~ '^[0-9]+(\\.[0]+)?$'
                                            THEN DATE '1899-12-30' + FLOOR(("{col}")::numeric)::int
                                        ELSE NULL
                                    END
                                )
                                ELSE NULL
                            END
                    END
                )
            '''
            conn.execute(text(sql))
            conn.commit()
        if logger:
            logger.info(f"TYPE_UPGRADE — {table_name}.{col} → DATE")
        return True
    except Exception as e:
        if logger:
            logger.debug(f"SKIP_UPGRADE — {table_name}.{col} → DATE: {e}")
        return False


def _column_can_convert_to_numeric(
    engine: Engine,
    table_name: str,
    col: str,
) -> bool:
    """
    Check whether all non-empty values in a TEXT column
    can be safely converted to NUMERIC.

    A column with no values at all is left as TEXT: there is no evidence it is
    numeric, and upgrading it would reject the first real text value that shows up.
    """
    if engine.dialect.name != "postgresql":
        return False

    sql = f'''
        SELECT EXISTS (
            SELECT 1
            FROM "{table_name}"
            WHERE "{col}" IS NOT NULL
              AND BTRIM("{col}"::text) <> ''
        ) AND NOT EXISTS (
            SELECT 1
            FROM "{table_name}"
            WHERE "{col}" IS NOT NULL
              AND BTRIM("{col}"::text) <> ''
              AND BTRIM("{col}"::text) !~
                  '^[+-]?([0-9]+(\\.[0-9]+)?|\\.[0-9]+)$'
        )
    '''

    try:
        with engine.connect() as conn:
            return bool(conn.execute(text(sql)).scalar())
    except Exception:
        return False

def _column_can_convert_to_timestamp(
    engine: Engine,
    table_name: str,
    col: str,
) -> bool:
    """
    Check whether all non-empty values in a TEXT column
    look like standard timestamp/date values.

    An empty column stays TEXT — see _column_can_convert_to_numeric.
    """
    if engine.dialect.name != "postgresql":
        return False

    sql = f'''
        SELECT EXISTS (
            SELECT 1
            FROM "{table_name}"
            WHERE "{col}" IS NOT NULL
              AND BTRIM("{col}"::text) <> ''
        ) AND NOT EXISTS (
            SELECT 1
            FROM "{table_name}"
            WHERE "{col}" IS NOT NULL
              AND BTRIM("{col}"::text) <> ''
              AND BTRIM("{col}"::text) !~
                  '^\\d{{4}}-\\d{{2}}-\\d{{2}}([ T]\\d{{2}}:\\d{{2}}:\\d{{2}}(\\.\\d+)?)?$'
        )
    '''

    try:
        with engine.connect() as conn:
            return bool(conn.execute(text(sql)).scalar())
    except Exception:
        return False

def _upgrade_column_to_type(
    engine: Engine,
    table_name: str,
    col: str,
    sql_type: str,
    logger=None,
) -> bool:
    """
    Perform the actual PostgreSQL type conversion.
    """
    if engine.dialect.name != "postgresql":
        return False

    try:
        with engine.connect() as conn:
            conn.execute(text(
                f'ALTER TABLE "{table_name}" '
                f'ALTER COLUMN "{col}" TYPE {sql_type} '
                f'USING "{col}"::{sql_type}'
            ))
            conn.commit()

        if logger:
            logger.info(
                f"TYPE_UPGRADE — {table_name}.{col} → {sql_type}"
            )

        return True

    except Exception as e:
        if logger:
            logger.debug(
                f"SKIP_UPGRADE — {table_name}.{col} → "
                f"{sql_type}: {e}"
            )

        return False


def upgrade_column_types(
    engine: Engine,
    table_name: str,
    logger=None,
    cols: list[str] | None = None,
):
    """
    Upgrade TEXT columns only when their actual data
    clearly matches NUMERIC or TIMESTAMP.
    """

    all_cols = get_table_columns(engine, table_name)

    if not all_cols:
        if logger:
            logger.warning(
                f"TYPE_UPGRADE — {table_name} not found, skipping"
            )
        return

    target_cols = cols if cols is not None else all_cols

    for col in target_cols:

        # Skip system / identifier columns
        if col in _UPGRADE_SKIP_COLS or _is_identifier_col(col):
            continue

        # Special handling for customer_data.ngay_sinh
        if table_name == "customer_data" and col == "ngay_sinh":
            _upgrade_birth_date_column(
                engine,
                table_name,
                col,
                logger,
            )
            continue

        # Get current database type
        try:
            inspector = sa_inspect(engine)

            column_info = next(
                (
                    c
                    for c in inspector.get_columns(table_name)
                    if c["name"] == col
                ),
                None,
            )

            if not column_info:
                continue

            current_type = str(column_info["type"]).upper()

            # Only automatically upgrade TEXT columns
            if not current_type.startswith("TEXT"):
                continue

        except Exception as e:
            if logger:
                logger.debug(
                    f"TYPE_CHECK_SKIP — "
                    f"{table_name}.{col}: {e}"
                )
            continue

        # --------------------------------------------------
        # 1. NUMERIC
        # --------------------------------------------------
        if _column_can_convert_to_numeric(
            engine,
            table_name,
            col,
        ):
            if _upgrade_column_to_type(
                engine,
                table_name,
                col,
                "NUMERIC",
                logger,
            ):
                continue

        # --------------------------------------------------
        # 2. TIMESTAMP
        # --------------------------------------------------
        if _column_can_convert_to_timestamp(
            engine,
            table_name,
            col,
        ):
            if _upgrade_column_to_type(
                engine,
                table_name,
                col,
                "TIMESTAMP",
                logger,
            ):
                continue

        # --------------------------------------------------
        # 3. Keep TEXT
        # --------------------------------------------------
        if logger:
            logger.debug(
                f"TYPE_KEEP_TEXT — {table_name}.{col}"
            )

# def upgrade_column_types(engine: Engine, table_name: str, logger=None,
#                           cols: list[str] | None = None):
#     all_cols = get_table_columns(engine, table_name)
#     if not all_cols:
#         if logger:
#             logger.warning(f"TYPE_UPGRADE — {table_name} not found, skipping")
#         return
#     target_cols = cols if cols is not None else all_cols
#     for col in target_cols:
#         if col in _UPGRADE_SKIP_COLS or _is_identifier_col(col):
#             continue
#         if table_name == "customer_data" and col == "ngay_sinh":
#             _upgrade_birth_date_column(engine, table_name, col, logger)
#             continue
#         for sql_type in _UPGRADE_CANDIDATE_TYPES:
#             try:
#                 with engine.connect() as conn:
#                     conn.execute(text(
#                         f'ALTER TABLE "{table_name}" '
#                         f'ALTER COLUMN "{col}" TYPE {sql_type} '
#                         f'USING "{col}"::{sql_type}'
#                     ))
#                     conn.commit()
#                 if logger:
#                     logger.info(f"TYPE_UPGRADE — {table_name}.{col} → {sql_type}")
#                 break
#             except Exception as e:
#                 if logger:
#                     logger.debug(f"SKIP_UPGRADE — {table_name}.{col} → {sql_type}: {e}")


def get_last_run_time(engine: Engine) -> datetime | None:
    with engine.connect() as conn:
        row = conn.execute(text(
            "SELECT MAX(started_at) FROM _run_log WHERE finished_at IS NOT NULL"
        )).fetchone()
        return row[0] if row and row[0] is not None else None


def insert_load_metadata(engine: Engine, file_path: str, table_name: str,
                          row_count: int, status: str, operation: str):
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO _load_metadata (file_path, table_name, last_loaded_at, row_count, status, operation)
            VALUES (:fp, :tn, :now, :rc, :st, :op)
        """), {
            "fp": file_path,
            "tn": table_name,
            "now": datetime.now(timezone.utc),
            "rc": row_count,
            "st": status,
            "op": operation,
        })
        conn.commit()


def is_file_loaded(engine: Engine, file_path: str) -> bool:
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT operation, status FROM _load_metadata
            WHERE file_path = :fp
            ORDER BY last_loaded_at DESC
            LIMIT 1
        """), {"fp": file_path}).fetchone()
    if row is None:
        return False
    operation, status = row[0], row[1]
    return operation in ('INSERT', 'UPDATE') or (operation is None and status == STATUS_SUCCESS)


def get_active_files(engine: Engine) -> list[dict]:
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT file_path, table_name FROM (
                SELECT file_path, table_name, operation, status,
                       ROW_NUMBER() OVER (
                           PARTITION BY file_path ORDER BY last_loaded_at DESC
                       ) AS rn
                FROM _load_metadata
            ) t
            WHERE rn = 1
              AND (
                (operation IN ('INSERT', 'UPDATE') AND status != 'failed')
                OR (operation IS NULL AND status = :s)
              )
        """), {"s": STATUS_SUCCESS}).fetchall()
    return [{"file_path": r[0], "table_name": r[1]} for r in rows]


def ensure_deleted_table(engine: Engine, table_name: str):
    deleted_table = f"{table_name}_deleted"
    insp = sa_inspect(engine)
    if not insp.has_table(deleted_table):
        with engine.connect() as conn:
            conn.execute(text(
                f'CREATE TABLE IF NOT EXISTS "{deleted_table}" AS '
                f'SELECT * FROM "{table_name}" WHERE 1=0'
            ))
            conn.commit()
        add_column(engine, deleted_table, "deleted_at", "TIMESTAMP")
    else:
        main_cols = set(get_table_columns(engine, table_name))
        deleted_cols = set(get_table_columns(engine, deleted_table))
        for col in main_cols:
            if col not in deleted_cols:
                add_column(engine, deleted_table, col, "TEXT")


def archive_and_delete_file(engine: Engine, file_path: str, table_name: str,
                             logger=None) -> int:
    deleted_table = f"{table_name}_deleted"
    try:
        ensure_deleted_table(engine, table_name)

        main_cols = get_table_columns(engine, table_name)
        cols_sql = ", ".join(f'"{c}"' for c in main_cols)

        with engine.connect() as conn:
            row_count = conn.execute(text(
                f'SELECT COUNT(*) FROM "{table_name}" WHERE source_file = :fp'
            ), {"fp": file_path}).scalar() or 0

            conn.execute(text(
                f'INSERT INTO "{deleted_table}" ({cols_sql}, "deleted_at") '
                f'SELECT {cols_sql}, CURRENT_TIMESTAMP '
                f'FROM "{table_name}" WHERE source_file = :fp'
            ), {"fp": file_path})

            conn.execute(text(
                f'DELETE FROM "{table_name}" WHERE source_file = :fp'
            ), {"fp": file_path})
            conn.commit()

        insert_load_metadata(engine, file_path, table_name, row_count, "success", "DELETED")
        if logger:
            logger.info(f"DELETED — {file_path} — {row_count} rows archived to {deleted_table}")
        return row_count

    except Exception as e:
        try:
            insert_load_metadata(engine, file_path, table_name, 0, "failed", "DELETED")
        except Exception:
            pass
        if logger:
            logger.error(f"DELETE_FAILED — {file_path} — {e}")
        raise


def insert_run_log(engine: Engine, mode: str, started_at: datetime) -> int:
    with engine.connect() as conn:
        meta = MetaData()
        meta.reflect(bind=engine, only=["_run_log"])
        run_log_tbl = meta.tables["_run_log"]
        result = conn.execute(
            sa_insert(run_log_tbl).values(mode=mode, started_at=started_at)
        )
        conn.commit()
        return result.inserted_primary_key[0]


def finish_run_log(engine: Engine, run_id: int, processed: int, skipped: int, errors: int):
    with engine.connect() as conn:
        conn.execute(text("""
            UPDATE _run_log
            SET finished_at = :now, files_processed = :p, files_skipped = :s, errors = :e
            WHERE run_id = :rid
        """), {"now": datetime.now(timezone.utc), "p": processed, "s": skipped, "e": errors, "rid": run_id})
        conn.commit()
