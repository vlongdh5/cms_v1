import re
from pathlib import Path
from sqlalchemy import Engine, text


_VIEW_TARGET_RE = re.compile(
    r'CREATE\s+(?:OR\s+REPLACE\s+)?(?:MATERIALIZED\s+)?VIEW\s+'
    r'(?:IF\s+NOT\s+EXISTS\s+)?"(?P<schema>[^"]+)"\."(?P<name>[^"]+)"',
    re.IGNORECASE,
)


def _matview_indexes(conn, schema: str, name: str) -> list[str]:
    """Index DDL defined on a materialized view.

    A matview is dropped and recreated on every init, which takes its indexes with
    it — including the unique index REFRESH CONCURRENTLY needs. They have to be
    saved alongside the definition or they are silently lost.
    """
    rows = conn.execute(text("""
        SELECT indexdef FROM pg_indexes
        WHERE schemaname = :schema AND tablename = :name
        ORDER BY indexname
    """), {"schema": schema, "name": name}).fetchall()
    return [r[0].rstrip().rstrip(";") + ";" for r in rows]


def _get_user_views(engine: Engine) -> list[dict]:
    """Returns list of {name, schema, kind, ddl} for user-defined views and matviews."""
    with engine.connect() as conn:
        if engine.dialect.name == "postgresql":
            views = [
                {
                    "name": r[1],
                    "schema": r[0],
                    "kind": "view",
                    "ddl": (
                        f'CREATE OR REPLACE VIEW "{r[0]}"."{r[1]}" AS\n'
                        + r[2].rstrip().rstrip(";")
                        + ";"
                    ),
                }
                for r in conn.execute(text("""
                    SELECT schemaname, viewname, definition
                    FROM pg_views
                    WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
                    ORDER BY viewname
                """)).fetchall()
            ]

            matviews = []
            for r in conn.execute(text("""
                SELECT schemaname, matviewname, definition
                FROM pg_matviews
                WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
                ORDER BY matviewname
            """)).fetchall():
                schema, name, definition = r[0], r[1], r[2]
                # No CREATE OR REPLACE for materialized views — drop first so the
                # restore is repeatable.
                statements = [
                    f'DROP MATERIALIZED VIEW IF EXISTS "{schema}"."{name}" CASCADE;',
                    f'CREATE MATERIALIZED VIEW "{schema}"."{name}" AS\n'
                    + definition.rstrip().rstrip(";")
                    + ";",
                    *_matview_indexes(conn, schema, name),
                ]
                matviews.append({
                    "name": name,
                    "schema": schema,
                    "kind": "matview",
                    "ddl": "\n".join(statements),
                })

            return views + matviews
        else:
            rows = conn.execute(text(
                "SELECT name, sql FROM sqlite_master WHERE type='view' ORDER BY name"
            )).fetchall()
            return [
                {"name": r[0], "schema": None, "kind": "view", "ddl": r[1].rstrip(";") + ";"}
                for r in rows
            ]


def save_views(engine: Engine, view_dir: Path) -> list[str]:
    views = _get_user_views(engine)
    if not views:
        return []

    view_dir.mkdir(parents=True, exist_ok=True)
    for old in view_dir.glob("*.sql"):
        old.unlink()

    saved_names: list[str] = []
    used_filenames: set[str] = set()

    for v in views:
        filename = f"{v['name']}.sql"
        if filename in used_filenames:
            prefix = v["schema"] or "default"
            filename = f"{prefix}__{v['name']}.sql"
        used_filenames.add(filename)
        (view_dir / filename).write_text(v["ddl"], encoding="utf-8")
        saved_names.append(v["name"])

    return saved_names


def drop_all_views(engine: Engine) -> int:
    views = _get_user_views(engine)
    if not views:
        return 0
    # Matviews first: they can depend on plain views, and a matview left behind is
    # what blocks the DROP TABLE that init does next.
    views = sorted(views, key=lambda v: v["kind"] != "matview")
    with engine.connect() as conn:
        for v in views:
            if engine.dialect.name == "postgresql":
                kind = "MATERIALIZED VIEW" if v["kind"] == "matview" else "VIEW"
                conn.execute(text(
                    f'DROP {kind} IF EXISTS "{v["schema"]}"."{v["name"]}" CASCADE'
                ))
            else:
                conn.execute(text(f'DROP VIEW IF EXISTS "{v["name"]}"'))
        conn.commit()
    return len(views)


def restore_views(engine: Engine, view_dir: Path, logger=None) -> tuple[int, int]:
    if not view_dir.exists():
        return 0, 0
    sql_files = sorted(view_dir.glob("*.sql"))
    if not sql_files:
        return 0, 0

    restored = 0
    pending = list(sql_files)
    last_errors: dict[Path, Exception] = {}

    # A view may depend on another view (or a matview on a view), and filename order
    # says nothing about that. Keep sweeping while at least one more succeeds; stop
    # as soon as a whole pass makes no progress.
    while pending:
        still_pending: list[Path] = []
        for sql_file in pending:
            sql = sql_file.read_text(encoding="utf-8").strip()
            try:
                with engine.connect() as conn:
                    if engine.dialect.name == "postgresql":
                        match = _VIEW_TARGET_RE.search(sql)
                        if match:
                            conn.execute(text(
                                f'CREATE SCHEMA IF NOT EXISTS "{match.group("schema")}"'
                            ))
                    conn.execute(text(sql))
                    conn.commit()
                if logger:
                    logger.info(f"VIEW_RESTORED — {sql_file.stem}")
                restored += 1
            except Exception as e:
                last_errors[sql_file] = e
                still_pending.append(sql_file)

        if len(still_pending) == len(pending):
            for sql_file in still_pending:
                if logger:
                    logger.warning(
                        f"VIEW_RESTORE_FAILED — {sql_file.stem} — {last_errors[sql_file]}"
                    )
            return restored, len(still_pending)

        pending = still_pending

    return restored, 0
