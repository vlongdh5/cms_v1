import pytest
from pathlib import Path
from sqlalchemy import create_engine, text


@pytest.fixture
def engine():
    return create_engine("sqlite:///:memory:")


def _create_view(engine, name: str, sql: str):
    with engine.connect() as conn:
        conn.execute(text(f'CREATE VIEW "{name}" AS {sql}'))
        conn.commit()


def test_save_views_creates_sql_files(engine, tmp_path):
    from loader.view_manager import save_views
    _create_view(engine, "v_test", "SELECT 1 AS val")
    saved = save_views(engine, tmp_path)
    assert "v_test" in saved
    assert (tmp_path / "v_test.sql").exists()


def test_save_views_file_contains_ddl(engine, tmp_path):
    from loader.view_manager import save_views
    _create_view(engine, "v_revenue", "SELECT 42 AS amount")
    save_views(engine, tmp_path)
    content = (tmp_path / "v_revenue.sql").read_text()
    assert "v_revenue" in content
    assert "42" in content


def test_save_views_clears_old_files(engine, tmp_path):
    from loader.view_manager import save_views
    old_file = tmp_path / "old_view.sql"
    old_file.write_text("-- old")
    _create_view(engine, "v_new", "SELECT 1 AS x")
    save_views(engine, tmp_path)
    assert not old_file.exists()
    assert (tmp_path / "v_new.sql").exists()


def test_save_views_no_views_returns_empty(engine, tmp_path):
    from loader.view_manager import save_views
    result = save_views(engine, tmp_path)
    assert result == []


def test_save_views_creates_dir_if_missing(engine, tmp_path):
    from loader.view_manager import save_views
    new_dir = tmp_path / "subdir" / "views"
    _create_view(engine, "v_x", "SELECT 1 AS n")
    save_views(engine, new_dir)
    assert new_dir.exists()
    assert (new_dir / "v_x.sql").exists()


def test_drop_all_views_removes_views(engine):
    from loader.view_manager import drop_all_views
    from sqlalchemy import inspect as sa_inspect
    with engine.connect() as conn:
        conn.execute(text('CREATE VIEW "v_drop1" AS SELECT 1 AS a'))
        conn.execute(text('CREATE VIEW "v_drop2" AS SELECT 2 AS b'))
        conn.commit()
    count = drop_all_views(engine)
    assert count == 2
    view_names = sa_inspect(engine).get_view_names()
    assert "v_drop1" not in view_names
    assert "v_drop2" not in view_names


def test_drop_all_views_no_views_returns_zero(engine):
    from loader.view_manager import drop_all_views
    assert drop_all_views(engine) == 0


def test_restore_views_recreates_views(engine, tmp_path):
    from loader.view_manager import save_views, drop_all_views, restore_views
    from sqlalchemy import inspect as sa_inspect
    with engine.connect() as conn:
        conn.execute(text('CREATE VIEW "v_restore" AS SELECT 99 AS n'))
        conn.commit()
    save_views(engine, tmp_path)
    drop_all_views(engine)
    restored, failed = restore_views(engine, tmp_path, logger=None)
    assert restored == 1
    assert failed == 0
    assert "v_restore" in sa_inspect(engine).get_view_names()


def test_restore_views_failed_keeps_file(engine, tmp_path):
    from loader.view_manager import restore_views
    with engine.connect() as conn:
        conn.execute(text('CREATE VIEW "existing_view" AS SELECT 1 AS x'))
        conn.commit()
    bad_sql_file = tmp_path / "existing_view.sql"
    bad_sql_file.write_text('CREATE VIEW "existing_view" AS SELECT 2 AS y;')
    restored, failed = restore_views(engine, tmp_path, logger=None)
    assert restored == 0
    assert failed == 1
    assert bad_sql_file.exists()


def test_restore_views_empty_dir_returns_zero(engine, tmp_path):
    from loader.view_manager import restore_views
    restored, failed = restore_views(engine, tmp_path, logger=None)
    assert restored == 0
    assert failed == 0


def test_restore_views_missing_dir_returns_zero(engine, tmp_path):
    from loader.view_manager import restore_views
    missing = tmp_path / "no_such_dir"
    restored, failed = restore_views(engine, missing, logger=None)
    assert restored == 0
    assert failed == 0


def test_restore_views_from_manual_sql_file(engine, tmp_path):
    from loader.view_manager import restore_views
    from sqlalchemy import inspect as sa_inspect
    sql_file = tmp_path / "v_manual.sql"
    sql_file.write_text('CREATE VIEW "v_manual" AS SELECT 7 AS n;')
    restored, failed = restore_views(engine, tmp_path, logger=None)
    assert restored == 1
    assert failed == 0
    assert "v_manual" in sa_inspect(engine).get_view_names()


def test_save_drop_restore_roundtrip(engine, tmp_path):
    from loader.view_manager import save_views, drop_all_views, restore_views
    from sqlalchemy import inspect as sa_inspect
    with engine.connect() as conn:
        conn.execute(text("CREATE TABLE t (id INTEGER, val TEXT)"))
        conn.execute(text('CREATE VIEW "v_roundtrip" AS SELECT id, val FROM t'))
        conn.commit()
    save_views(engine, tmp_path)
    drop_all_views(engine)
    assert "v_roundtrip" not in sa_inspect(engine).get_view_names()
    restore_views(engine, tmp_path, logger=None)
    assert "v_roundtrip" in sa_inspect(engine).get_view_names()


def test_view_target_re_matches_materialized_view():
    """The schema-creation lookup must recognise matview DDL, which starts with a DROP."""
    from loader.view_manager import _VIEW_TARGET_RE

    ddl = (
        'DROP MATERIALIZED VIEW IF EXISTS "Update_Bills"."updatebill" CASCADE;\n'
        'CREATE MATERIALIZED VIEW "Update_Bills"."updatebill" AS\n'
        'SELECT 1;'
    )
    match = _VIEW_TARGET_RE.search(ddl)
    assert match is not None
    assert match.group("schema") == "Update_Bills"
    assert match.group("name") == "updatebill"

    plain = 'CREATE OR REPLACE VIEW "Update_Customer_Info"."Newcustomerinfo" AS SELECT 1;'
    match = _VIEW_TARGET_RE.search(plain)
    assert match.group("schema") == "Update_Customer_Info"


def test_restore_views_retries_dependent_views(engine, tmp_path):
    """A view depending on another must restore even when filename order is wrong."""
    from loader.view_manager import restore_views
    from sqlalchemy import inspect as sa_inspect

    with engine.connect() as conn:
        conn.execute(text("CREATE TABLE base (id INTEGER)"))
        conn.commit()

    # "a_" sorts first but depends on "b_", which has not been created yet
    (tmp_path / "a_dependent.sql").write_text(
        'CREATE VIEW "a_dependent" AS SELECT id FROM "b_source";'
    )
    (tmp_path / "b_source.sql").write_text(
        'CREATE VIEW "b_source" AS SELECT id FROM base;'
    )

    restored, failed = restore_views(engine, tmp_path, logger=None)

    assert (restored, failed) == (2, 0)
    view_names = sa_inspect(engine).get_view_names()
    assert "a_dependent" in view_names and "b_source" in view_names
