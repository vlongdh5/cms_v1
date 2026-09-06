import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent / "src"))

import argparse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import pandas as pd

from loader.config import Config
from loader.db import (get_engine, ensure_database, create_metadata_tables,
                        reset_metadata_tables,
                        get_last_run_time, insert_run_log, finish_run_log,
                        insert_load_metadata, get_table_columns, drop_table,
                        upgrade_column_types, get_active_files,
                        archive_and_delete_file, is_file_loaded)
from loader.logger import setup_logger
from loader.file_scanner import scan_all_files
from loader.excel_reader import read_excel
from loader.loader import load_file, normalize_col_name, build_table_schemas
from loader.view_manager import save_views, drop_all_views, restore_views


def apply_init_sql(engine, sql_file: Path, logger) -> tuple[int, int]:
    """Execute a SQL file statement-by-statement.

    Returns (applied, failed) counts.
    """
    if not sql_file.exists():
        return 0, 0

    raw_sql = sql_file.read_text(encoding="utf-8")
    statements = [s.strip() for s in raw_sql.split(";") if s.strip()]
    if not statements:
        return 0, 0

    applied = 0
    failed = 0
    with engine.connect() as conn:
        for stmt in statements:
            try:
                conn.exec_driver_sql(stmt)
                applied += 1
            except Exception as e:
                failed += 1
                if logger:
                    logger.warning(f"INIT_SQL_FAILED — {sql_file.name} — {e}")
        conn.commit()
    return applied, failed


def run(mode: str):
    cfg = Config()
    start_time = datetime.now(tz=timezone.utc)
    run_id_str = start_time.strftime("%Y%m%d_%H%M%S")
    logger = setup_logger(log_dir="logs", run_id=run_id_str)

    try:
        ensure_database(cfg.db_url)
        engine = get_engine(cfg.db_url, pool_size=5)
        if mode == "init":
            reset_metadata_tables(engine)
        else:
            create_metadata_tables(engine)
    except Exception as e:
        logger.critical(f"DB bootstrap failed: {e}")
        raise

    run_id = insert_run_log(engine, mode, start_time)
    logger.info(f"Run started — mode={mode} run_id={run_id}")

    processed = skipped = errors = deleted = 0
    loaded_tables: set[str] = set()
    new_cols_by_table: dict[str, set[str]] = defaultdict(set)

    try:
        if mode == "init":
            all_files = scan_all_files(cfg.folder_map)
            logger.info(f"Scanning: {len(all_files)} files found")

            table_names = {f["table_name"] for f in all_files}

            view_dir = Path("view")
            saved_views = save_views(engine, view_dir)
            if saved_views:
                logger.info(f"VIEW_SAVED — {len(saved_views)} views saved to view/")
                n_dropped = drop_all_views(engine)
                logger.info(f"VIEW_DROPPED — {n_dropped} views dropped")

            logger.info("INIT — dropping existing tables")
            for table_name in table_names:
                drop_table(engine, table_name)

            # Phase 1: build schemas from headers
            logger.info("INIT — Phase 1: building table schemas from file headers")
            files_to_load, n_skipped = build_table_schemas(engine, all_files, cfg, logger)
            skipped += n_skipped

            # Phase 2: parallel bulk load
            total = len(files_to_load)
            logger.info(f"INIT — Phase 2: loading {total} files (3 workers)")

            completed_count = 0

            def load_one(f: dict) -> dict:
                path = f["file_path"]
                rel = f["rel_path"]
                table = f["table_name"]
                header = cfg.table_header_map.get(table, 0)
                df, read_err = read_excel(path, header=header)
                if read_err:
                    logger.warning(f"SKIP_FILE — {rel} — cannot read: {read_err}")
                    insert_load_metadata(engine, rel, table, 0, "failed", "INSERT")
                    return {"rel": rel, "table": table, "outcome": "skip"}
                try:
                    stats = load_file(engine, df, table, rel, logger)
                    if stats["status"] == "failed":
                        logger.error(
                            f"LOAD_FAILED — {rel} — 0 of {stats['skipped']} rows accepted, "
                            f"nothing written"
                        )
                        return {"rel": rel, "table": table, "outcome": "error"}
                    logger.info(f"LOADED — {rel} — {stats['loaded']} rows ({stats['skipped']} skipped)")
                    if stats["skipped"]:
                        logger.warning(
                            f"PARTIAL_LOAD — {rel} — {stats['skipped']} rows rejected"
                        )
                        return {"rel": rel, "table": table, "outcome": "partial"}
                    return {"rel": rel, "table": table, "outcome": "ok"}
                except Exception as e:
                    logger.error(f"ERROR — {rel} — {e}")
                    insert_load_metadata(engine, rel, table, 0, "failed", "INSERT")
                    return {"rel": rel, "table": table, "outcome": "error"}

            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = {executor.submit(load_one, f): f for f in files_to_load}
                for future in as_completed(futures):
                    completed_count += 1
                    pct = completed_count * 100 // total if total else 100
                    try:
                        result = future.result()
                    except Exception as e:
                        src = futures.get(future, {})
                        rel = src.get("rel_path", "unknown")
                        logger.error(f"WORKER_ERROR — {rel} — {e}")
                        errors += 1
                        logger.info(f"PROGRESS — {completed_count}/{total} ({pct}%)")
                        continue

                    logger.info(f"PROGRESS — {completed_count}/{total} ({pct}%)")
                    if result["outcome"] == "ok":
                        processed += 1
                        loaded_tables.add(result["table"])
                    elif result["outcome"] == "partial":
                        # Data did land, but rows were dropped — must not read as a clean run.
                        processed += 1
                        loaded_tables.add(result["table"])
                        errors += 1
                    elif result["outcome"] == "skip":
                        skipped += 1
                    else:
                        errors += 1

            # Phase 3: upgrade column types
            logger.info("INIT — Phase 3: upgrading column types")
            for table_name in loaded_tables:
                upgrade_column_types(engine, table_name, logger)

            # Phase 3.5: enforce index policy for init
            index_sql = Path("script") / "init_indexes.sql"
            applied_sql, failed_sql = apply_init_sql(engine, index_sql, logger)
            if applied_sql or failed_sql:
                logger.info(
                    f"INIT — Phase 3.5: index SQL applied={applied_sql}, failed={failed_sql}"
                )
                if failed_sql:
                    errors += failed_sql

            # Phase 4: restore views
            if saved_views or any(view_dir.glob("*.sql")):
                logger.info("INIT — Phase 4: restoring views")
                restored, view_failed = restore_views(engine, view_dir, logger)
                logger.info(f"INIT — {restored} views restored, {view_failed} failed")
                if view_failed:
                    errors += view_failed

        else:  # daily
            last_run = get_last_run_time(engine)
            current_files = scan_all_files(cfg.folder_map)

            if last_run is not None:
                if last_run.tzinfo is None:
                    last_run = last_run.replace(tzinfo=timezone.utc)
                if last_run > start_time:
                    # Written by a build that stored local wall-clock as UTC. Trusting
                    # it would hide every file changed since, so fall back to a full scan.
                    logger.warning(
                        f"CLOCK_SKEW — last run recorded at {last_run.isoformat()} is after "
                        f"this run started ({start_time.isoformat()}); scanning all files"
                    )
                    last_run = None

            if last_run is None:
                logger.info("No previous run found, scanning all files")
                files = current_files
            else:
                # mtime alone is not enough: a file copied in with its original
                # timestamp, or one that failed to load earlier, would stay
                # invisible forever. Anything not currently active in the
                # metadata is picked up regardless of how old it looks.
                active_rel_paths = {a["file_path"] for a in get_active_files(engine)}
                files = [
                    f for f in current_files
                    if f["modified_at"] > last_run or f["rel_path"] not in active_rel_paths
                ]
                n_never_loaded = sum(
                    1 for f in files
                    if f["modified_at"] <= last_run and f["rel_path"] not in active_rel_paths
                )
                if n_never_loaded:
                    logger.info(
                        f"DAILY — {n_never_loaded} file(s) older than last run but never "
                        f"loaded successfully, including them"
                    )

            logger.info(f"Scanning: {len(files)} files to process")
            total = len(files)

            for idx, f in enumerate(files, 1):
                path = f["file_path"]
                rel = f["rel_path"]
                table = f["table_name"]
                header = cfg.table_header_map.get(table, 0)
                df, read_err = read_excel(path, header=header)
                if read_err:
                    logger.warning(f"SKIP_FILE — {rel} — cannot read: {read_err}")
                    op = "UPDATE" if is_file_loaded(engine, rel) else "INSERT"
                    insert_load_metadata(engine, rel, table, 0, "failed", op)
                    skipped += 1
                    logger.info(f"PROGRESS — {idx}/{total} ({idx*100//total if total else 100}%)")
                    continue

                existing = get_table_columns(engine, table)
                norm_df_cols = {normalize_col_name(c) for c in df.columns}
                schema_cols = [c for c in existing if c not in ("source_file", "uuid")]
                missing = [c for c in schema_cols if c not in norm_df_cols]
                if missing:
                    logger.info(f"MISSING_COLS — {rel} — {missing} will be NULL")

                new_cols = [c for c in norm_df_cols if c not in existing]

                try:
                    stats = load_file(engine, df, table, rel, logger)
                    if stats["status"] == "failed":
                        errors += 1
                        logger.error(
                            f"LOAD_FAILED — {rel} — 0 of {stats['skipped']} rows accepted, "
                            f"nothing written (previously loaded rows left untouched)"
                        )
                    else:
                        processed += 1
                        loaded_tables.add(table)
                        if new_cols:
                            new_cols_by_table[table].update(new_cols)
                        logger.info(f"LOADED — {rel} — {stats['loaded']} rows ({stats['skipped']} skipped)")
                        if stats["skipped"]:
                            errors += 1
                            logger.warning(
                                f"PARTIAL_LOAD — {rel} — {stats['skipped']} rows rejected"
                            )
                except Exception as e:
                    errors += 1
                    logger.error(f"ERROR — {rel} — {e}")
                    op = "UPDATE" if is_file_loaded(engine, rel) else "INSERT"
                    insert_load_metadata(engine, rel, table, 0, "failed", op)

                logger.info(f"PROGRESS — {idx}/{total} ({idx*100//total if total else 100}%)")

            if new_cols_by_table:
                logger.info("DAILY — upgrading column types for new columns")
                for table_name, cols in new_cols_by_table.items():
                    upgrade_column_types(engine, table_name, logger, cols=list(cols))

            # Detect and archive deleted files
            logger.info("DAILY — checking for deleted files")

            # Files currently existing in source folders (scanned above)
            current_rel_paths = {
                f["rel_path"]
                for f in current_files
            }

            active_files = get_active_files(engine)

            logger.info(
                f"DAILY — active metadata files={len(active_files)}, "
                f"current source files={len(current_rel_paths)}"
            )
            for active in active_files:
                rel = active["file_path"]
                table = active["table_name"]

                if rel not in current_rel_paths:
                    try:
                        archive_and_delete_file(
                            engine,
                            rel,
                            table,
                            logger
                        )
                        deleted += 1

                    except Exception as e:
                        errors += 1
                        logger.error(
                            f"DELETE_FAILED — {rel} — {e}"
                        )

    finally:
        if mode == "init":
            try:
                # Ensure birth-date normalization is applied even if init exited early.
                upgrade_column_types(engine, "customer_data", logger, cols=["ngay_sinh"])
            except Exception as e:
                logger.error(f"FINAL_TYPE_UPGRADE_FAILED — customer_data.ngay_sinh — {e}")
        finish_run_log(engine, run_id, processed, skipped, errors)
        summary = f"Run finished — {processed} loaded, {skipped} skipped, {errors} errors, {deleted} deleted"
        if errors:
            logger.error(summary)
        else:
            logger.info(summary)

    return errors


def run_scripts(script_path: str | None = None):
    cfg = Config()
    engine = get_engine(cfg.db_url)

    script_dir = Path("script")
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)

    if script_path:
        scripts = [Path(script_path)]
    else:
        scripts = sorted(script_dir.glob("*.sql"))

    if not scripts:
        print("No SQL scripts found.")
        return

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    ok_count = err_count = 0

    for sql_file in scripts:
        sql = sql_file.read_text(encoding="utf-8").strip()
        if not sql:
            print(f"SKIP  {sql_file.name} — empty file")
            continue
        try:
            df = pd.read_sql(sql, engine)
            out_path = output_dir / f"{sql_file.stem}_{timestamp}.xlsx"
            df.to_excel(out_path, index=False, engine="openpyxl")
            print(f"OK    {sql_file.name} → {out_path} ({len(df)} rows)")
            ok_count += 1
        except Exception as e:
            print(f"ERR   {sql_file.name}: {e}")
            err_count += 1

    print(f"\nDone — {ok_count} exported, {err_count} errors")


def main():
    parser = argparse.ArgumentParser(description="CMS data pipeline loader")
    parser.add_argument("--mode", choices=["init", "daily", "run_script"], required=True,
                        help="init: load all files; daily: load changed files since last run; run_script: execute SQL scripts and export to Excel")
    parser.add_argument("--script", default=None,
                        help="(run_script mode) path to a specific .sql file; omit to run all scripts in script/")
    args = parser.parse_args()

    if args.mode == "run_script":
        run_scripts(args.script)
    else:
        # Non-zero exit so a failed load is visible to cron / CI instead of
        # being reported as a clean run.
        if run(args.mode):
            sys.exit(1)


if __name__ == "__main__":
    main()
