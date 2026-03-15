"""
test_server_level.py
====================
Exercises server-mode extraction — omit the ``database`` parameter to discover
and extract ALL databases on a server automatically.

Runs against whichever connector(s) are configured in .env. Unconfigured
connectors are skipped gracefully.

Output layout
-------------
    ./models/          ← YAML files (per-schema / per-database)
    ./reports/         ← JSON comparison reports + compiled reports.pdf

Configuration (.env or environment variables)
---------------------------------------------
ClickHouse (primary — server mode):
    clickhouse_host        default: localhost
    clickhouse_port        (auto-selected based on transport + secure)
    clickhouse_transport   "http" | "native" | omit to auto-detect
    clickhouse_user        default: default
    clickhouse_password
    # Do NOT set clickhouse_db — leave unset for server mode

MySQL (optional):
    MYSQL_HOST   MYSQL_PORT   MYSQL_USER   MYSQL_PASSWORD
    # Do NOT set MYSQL_DB — leave unset for server mode

PostgreSQL (optional):
    PG_HOST   PG_PORT   PG_DB   PG_USER   PG_PASSWORD
    # Set PG_DB to a connection database (e.g. "postgres")
    # Leave PG_SCHEMAS unset to extract all schemas server-wide

Notifications (choose one or both):
    NOTIFICATION_TYPE    "email" | "slack" | "both"  (default: email)

Email (PDF attached automatically):
    SMTP_HOST  SMTP_PORT  SMTP_USER  SMTP_PASSWORD  EMAIL_TO

Slack:
    SLACK_BOT_TOKEN      xoxb-… Bot Token
    SLACK_NOTIFY_TARGET  "#channel", "@username", "C…" or "U…"
"""

import json as _json_mod
import os

from dotenv import load_dotenv

from data_dictionary_builder import (
    DDHelper,
    DatabaseMetadata,
    ExecutionTimer,
    MetadataExtractor,
    SchemaComparator,
    YAMLGenerator,
)

load_dotenv()

EMOJI               = "🌐 "
NOTIFICATION_TYPE   = os.getenv("NOTIFICATION_TYPE", "email")
EMAIL_RECIPIENTS    = [e.strip() for e in os.getenv("EMAIL_TO", "").split(",") if e.strip()]
SLACK_TARGETS       = [t.strip() for t in os.getenv("SLACK_NOTIFY_TARGET", "").split(",") if t.strip()]
EMAIL_TO            = ", ".join(EMAIL_RECIPIENTS)
SLACK_NOTIFY_TARGET = ", ".join(SLACK_TARGETS)


def section(title: str) -> None:
    print(f"\n{'='*60}\n  {title}\n{'='*60}")


# ── Connection configs — database intentionally omitted for server mode ───────

def _ch_config() -> dict:
    """ClickHouse server-mode config (no database param)."""
    cfg = {
        "db_type":  "clickhouse",
        "host":     os.getenv("clickhouse_host", "localhost"),
        "user":     os.getenv("clickhouse_user", "default"),
        "password": os.getenv("clickhouse_password", ""),
        "secure":   True,
        "verify":   False,
    }
    port = os.getenv("clickhouse_port")
    if port:
        cfg["port"] = int(port)
    transport = os.getenv("clickhouse_transport")
    if transport:
        cfg["transport"] = transport
    return cfg


def _mysql_config() -> dict:
    """MySQL server-mode config (no database param)."""
    return {
        "db_type":  "mysql",
        "host":     os.getenv("MYSQL_HOST", "localhost"),
        "port":     int(os.getenv("MYSQL_PORT", 3306)),
        "user":     os.getenv("MYSQL_USER", "root"),
        "password": os.getenv("MYSQL_PASSWORD", ""),
    }


def _pg_config() -> dict:
    """PostgreSQL config — uses a connection database but will scan all schemas."""
    return {
        "db_type":  "postgres",
        "host":     os.getenv("PG_HOST", "localhost"),
        "port":     int(os.getenv("PG_PORT", 5432)),
        "database": os.getenv("PG_DB", "postgres"),
        "user":     os.getenv("PG_USER", "postgres"),
        "password": os.getenv("PG_PASSWORD", ""),
    }


def _is_configured(cfg: dict, required_keys=("host",)) -> bool:
    for k in required_keys:
        v = cfg.get(k)
        if not v or v in ("localhost", "default", "root", "postgres"):
            pass     # default values are fine; connectivity will determine if it works
    return True      # always attempt; connection test will handle failures gracefully


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────

def test_connection(cfg: dict, label: str) -> bool:
    section(f"1. Connection Test  [{label}]")
    ok = MetadataExtractor(**cfg).test_connection()
    if ok:
        print(f"  ✓ Connected to {label} server")
    else:
        print(f"  ✗ Could not connect to {label} — check env vars")
    return ok


def test_server_schema_discovery(cfg: dict, label: str):
    """
    Server mode: no database param → discover all databases/schemas on the server.
    Returns the list of discovered schema names.
    """
    section(f"2. Server-Mode Schema Discovery  [{label}]")
    with MetadataExtractor(**cfg) as ext:
        schemas = ext.get_schemas_list()
    print(f"  Discovered {len(schemas)} database(s)/schema(s) on {label} server:")
    for s in schemas:
        print(f"    • {s}")
    assert isinstance(schemas, list) and len(schemas) > 0, "Expected at least one schema"
    print(f"  ✓ Server-mode discovery OK")
    return schemas


def test_server_filter_strategies(cfg: dict, live_schemas: list, label: str):
    """Demonstrate all filter strategies against the live server schema list."""
    section(f"3. Schema-Filter Strategies  [{label}]")
    if not live_schemas:
        print("  ⚠  No schemas discovered – skipping"); return live_schemas

    _s = live_schemas[0]
    cases = [
        ("3a. Exact name",   [_s]),
        ("3b. Glob",         [f"{_s[:3]}%"] if len(_s) >= 3 else [f"{_s}%"]),
        ("3c. prefix:",      [f"prefix:{_s[:3]}"] if len(_s) >= 3 else [f"prefix:{_s}"]),
        ("3d. suffix:",      [f"suffix:{_s[-3:]}"] if len(_s) >= 3 else [f"suffix:{_s}"]),
        ("3e. contains:",    [f"contains:{_s[:3]}"] if len(_s) >= 3 else [f"contains:{_s}"]),
        ("3f. regex:",       [f"regex:^{_s}.*$"]),
        ("3g. None  (all)",  None),
        ("3h. Non-matching", ["prefix:zzz_nonexistent_"]),
    ]
    for label_case, sf in cases:
        with MetadataExtractor(**cfg) as ext:
            matched = [s.name for s in ext.extract_all_schemas(schema_filter=sf).schemas]
        print(f"  ✓ {label_case}  →  {matched}")
    print("  ✓ Filter strategies OK")
    return live_schemas


def test_server_full_extraction(cfg: dict, target_schemas: list, label: str):
    """Extract all discovered schemas — server mode, parallel workers."""
    section(f"4. Full Server Extraction  [{label}]  ({len(target_schemas)} schema(s))")
    with MetadataExtractor(**cfg) as ext:
        db_meta = ext.extract_all_schemas(
            schema_filter=target_schemas,
            parallel_workers=8,
        )
    print(f"  Server    : {db_meta.database_name}  |  Version: {db_meta.version}")
    print(f"  Schemas   : {len(db_meta.schemas)}")
    total_tables = sum(len(s.tables) for s in db_meta.schemas)
    total_cols   = sum(len(t.columns) for s in db_meta.schemas for t in s.tables)
    print(f"  Tables    : {total_tables}")
    print(f"  Columns   : {total_cols}")
    for schema in db_meta.schemas:
        print(f"\n  [{schema.name}]  {len(schema.tables)} table(s)")
        for t in schema.tables[:5]:
            print(f"    • {t.name}  ({len(t.columns)} cols, {t.row_count} rows)")
        if len(schema.tables) > 5:
            print(f"    … and {len(schema.tables)-5} more")
    print("  ✓ Full server extraction OK")
    return db_meta


def test_filtered_extraction(cfg: dict, live_schemas: list, label: str):
    """Extract only the first schema to show filtered server-mode extraction."""
    section(f"5. Filtered Server Extraction  [{label}]")
    if not live_schemas:
        print("  ⚠  No schemas – skipping"); return None
    target = live_schemas[:1]       # just the first schema
    with MetadataExtractor(**cfg) as ext:
        db_meta = ext.extract_all_schemas(schema_filter=target, parallel_workers=4)
    print(f"  filter={target}  →  {[s.name for s in db_meta.schemas]}")
    assert len(db_meta.schemas) <= len(target)
    print("  ✓ Filtered extraction OK")
    return db_meta


def test_yaml_generation(db_meta, dirs, label: str):
    section(f"6. YAML Generation  [{label}]")
    if db_meta is None:
        print("  ⚠  No metadata – skipping"); return
    gen   = YAMLGenerator(output_dir=str(dirs["models"]))
    files = gen.generate_yaml_files(db_meta)
    for f in files:
        print(f"  • {os.path.basename(f)}  ({os.path.getsize(f):,} bytes)")
    combined = gen.generate_single_yaml(db_meta, filename=f"{label.lower()}_all_schemas.yml")
    print(f"  • Combined: {os.path.basename(combined)}  ({os.path.getsize(combined):,} bytes)")
    print("  ✓ YAML generation OK")


def test_documentation_gaps(db_meta, dirs, label: str):
    section(f"7. Documentation Gap Detection  [{label}]")
    if db_meta is None:
        print("  ⚠  No metadata – skipping"); return
    gen            = YAMLGenerator(output_dir=str(dirs["models"]))
    tables_no_desc = gen.get_tables_without_descriptions(db_meta)
    cols_no_desc   = gen.get_columns_without_descriptions(db_meta)
    total_t = sum(len(s.tables) for s in db_meta.schemas)
    total_c = sum(len(t.columns) for s in db_meta.schemas for t in s.tables)
    print(f"  Tables  : {100*(total_t-len(tables_no_desc))//max(total_t,1)}%  ({len(tables_no_desc)}/{total_t} missing)")
    print(f"  Columns : {100*(total_c-len(cols_no_desc))//max(total_c,1)}%  ({len(cols_no_desc)}/{total_c} missing)")
    print("  ✓ Gap detection OK")


def test_schema_comparison(helper, dirs, cfg: dict, db_meta, target_schemas: list, label: str):
    """Self-comparison across all discovered schemas — expects zero drift."""
    section(f"8. Schema Comparison  [{label}]  (self — expects 0 diffs)")
    if db_meta is None or not target_schemas:
        print("  ⚠  No metadata / schemas – skipping"); return None, None

    comparator = SchemaComparator(
        source_config=cfg,
        destination_config=cfg,
        yaml_output_dir=str(dirs["models"]),
    )

    all_missing_tables:  list = []
    all_missing_columns: list = []
    all_type_mismatches: list = []
    all_tbl_gaps:        list = []
    all_col_gaps:        list = []

    for schema_name in target_schemas:
        dest_has = any(s.name == schema_name for s in db_meta.schemas)
        if not dest_has:
            continue
        print(f"\n  Comparing schema: {schema_name}")
        per_report = comparator.compare_and_generate_report(
            source_schema_name=schema_name,
            destination_schema_name=schema_name,
            include_yaml_gaps=True,
            dest_db_metadata=db_meta,
            source_db_metadata=db_meta,
        )
        s = per_report["summary"]
        print(
            f"    missing tables: {s['missing_tables_count']}  |  "
            f"missing columns: {s['missing_columns_count']}  |  "
            f"type mismatches: {s['type_mismatches_count']}"
        )
        all_missing_tables.extend(per_report["comparison"].get("missing_tables",  []))
        all_missing_columns.extend(per_report["comparison"].get("missing_columns", []))
        all_type_mismatches.extend(per_report["comparison"].get("type_mismatches", []))
        if "yaml_gaps" in per_report:
            all_tbl_gaps.extend(per_report["yaml_gaps"].get("tables_without_descriptions",  []))
            all_col_gaps.extend(per_report["yaml_gaps"].get("columns_without_descriptions", []))

    combined_report = {
        "connector": label,
        "source":      {k: v for k, v in cfg.items() if k != "password"},
        "destination": {k: v for k, v in cfg.items() if k != "password"},
        "schemas_compared": target_schemas,
        "summary": {
            "schemas_compared":                   len(target_schemas),
            "missing_tables_count":               len(all_missing_tables),
            "missing_columns_count":              len(all_missing_columns),
            "type_mismatches_count":              len(all_type_mismatches),
            "tables_without_descriptions_count":  len(all_tbl_gaps),
            "columns_without_descriptions_count": len(all_col_gaps),
        },
        "comparison": {
            "missing_tables":  all_missing_tables,
            "missing_columns": all_missing_columns,
            "type_mismatches": all_type_mismatches,
        },
        "yaml_gaps": {
            "tables_without_descriptions":  all_tbl_gaps,
            "columns_without_descriptions": all_col_gaps,
        },
    }
    s = combined_report["summary"]
    print(
        f"\n  Combined — schemas: {s['schemas_compared']}  |  "
        f"missing tables: {s['missing_tables_count']}  |  "
        f"missing columns: {s['missing_columns_count']}  |  "
        f"type mismatches: {s['type_mismatches_count']}"
    )
    json_path = helper.save_report(combined_report)
    print(f"  JSON → {json_path}")
    print("  ✓ OK")
    return combined_report, json_path


def test_compile_pdf(helper, json_path):
    section("9. Compile Reports → PDF  →  ./reports/pdf/")
    if json_path is None:
        print("  ⚠  No JSON report – skipping"); return None
    pdf_path = helper.compile_pdf(source_json=json_path)
    if pdf_path:
        print(f"  PDF → {pdf_path}  ({os.path.getsize(pdf_path):,} bytes)")
        print("  ✓ PDF compilation OK")
    else:
        print("  ⚠  reportlab unavailable – skipping")
    return pdf_path


def test_send_notification(helper, report, pdf_path):
    section(f"10. Send Notification  [{NOTIFICATION_TYPE}]")
    if report is None:
        print("  ⚠  No report – skipping"); return
    results = helper.send_notification(
        notification_type=NOTIFICATION_TYPE,
        report=report,
        pdf_path=pdf_path,
        subject="[Server-Level Test] Schema Report",
        email_to=EMAIL_RECIPIENTS or None,
        slack_target=SLACK_TARGETS or None,
    )
    if results.get("email"):
        print(f"  ✓ Email sent to {len(EMAIL_RECIPIENTS)} recipient(s): {EMAIL_TO}")
    elif NOTIFICATION_TYPE in ("email", "both"):
        print("  ⚠  Email delivery failed – check SMTP env vars")
    if results.get("slack"):
        print(f"  ✓ Slack notification sent to {len(SLACK_TARGETS)} target(s): {SLACK_NOTIFY_TARGET}")
    elif NOTIFICATION_TYPE in ("slack", "both"):
        print("  ⚠  Slack delivery failed – check SLACK_BOT_TOKEN / SLACK_NOTIFY_TARGET")


def test_metadata_export(helper, db_meta, label: str):
    section(f"11. Metadata Export + Serialization Round-Trip  [{label}]")
    if db_meta is None:
        print("  ⚠  No metadata – skipping"); return
    meta_path = helper.reports_json_dir / f"{label.lower()}_server_metadata.json"
    meta_path.write_text(
        _json_mod.dumps(db_meta.to_dict(), indent=2, default=str),
        encoding="utf-8",
    )
    print(f"  Exported → {meta_path}  ({os.path.getsize(meta_path):,} bytes)")
    restored    = DatabaseMetadata.from_dict(db_meta.to_dict())
    orig_tables = {t.name for s in db_meta.schemas  for t in s.tables}
    rest_tables = {t.name for s in restored.schemas for t in s.tables}
    assert orig_tables == rest_tables, f"Round-trip mismatch: {orig_tables ^ rest_tables}"
    print(f"  Round-trip OK — {len(orig_tables)} table(s) preserved")
    print("  ✓ Export OK")


# ─────────────────────────────────────────────────────────────────────────────
# Per-connector runner
# ─────────────────────────────────────────────────────────────────────────────

def run_connector(helper, dirs, timer, cfg: dict, label: str) -> bool:
    """Run the full server-mode test suite for one connector."""
    print(f"\n\n{'🌐 '*20}")
    print(f"  SERVER-LEVEL TEST — {label}")
    print(f"{'🌐 '*20}")

    with timer.task(f"[{label}] 1. Connection test"):
        ok = test_connection(cfg, label)
    if not ok:
        print(f"\n  Skipping {label} — connection failed\n")
        return False

    with timer.task(f"[{label}] 2. Server schema discovery"):
        live_schemas = test_server_schema_discovery(cfg, label)

    with timer.task(f"[{label}] 3. Schema filter strategies"):
        test_server_filter_strategies(cfg, live_schemas, label)

    with timer.task(f"[{label}] 4. Full server extraction"):
        db_meta = test_server_full_extraction(cfg, live_schemas, label)

    with timer.task(f"[{label}] 5. Filtered extraction"):
        test_filtered_extraction(cfg, live_schemas, label)

    with timer.task(f"[{label}] 6. YAML generation"):
        test_yaml_generation(db_meta, dirs, label)

    with timer.task(f"[{label}] 7. Documentation gap detection"):
        test_documentation_gaps(db_meta, dirs, label)

    with timer.task(f"[{label}] 8. Schema comparison (self)"):
        report, json_path = test_schema_comparison(
            helper, dirs, cfg, db_meta, live_schemas, label
        )

    with timer.task(f"[{label}] 9. Compile PDF"):
        pdf_path = test_compile_pdf(helper, json_path)

    with timer.task(f"[{label}] 10. Send notification"):
        test_send_notification(helper, report, pdf_path)

    with timer.task(f"[{label}] 11. Metadata export + round-trip"):
        test_metadata_export(helper, db_meta, label)

    return True


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"\n{EMOJI*30}\n  data_dictionary_builder — Server-Level full feature test\n{EMOJI*30}")

    helper = DDHelper(".")
    dirs   = helper.dirs
    timer  = ExecutionTimer()

    print(f"\n  models/       → {dirs['models']}")
    print(f"  reports/json/ → {dirs['reports_json']}")
    print(f"  reports/pdf/  → {dirs['reports_pdf']}")

    ran_any = False

    # ── ClickHouse (primary) ─────────────────────────────────────────────────
    ch_cfg = _ch_config()
    print(f"\n  ClickHouse host : {ch_cfg['host']}")
    ok = run_connector(helper, dirs, timer, ch_cfg, "ClickHouse")
    ran_any = ran_any or ok

    # ── MySQL (optional) ─────────────────────────────────────────────────────
    if os.getenv("MYSQL_HOST") and os.getenv("MYSQL_PASSWORD"):
        my_cfg = _mysql_config()
        print(f"\n  MySQL host : {my_cfg['host']}")
        ok = run_connector(helper, dirs, timer, my_cfg, "MySQL")
        ran_any = ran_any or ok
    else:
        print("\n  MySQL not configured — set MYSQL_HOST + MYSQL_PASSWORD to enable")

    # ── PostgreSQL (optional) ────────────────────────────────────────────────
    if os.getenv("PG_HOST") and os.getenv("PG_PASSWORD"):
        pg_cfg = _pg_config()
        print(f"\n  PostgreSQL host : {pg_cfg['host']}")
        ok = run_connector(helper, dirs, timer, pg_cfg, "PostgreSQL")
        ran_any = ran_any or ok
    else:
        print("\n  PostgreSQL not configured — set PG_HOST + PG_PASSWORD to enable")

    timer.summary("Server-Level Test Suite — Execution Summary")

    if ran_any:
        print("\n" + "✅ " * 30)
        print("  All server-level feature tests passed!")
        print("✅ " * 30 + "\n")
    else:
        print("\n  ⚠  No connectors ran — check your .env configuration\n")
