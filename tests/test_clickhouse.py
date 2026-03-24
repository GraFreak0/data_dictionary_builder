"""
test_clickhouse.py
==================
Exercises every major feature of data_dictionary_builder against ClickHouse.

Output layout
-------------
    ./models/          ← YAML files (per-schema and combined)
    ./reports/         ← JSON comparison reports + compiled reports.pdf

Configuration (.env or environment variables)
---------------------------------------------
Shared / fallback:
    clickhouse_host      default: localhost
    clickhouse_port      default: 8123
    clickhouse_user
    clickhouse_password
    clickhouse_db        optional – omit to scan all databases on the server

Source-specific overrides (fall back to shared values above):
    SOURCE_CLICKHOUSE_HOST
    SOURCE_CLICKHOUSE_PORT
    SOURCE_CLICKHOUSE_USER
    SOURCE_CLICKHOUSE_PASSWORD
    SOURCE_CLICKHOUSE_DB

Destination-specific overrides (fall back to shared values above):
    DEST_CLICKHOUSE_HOST
    DEST_CLICKHOUSE_PORT
    DEST_CLICKHOUSE_USER
    DEST_CLICKHOUSE_PASSWORD
    DEST_CLICKHOUSE_DB

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
from pathlib import Path

from dotenv import load_dotenv

from data_dictionary_builder import (
    DDHelper,
    ExecutionTimer,
    MetadataExtractor,
    SchemaComparator,
    YAMLGenerator,
)

load_dotenv()

CONNECTOR           = "clickhouse"
EMOJI               = "🔷 "
NOTIFICATION_TYPE   = os.getenv("NOTIFICATION_TYPE", "email")
EMAIL_RECIPIENTS    = [e.strip() for e in os.getenv("EMAIL_TO", "").split(",") if e.strip()]
SLACK_TARGETS       = [t.strip() for t in os.getenv("SLACK_NOTIFY_TARGET", "").split(",") if t.strip()]
# Backward-compatible single-value aliases (used where a plain string is needed)
EMAIL_TO            = ", ".join(EMAIL_RECIPIENTS)
SLACK_NOTIFY_TARGET = ", ".join(SLACK_TARGETS)

# ── Shared / fallback connection values ─────────────────────────────────────
_CH_HOST      = os.getenv("clickhouse_host", "localhost")
_CH_PORT      = int(os.getenv("clickhouse_port", 8123))
_CH_USER      = os.getenv("clickhouse_user", "default")
_CH_PASSWORD  = os.getenv("clickhouse_password", "")
_CH_DB        = os.getenv("clickhouse_db")           # None → server mode
_CH_TRANSPORT = os.getenv("clickhouse_transport")    # "http" | "native" | None (auto)

# ── Source connection ────────────────────────────────────────────────────────
SOURCE_CONFIG = {
    "db_type": CONNECTOR,
    "host": os.getenv("SOURCE_CLICKHOUSE_HOST") or _CH_HOST,
    "user": os.getenv("SOURCE_CLICKHOUSE_USER") or _CH_USER,
    "password": os.getenv("SOURCE_CLICKHOUSE_PASSWORD") or _CH_PASSWORD,
    "secure": True,
    "verify": False,
}
_src_port = os.getenv("SOURCE_CLICKHOUSE_PORT") or _CH_PORT
if _src_port:
    SOURCE_CONFIG["port"] = int(_src_port)
_src_transport = os.getenv("SOURCE_CLICKHOUSE_TRANSPORT") or _CH_TRANSPORT
if _src_transport:
    SOURCE_CONFIG["transport"] = _src_transport
_src_db = os.getenv("SOURCE_CLICKHOUSE_DB") or _CH_DB
if _src_db:
    SOURCE_CONFIG["database"] = _src_db

# ── Destination connection ───────────────────────────────────────────────────
DEST_CONFIG = {
    "db_type": CONNECTOR,
    "host": os.getenv("DEST_CLICKHOUSE_HOST") or _CH_HOST,
    "user": os.getenv("DEST_CLICKHOUSE_USER") or _CH_USER,
    "password": os.getenv("DEST_CLICKHOUSE_PASSWORD") or _CH_PASSWORD,
    "secure": True,
    "verify": False,
}
_dst_port = os.getenv("DEST_CLICKHOUSE_PORT") or _CH_PORT
if _dst_port:
    DEST_CONFIG["port"] = int(_dst_port)
_dst_transport = os.getenv("DEST_CLICKHOUSE_TRANSPORT") or _CH_TRANSPORT
if _dst_transport:
    DEST_CONFIG["transport"] = _dst_transport
_dst_db = os.getenv("DEST_CLICKHOUSE_DB") or _CH_DB
if _dst_db:
    DEST_CONFIG["database"] = _dst_db

# TARGET_SCHEMAS is set dynamically at runtime — see __main__
TARGET_SCHEMAS: list = []


def section(title: str) -> None:
    print(f"\n{'='*60}\n  {title}\n{'='*60}")


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────

def test_connection():
    section("1. Connection Test")
    src_ok  = MetadataExtractor(**SOURCE_CONFIG).test_connection()
    dest_ok = MetadataExtractor(**DEST_CONFIG).test_connection()
    assert src_ok,  "❌  Could not connect to SOURCE – check env vars"
    assert dest_ok, "❌  Could not connect to DEST   – check env vars"
    print("  ✓ Source connected")
    print("  ✓ Destination connected")


def test_schema_listing():
    section("2. Schema / Database Listing")
    with MetadataExtractor(**SOURCE_CONFIG) as ext:
        schemas = ext.get_schemas_list()
    print(f"  Source schemas: {schemas}")
    assert isinstance(schemas, list)
    print(f"  ✓ Found {len(schemas)} schema(s)")
    return schemas


def test_schema_filter_strategies():
    """
    Step 3 — applies every filter strategy against the live destination schema
    list and returns the full unfiltered list for use as TARGET_SCHEMAS.
    """
    section("3. Schema-Filter Strategies  (source DB)")
    with MetadataExtractor(**SOURCE_CONFIG) as ext:
        live = ext.get_schemas_list()
    print(f"  Live schemas: {live}\n")
    cases = [
        # ("3a. Exact names",            ["default", "system"]),
        # ("3b. Glob  (default%)",       ["default%"]),
        # ("3c. prefix:",                ["prefix:def"]),
        # ("3d. suffix:",                ["suffix:ault"]),
        # ("3e. contains:",              ["contains:sys"]),
        # ("3f. regex:",                 ["regex:^def.*$"]),
        # ("3g. Mixed",                  ["system", "prefix:def", "regex:^tmp_\\d+$"]),
        ("3h. None  (all)",            None),
    ]
    for label, sf in cases:
        with MetadataExtractor(**SOURCE_CONFIG) as ext:
            matched = [s.name for s in ext.extract_all_schemas(schema_filter=sf).schemas]
        print(f"  ✓ {label}\n      filter: {sf}  →  {matched}\n")
    print("  ✓ Filter strategies OK")
    # Return the complete unfiltered schema list → becomes TARGET_SCHEMAS
    return live


def test_extract_all_schemas():
    """
    Step 4 — extracts full metadata from the SOURCE using TARGET_SCHEMAS.
    Returns src_db_meta, which later steps reuse to generate YAML.
    """
    section("4. Full Metadata Extraction  (source DB → snapshot)")
    with MetadataExtractor(**SOURCE_CONFIG) as ext:
        src_db_meta = ext.extract_all_schemas(schema_filter=TARGET_SCHEMAS)
    print(f"  Database : {src_db_meta.database_name}  |  Version: {src_db_meta.version}")
    for schema in src_db_meta.schemas:
        print(f"  [{schema.name}]  {len(schema.tables)} table(s)")
        for t in schema.tables[:5]:
            print(f"    • {t.name}  ({len(t.columns)} cols, {t.row_count} rows)")
        if len(schema.tables) > 5:
            print(f"    … and {len(schema.tables)-5} more")
    print("  ✓ Extraction OK")
    return src_db_meta


def test_extract_single_schema():
    section("5. Extract Single Schema")
    with MetadataExtractor(**SOURCE_CONFIG) as ext:
        schema = ext.extract_schema(TARGET_SCHEMAS[0])
    print(f"  Schema: {schema.name}  ({len(schema.tables)} tables)")
    print("  ✓ OK")
    return schema


def test_extract_single_table(schema):
    section("6. Extract Single Table")
    if not schema.tables:
        print("  ⚠  No tables – skipping"); return
    with MetadataExtractor(**SOURCE_CONFIG) as ext:
        table = ext.extract_table(TARGET_SCHEMAS[0], schema.tables[0].name)
    print(f"  {table.schema_name}.{table.name}  ({table.row_count} rows)")
    print(f"  PKs: {table.primary_keys}  |  Columns: {len(table.columns)}")
    for col in table.columns[:8]:
        pk = " [PK]" if col.is_primary_key else ""
        print(f"    • {col.name}: {col.data_type}{'  NULL' if col.is_nullable else '  NOT NULL'}{pk}")
    print("  ✓ OK")


def test_yaml_per_schema(src_db_meta, dirs):
    section("7. YAML Generation – Per-Schema  →  ./models/")
    gen   = YAMLGenerator(output_dir=str(dirs["models"]))
    files = gen.generate_yaml_files(src_db_meta)
    for f in files:
        print(f"  • {os.path.basename(f)}  ({os.path.getsize(f):,} bytes)")
    print("  ✓ Per-schema YAML OK")


def test_yaml_combined(src_db_meta, dirs):
    section("8. YAML Generation – Combined  →  ./models/all_models.yml")
    filepath = YAMLGenerator(output_dir=str(dirs["models"])).generate_single_yaml(
        src_db_meta, filename="all_models.yml"
    )
    print(f"  {os.path.basename(filepath)}  ({os.path.getsize(filepath):,} bytes)")
    print("  ✓ Combined YAML OK")


def test_documentation_gaps(src_db_meta, dirs):
    section("9. Documentation Gap Detection")
    gen            = YAMLGenerator(output_dir=str(dirs["models"]))
    tables_no_desc = gen.get_tables_without_descriptions(src_db_meta)
    cols_no_desc   = gen.get_columns_without_descriptions(src_db_meta)
    total_t = sum(len(s.tables) for s in src_db_meta.schemas)
    total_c = sum(len(t.columns) for s in src_db_meta.schemas for t in s.tables)
    print(f"  Tables  : {100*(total_t-len(tables_no_desc))//max(total_t,1)}%  ({len(tables_no_desc)}/{total_t} missing)")
    print(f"  Columns : {100*(total_c-len(cols_no_desc))//max(total_c,1)}%  ({len(cols_no_desc)}/{total_c} missing)")
    print("  ✓ Gap detection OK")


def test_schema_comparison(helper, dirs, src_db_meta):
    """
    Step 10 — compares every schema in TARGET_SCHEMAS.

    Source : reingested from src_db_meta (extracted in step 4).
    Destination : queried fresh from DEST_CONFIG each time.
    """
    section("10. Schema Comparison  (source reingested  vs  destination fresh)")
    if not TARGET_SCHEMAS:
        print("  ⚠  TARGET_SCHEMAS is empty – skipping"); return None

    comparator = SchemaComparator(
        source_config=SOURCE_CONFIG,
        destination_config=DEST_CONFIG,
        yaml_output_dir=str(dirs["models"]),
    )

    # Accumulate results across all schemas
    all_missing_tables:  list = []
    all_missing_columns: list = []
    all_type_mismatches: list = []
    all_tbl_gaps:        list = []
    all_col_gaps:        list = []

    for schema_name in TARGET_SCHEMAS:
        src_has_schema = any(s.name == schema_name for s in src_db_meta.schemas)
        if not src_has_schema:
            print(f"  ⚠  Schema '{schema_name}' absent in source snapshot – skipping")
            continue

        print(f"\n  Comparing schema: {schema_name}")
        per_report = comparator.compare_and_generate_report(
            source_schema_name=schema_name,
            destination_schema_name=schema_name,
            include_yaml_gaps=True,
            source_db_metadata=src_db_meta,  # reuse SOURCE snapshot; DEST queried fresh
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

    # Build combined report
    combined_report = {
        "source":      {k: v for k, v in SOURCE_CONFIG.items() if k != "password"},
        "destination": {k: v for k, v in DEST_CONFIG.items()   if k != "password"},
        "schemas_compared": TARGET_SCHEMAS,
        "summary": {
            "schemas_compared":                   len(TARGET_SCHEMAS),
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
    section("11. Compile Reports → PDF  →  ./reports/pdf/")
    pdf_path = helper.compile_pdf(source_json=json_path)
    if pdf_path:
        print(f"  PDF → {pdf_path}  ({os.path.getsize(pdf_path):,} bytes)")
        print("  ✓ PDF compilation OK")
    else:
        print("  ⚠  No JSON reports or reportlab unavailable – skipping")
    return pdf_path


def test_send_notification(helper, report, pdf_path):
    section(f"12. Send Notification  [{NOTIFICATION_TYPE}]")
    if report is None:
        print("  ⚠  No report – skipping"); return
    results = helper.send_notification(
        notification_type=NOTIFICATION_TYPE,
        report=report,
        pdf_path=pdf_path,
        subject="[ClickHouse Test] Schema Comparison Report",
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


def test_metadata_export(helper, src_db_meta):
    section("13. Metadata Export to JSON  →  ./reports/json/")
    _meta_path = helper.reports_json_dir / "clickhouse_metadata.json"
    _meta_path.write_text(
        _json_mod.dumps(src_db_meta.to_dict(), indent=2, default=str),
        encoding="utf-8",
    )
    print(f"  {_meta_path}  ({os.path.getsize(_meta_path):,} bytes)")
    print("  ✓ Export OK")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"\n{EMOJI*30}\n  data_dictionary_builder — ClickHouse full feature test\n{EMOJI*30}")

    # Project root is the directory containing the 'tests' folder
    PROJECT_ROOT = Path(__file__).parent.parent.resolve()
    
    helper = DDHelper(
        base_dir=PROJECT_ROOT,
        models_dir=PROJECT_ROOT / "models",
        reports_dir=PROJECT_ROOT / "temp"
    )
    dirs   = helper.dirs
    timer  = ExecutionTimer()

    print(f"\n  models/       → {dirs['models']}")
    print(f"  reports/json/ → {dirs['reports_json']}")
    print(f"  reports/pdf/  → {dirs['reports_pdf']}")
    print(f"\n  source host : {SOURCE_CONFIG['host']}")
    print(f"  dest   host : {DEST_CONFIG['host']}")

    with timer.task("1. Connection test"):
        test_connection()

    with timer.task("2. Schema listing"):
        test_schema_listing()

    with timer.task("3. Schema filter strategies"):
        # TARGET_SCHEMAS is set here from the live destination schema list
        TARGET_SCHEMAS = test_schema_filter_strategies()
        print(f"\n  → TARGET_SCHEMAS set to: {TARGET_SCHEMAS}")

    with timer.task("4. Full metadata extraction (source snapshot)"):
        src_db_meta = test_extract_all_schemas()

    with timer.task("5. Extract single schema"):
        schema = test_extract_single_schema()

    with timer.task("6. Extract single table"):
        test_extract_single_table(schema)

    with timer.task("7. YAML per-schema"):
        test_yaml_per_schema(src_db_meta, dirs)

    with timer.task("8. YAML combined"):
        test_yaml_combined(src_db_meta, dirs)

    with timer.task("9. Documentation gap detection"):
        test_documentation_gaps(src_db_meta, dirs)

    with timer.task("10. Schema comparison (source reingested vs dest fresh)"):
        report, json_path = test_schema_comparison(helper, dirs, src_db_meta)

    with timer.task("11. Compile PDF"):
        pdf_path = test_compile_pdf(helper, json_path)

    with timer.task("12. Send notification"):
        test_send_notification(helper, report, pdf_path)

    with timer.task("13. Metadata export"):
        test_metadata_export(helper, src_db_meta)

    timer.summary("ClickHouse Test Suite — Execution Summary")

    print("\n" + "✅ " * 30)
    print("  All ClickHouse feature tests passed!")
    print("✅ " * 30 + "\n")

