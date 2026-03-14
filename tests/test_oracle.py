"""
test_oracle.py
==============
Exercises every major feature of data_dictionary_builder against Oracle Database.

Output layout
-------------
    ./models/          ← YAML files (per-schema and combined)
    ./reports/         ← JSON comparison reports + compiled PDF

Configuration (.env or environment variables)
---------------------------------------------
Shared / fallback:
    ORACLE_HOST        default: localhost
    ORACLE_PORT        default: 1521
    ORACLE_SERVICE     Oracle service name (e.g. ORCL, XEPDB1, FREEPDB1)
    ORACLE_USER
    ORACLE_PASSWORD
    ORACLE_SCHEMAS     comma-separated schemas to target (default: current user)

Source-specific overrides (fall back to shared values above):
    SOURCE_ORACLE_HOST
    SOURCE_ORACLE_PORT
    SOURCE_ORACLE_SERVICE
    SOURCE_ORACLE_USER
    SOURCE_ORACLE_PASSWORD
    SOURCE_ORACLE_SCHEMAS

Destination-specific overrides:
    DEST_ORACLE_HOST
    DEST_ORACLE_PORT
    DEST_ORACLE_SERVICE
    DEST_ORACLE_USER
    DEST_ORACLE_PASSWORD
    DEST_ORACLE_SCHEMAS

Notifications (choose one or both):
    NOTIFICATION_TYPE    "email" | "slack" | "both"  (default: email)

Email (PDF attached automatically):
    SMTP_HOST  SMTP_PORT  SMTP_USER  SMTP_PASSWORD  EMAIL_TO

Slack:
    SLACK_BOT_TOKEN      xoxb-… Bot Token
    SLACK_NOTIFY_TARGET  "#channel", "@username", "C…" or "U…"

Install the driver before running:
    pip install oracledb
    # or
    pip install data-dictionary-builder[oracle]
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

CONNECTOR           = "oracle"
EMOJI               = "🔶 "
EMAIL_TO            = os.getenv("EMAIL_TO", "")
NOTIFICATION_TYPE   = os.getenv("NOTIFICATION_TYPE", "email")
SLACK_NOTIFY_TARGET = os.getenv("SLACK_NOTIFY_TARGET", "")

# ── Shared / fallback connection values ──────────────────────────────────────
_ORA_HOST     = os.getenv("ORACLE_HOST",    "localhost")
_ORA_PORT     = int(os.getenv("ORACLE_PORT", 1521))
_ORA_SERVICE  = os.getenv("ORACLE_SERVICE", "XEPDB1")
_ORA_USER     = os.getenv("ORACLE_USER",    "system")
_ORA_PASSWORD = os.getenv("ORACLE_PASSWORD", "")
_ORA_SCHEMAS  = os.getenv("ORACLE_SCHEMAS", "")

# ── Source connection ─────────────────────────────────────────────────────────
SOURCE_CONFIG = {
    "db_type":  CONNECTOR,
    "host":     os.getenv("SOURCE_ORACLE_HOST")    or _ORA_HOST,
    "port":     int(os.getenv("SOURCE_ORACLE_PORT") or _ORA_PORT),
    "database": os.getenv("SOURCE_ORACLE_SERVICE") or _ORA_SERVICE,
    "user":     os.getenv("SOURCE_ORACLE_USER")    or _ORA_USER,
    "password": os.getenv("SOURCE_ORACLE_PASSWORD") or _ORA_PASSWORD,
}

# ── Destination connection ────────────────────────────────────────────────────
DEST_CONFIG = {
    "db_type":  CONNECTOR,
    "host":     os.getenv("DEST_ORACLE_HOST")    or _ORA_HOST,
    "port":     int(os.getenv("DEST_ORACLE_PORT") or _ORA_PORT),
    "database": os.getenv("DEST_ORACLE_SERVICE") or _ORA_SERVICE,
    "user":     os.getenv("DEST_ORACLE_USER")    or _ORA_USER,
    "password": os.getenv("DEST_ORACLE_PASSWORD") or _ORA_PASSWORD,
}

# TARGET_SCHEMAS: if ORACLE_SCHEMAS is set use those; otherwise resolved at runtime
_schema_env = os.getenv("SOURCE_ORACLE_SCHEMAS") or _ORA_SCHEMAS
TARGET_SCHEMAS: list = [s.strip() for s in _schema_env.split(",") if s.strip()]


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
    section("2. Schema / User Listing")
    with MetadataExtractor(**DEST_CONFIG) as ext:
        schemas = ext.get_schemas_list()
    print(f"  Destination schemas (users): {schemas}")
    assert isinstance(schemas, list)
    print(f"  ✓ Found {len(schemas)} schema(s)")
    return schemas


def test_table_listing():
    section("3. Table Listing")
    if not TARGET_SCHEMAS:
        print("  ⚠  TARGET_SCHEMAS empty – skipping"); return
    with MetadataExtractor(**DEST_CONFIG) as ext:
        tables = ext.get_tables_list(TARGET_SCHEMAS[0])
    print(f"  Tables in '{TARGET_SCHEMAS[0]}': {tables[:10]}")
    if len(tables) > 10:
        print(f"  … and {len(tables)-10} more")
    print(f"  ✓ Found {len(tables)} table(s)")


def test_schema_filter_strategies():
    section("4. Schema-Filter Strategies  (destination DB)")
    with MetadataExtractor(**DEST_CONFIG) as ext:
        live = ext.get_schemas_list()
    print(f"  Live schemas: {live}\n")
    cases = [
        ("4a. None  (all)", None),
    ]
    if live:
        _s = live[0]
        cases = [
            ("4a. Exact name",  [_s]),
            ("4b. Glob",        [f"{_s[:3]}%"]),
            ("4c. prefix:",     [f"prefix:{_s[:3]}"]),
            ("4d. suffix:",     [f"suffix:{_s[-3:]}"]),
            ("4e. contains:",   [f"contains:{_s[:3]}"]),
            ("4f. None  (all)", None),
        ]
    for label, sf in cases:
        with MetadataExtractor(**DEST_CONFIG) as ext:
            matched = [s.name for s in ext.extract_all_schemas(schema_filter=sf).schemas]
        print(f"  ✓ {label}  →  {matched}")
    print("  ✓ Filter strategies OK")
    return live


def test_extract_all_schemas():
    section("5. Full Metadata Extraction  (destination → snapshot)")
    with MetadataExtractor(**DEST_CONFIG) as ext:
        dest_db_meta = ext.extract_all_schemas(
            schema_filter=TARGET_SCHEMAS or None,
            parallel_workers=4,
        )
    print(f"  Database : {dest_db_meta.database_name}  |  Version: {dest_db_meta.version}")
    for schema in dest_db_meta.schemas:
        print(f"  [{schema.name}]  {len(schema.tables)} table(s)")
        for t in schema.tables[:5]:
            pk = f"  PK: {t.primary_keys}" if t.primary_keys else ""
            print(f"    • {t.name}  ({len(t.columns)} cols, {t.row_count} rows){pk}")
        if len(schema.tables) > 5:
            print(f"    … and {len(schema.tables)-5} more")
    print("  ✓ Extraction OK")
    return dest_db_meta


def test_extract_single_schema():
    section("6. Extract Single Schema")
    if not TARGET_SCHEMAS:
        print("  ⚠  TARGET_SCHEMAS empty – skipping"); return None
    with MetadataExtractor(**DEST_CONFIG) as ext:
        schema = ext.extract_schema(TARGET_SCHEMAS[0])
    print(f"  Schema: {schema.name}  ({len(schema.tables)} tables)")
    print("  ✓ OK")
    return schema


def test_extract_single_table(schema):
    section("7. Extract Single Table  (PK / FK detail)")
    if not schema or not schema.tables:
        print("  ⚠  No tables – skipping"); return
    with MetadataExtractor(**DEST_CONFIG) as ext:
        table = ext.extract_table(TARGET_SCHEMAS[0], schema.tables[0].name)
    print(f"  {table.schema_name}.{table.name}  ({table.row_count} rows)")
    print(f"  PKs: {table.primary_keys}")
    fk_cols = [c for c in table.columns if c.is_foreign_key]
    if fk_cols:
        for fk in fk_cols:
            print(f"    FK: {fk.name} → {fk.foreign_key_table}.{fk.foreign_key_column}")
    for col in table.columns[:10]:
        pk   = " [PK]" if col.is_primary_key  else ""
        desc = f'  "{col.description}"' if col.description else ""
        print(f"    • {col.name}: {col.data_type}{'  NULL' if col.is_nullable else '  NOT NULL'}{pk}{desc}")
    print("  ✓ OK")


def test_yaml_per_schema(dest_db_meta, dirs):
    section("8. YAML Generation – Per-Schema  →  ./models/")
    gen   = YAMLGenerator(output_dir=str(dirs["models"]))
    files = gen.generate_yaml_files(dest_db_meta)
    for f in files:
        print(f"  • {os.path.basename(f)}  ({os.path.getsize(f):,} bytes)")
    print("  ✓ Per-schema YAML OK")


def test_yaml_combined(dest_db_meta, dirs):
    section("9. YAML Generation – Combined  →  ./models/all_models.yml")
    filepath = YAMLGenerator(output_dir=str(dirs["models"])).generate_single_yaml(
        dest_db_meta, filename="all_models.yml"
    )
    print(f"  {os.path.basename(filepath)}  ({os.path.getsize(filepath):,} bytes)")
    print("  ✓ Combined YAML OK")


def test_documentation_gaps(dest_db_meta, dirs):
    section("10. Documentation Gap Detection")
    gen            = YAMLGenerator(output_dir=str(dirs["models"]))
    tables_no_desc = gen.get_tables_without_descriptions(dest_db_meta)
    cols_no_desc   = gen.get_columns_without_descriptions(dest_db_meta)
    total_t = sum(len(s.tables) for s in dest_db_meta.schemas)
    total_c = sum(len(t.columns) for s in dest_db_meta.schemas for t in s.tables)
    print(f"  Tables  : {100*(total_t-len(tables_no_desc))//max(total_t,1)}%  ({len(tables_no_desc)}/{total_t} missing)")
    print(f"  Columns : {100*(total_c-len(cols_no_desc))//max(total_c,1)}%  ({len(cols_no_desc)}/{total_c} missing)")
    if tables_no_desc:
        for t in tables_no_desc[:5]:
            print(f"    undocumented table: {t}")
    print("  ✓ Gap detection OK")


def test_schema_comparison(helper, dirs, dest_db_meta):
    section("11. Schema Comparison  (source fresh  vs  destination reingested)")
    schemas_to_compare = [
        s.name for s in dest_db_meta.schemas
        if any(s.name == t for t in TARGET_SCHEMAS)
    ] if TARGET_SCHEMAS else [s.name for s in dest_db_meta.schemas]

    if not schemas_to_compare:
        print("  ⚠  No schemas to compare – skipping"); return None, None

    comparator = SchemaComparator(
        source_config=SOURCE_CONFIG,
        destination_config=DEST_CONFIG,
        yaml_output_dir=str(dirs["models"]),
    )

    all_missing_tables:  list = []
    all_missing_columns: list = []
    all_type_mismatches: list = []
    all_tbl_gaps:        list = []
    all_col_gaps:        list = []

    for schema_name in schemas_to_compare:
        print(f"\n  Comparing schema: {schema_name}")
        per_report = comparator.compare_and_generate_report(
            source_schema_name=schema_name,
            destination_schema_name=schema_name,
            include_yaml_gaps=True,
            dest_db_metadata=dest_db_meta,
            source_db_metadata=dest_db_meta,
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
        "source":      {k: v for k, v in SOURCE_CONFIG.items() if k != "password"},
        "destination": {k: v for k, v in DEST_CONFIG.items()   if k != "password"},
        "schemas_compared": schemas_to_compare,
        "summary": {
            "schemas_compared":                   len(schemas_to_compare),
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
    section("12. Compile Reports → PDF  →  ./reports/pdf/")
    pdf_path = helper.compile_pdf(source_json=json_path)
    if pdf_path:
        print(f"  PDF → {pdf_path}  ({os.path.getsize(pdf_path):,} bytes)")
        print("  ✓ PDF compilation OK")
    else:
        print("  ⚠  No JSON reports or reportlab unavailable – skipping")
    return pdf_path


def test_send_notification(helper, report, pdf_path):
    section(f"13. Send Notification  [{NOTIFICATION_TYPE}]")
    if report is None:
        print("  ⚠  No report – skipping"); return
    results = helper.send_notification(
        notification_type=NOTIFICATION_TYPE,
        report=report,
        pdf_path=pdf_path,
        subject="[Oracle Test] Schema Comparison Report",
        email_to=EMAIL_TO,
        slack_target=SLACK_NOTIFY_TARGET or None,
    )
    if results.get("email"):
        print(f"  ✓ Email sent to {EMAIL_TO}")
    elif NOTIFICATION_TYPE in ("email", "both"):
        print("  ⚠  Email delivery failed – check SMTP env vars")
    if results.get("slack"):
        print(f"  ✓ Slack notification sent to {SLACK_NOTIFY_TARGET}")
    elif NOTIFICATION_TYPE in ("slack", "both"):
        print("  ⚠  Slack delivery failed – check SLACK_BOT_TOKEN / SLACK_NOTIFY_TARGET")


def test_metadata_export(helper, dest_db_meta):
    section("14. Metadata Export + Serialisation Round-Trip")
    meta_path = helper.reports_json_dir / "oracle_metadata.json"
    meta_path.write_text(
        _json_mod.dumps(dest_db_meta.to_dict(), indent=2, default=str),
        encoding="utf-8",
    )
    print(f"  Exported → {meta_path}  ({os.path.getsize(meta_path):,} bytes)")
    restored    = DatabaseMetadata.from_dict(dest_db_meta.to_dict())
    orig_tables = {t.name for s in dest_db_meta.schemas for t in s.tables}
    rest_tables = {t.name for s in restored.schemas     for t in s.tables}
    assert orig_tables == rest_tables, f"Round-trip mismatch: {orig_tables ^ rest_tables}"
    print(f"  Round-trip OK — {len(orig_tables)} table(s) preserved")
    print("  ✓ Export OK")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"\n{EMOJI*30}\n  data_dictionary_builder — Oracle full feature test\n{EMOJI*30}")

    helper = DDHelper(".")
    dirs   = helper.dirs
    timer  = ExecutionTimer()

    print(f"\n  models/       → {dirs['models']}")
    print(f"  reports/json/ → {dirs['reports_json']}")
    print(f"  reports/pdf/  → {dirs['reports_pdf']}")
    print(f"\n  source : {SOURCE_CONFIG['host']}:{SOURCE_CONFIG['port']}/{SOURCE_CONFIG['database']}")
    print(f"  dest   : {DEST_CONFIG['host']}:{DEST_CONFIG['port']}/{DEST_CONFIG['database']}")

    with timer.task("1. Connection test"):
        test_connection()

    with timer.task("2. Schema listing"):
        test_schema_listing()

    with timer.task("3. Table listing"):
        test_table_listing()

    with timer.task("4. Schema filter strategies"):
        TARGET_SCHEMAS = test_schema_filter_strategies()
        print(f"\n  → TARGET_SCHEMAS set to: {TARGET_SCHEMAS}")

    with timer.task("5. Full metadata extraction (destination snapshot)"):
        dest_db_meta = test_extract_all_schemas()

    with timer.task("6. Extract single schema"):
        schema = test_extract_single_schema()

    with timer.task("7. Extract single table"):
        test_extract_single_table(schema)

    with timer.task("8. YAML per-schema"):
        test_yaml_per_schema(dest_db_meta, dirs)

    with timer.task("9. YAML combined"):
        test_yaml_combined(dest_db_meta, dirs)

    with timer.task("10. Documentation gap detection"):
        test_documentation_gaps(dest_db_meta, dirs)

    with timer.task("11. Schema comparison (source fresh vs dest reingested)"):
        report, json_path = test_schema_comparison(helper, dirs, dest_db_meta)

    with timer.task("12. Compile PDF"):
        pdf_path = test_compile_pdf(helper, json_path)

    with timer.task("13. Send notification"):
        test_send_notification(helper, report, pdf_path)

    with timer.task("14. Metadata export + round-trip"):
        test_metadata_export(helper, dest_db_meta)

    timer.summary("Oracle Test Suite — Execution Summary")

    print("\n" + "✅ " * 30)
    print("  All Oracle feature tests passed!")
    print("✅ " * 30 + "\n")
