"""
test_spanner.py
===============
Exercises every major feature of data_dictionary_builder against Google Cloud Spanner.

Output layout
-------------
    ./models/          ← YAML files (per-schema and combined)
    ./reports/         ← JSON comparison reports + compiled reports.pdf

Configuration (.env or environment variables)
---------------------------------------------
    SPANNER_INSTANCE               e.g. my-instance
    SPANNER_DATABASE               e.g. my-database
    SPANNER_PROJECT                e.g. my-gcp-project  (optional if ADC is set)
    GOOGLE_APPLICATION_CREDENTIALS path to service-account JSON key

Spanner always exposes a single schema named "public", so all filter
strategies are demonstrated against that real schema name. A non-matching
case verifies zero-match behaviour.

Source-specific overrides (fall back to shared values above):
    SOURCE_SPANNER_INSTANCE
    SOURCE_SPANNER_DATABASE
    SOURCE_SPANNER_PROJECT

Destination-specific overrides:
    DEST_SPANNER_INSTANCE
    DEST_SPANNER_DATABASE
    DEST_SPANNER_PROJECT

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

CONNECTOR           = "spanner"
EMOJI               = "☁️  "
NOTIFICATION_TYPE   = os.getenv("NOTIFICATION_TYPE", "email")
EMAIL_RECIPIENTS    = [e.strip() for e in os.getenv("EMAIL_TO", "").split(",") if e.strip()]
SLACK_TARGETS       = [t.strip() for t in os.getenv("SLACK_NOTIFY_TARGET", "").split(",") if t.strip()]
EMAIL_TO            = ", ".join(EMAIL_RECIPIENTS)
SLACK_NOTIFY_TARGET = ", ".join(SLACK_TARGETS)
TARGET_SCHEMA       = "public"        # Spanner always returns a single "public" schema

# ── Shared / fallback connection values ──────────────────────────────────────
_SP_INSTANCE = os.getenv("SPANNER_INSTANCE", "")
_SP_DATABASE = os.getenv("SPANNER_DATABASE", "")
_SP_PROJECT  = os.getenv("SPANNER_PROJECT",  "")

def _build_config(instance, database, project) -> dict:
    cfg = {"db_type": CONNECTOR, "instance_id": instance, "database_id": database}
    if project:
        cfg["project_id"] = project
    return cfg

# ── Source connection ─────────────────────────────────────────────────────────
SOURCE_CONFIG = _build_config(
    instance = os.getenv("SOURCE_SPANNER_INSTANCE") or _SP_INSTANCE,
    database = os.getenv("SOURCE_SPANNER_DATABASE") or _SP_DATABASE,
    project  = os.getenv("SOURCE_SPANNER_PROJECT")  or _SP_PROJECT,
)

# ── Destination connection ────────────────────────────────────────────────────
DEST_CONFIG = _build_config(
    instance = os.getenv("DEST_SPANNER_INSTANCE") or _SP_INSTANCE,
    database = os.getenv("DEST_SPANNER_DATABASE") or _SP_DATABASE,
    project  = os.getenv("DEST_SPANNER_PROJECT")  or _SP_PROJECT,
)

TARGET_SCHEMAS = [TARGET_SCHEMA]


def section(title: str) -> None:
    print(f"\n{'='*60}\n  {title}\n{'='*60}")


def _configured() -> bool:
    if not _SP_INSTANCE or not _SP_DATABASE:
        print("  ⚠  SPANNER_INSTANCE / SPANNER_DATABASE not set – skipping")
        return False
    return True


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────

def test_connection():
    section("1. Connection Test")
    if not _configured():
        return
    src_ok  = MetadataExtractor(**SOURCE_CONFIG).test_connection()
    dest_ok = MetadataExtractor(**DEST_CONFIG).test_connection()
    assert src_ok,  "❌  Could not connect to SOURCE – check env vars / ADC"
    assert dest_ok, "❌  Could not connect to DEST   – check env vars / ADC"
    print("  ✓ Source connected")
    print("  ✓ Destination connected")


def test_schema_listing():
    section("2. Schema Listing  (Spanner always returns ['public'])")
    if not _configured():
        return ["public"]
    with MetadataExtractor(**DEST_CONFIG) as ext:
        schemas = ext.get_schemas_list()
    print(f"  Schemas: {schemas}")
    assert schemas == ["public"], f"Expected ['public'], got {schemas}"
    print("  ✓ Schema listing OK")
    return schemas


def test_table_listing():
    section("3. Table Listing")
    if not _configured():
        return
    with MetadataExtractor(**DEST_CONFIG) as ext:
        tables = ext.get_tables_list(TARGET_SCHEMA)
    print(f"  Tables ({len(tables)}): {tables[:10]}")
    if len(tables) > 10:
        print(f"  … and {len(tables)-10} more")
    print(f"  ✓ Found {len(tables)} table(s)")


def test_schema_filter_strategies():
    section("4. Schema-Filter Strategies  (destination DB)")
    if not _configured():
        return [TARGET_SCHEMA]
    with MetadataExtractor(**DEST_CONFIG) as ext:
        live = ext.get_schemas_list()
    print(f"  Live schemas: {live}\n")
    cases = [
        ("4a. Exact name",          ["public"],                                        True),
        ("4b. Glob  (pub%)",        ["pub%"],                                          True),
        ("4c. prefix:",             ["prefix:pub"],                                    True),
        ("4d. suffix:",             ["suffix:lic"],                                    True),
        ("4e. contains:",           ["contains:pub"],                                  True),
        ("4f. regex:",              ["regex:^pub.*$"],                                 True),
        ("4g. Mixed",               ["public", "prefix:stg_", "regex:^analytics_\\d{4}$"], True),
        ("4h. None  (all)",         None,                                              True),
        ("4i. Non-matching filter", ["prefix:stg_"],                                   False),
    ]
    for label, sf, expect_public in cases:
        with MetadataExtractor(**DEST_CONFIG) as ext:
            matched = [s.name for s in ext.extract_all_schemas(schema_filter=sf).schemas]
        ok = ("public" in matched) == expect_public
        print(f"  {'✓' if ok else '✗'} {label}  →  {matched}")
    print("  ✓ Filter strategies OK")
    return live


def test_extract_all_schemas():
    section("5. Full Metadata Extraction  (destination → snapshot)")
    if not _configured():
        return None
    with MetadataExtractor(**DEST_CONFIG) as ext:
        dest_db_meta = ext.extract_all_schemas(
            schema_filter=TARGET_SCHEMAS,
            parallel_workers=4,
        )
    print(f"  Instance: {dest_db_meta.database_name}  |  Type: {dest_db_meta.database_type}")
    for schema in dest_db_meta.schemas:
        print(f"  [{schema.name}]  {len(schema.tables)} table(s)")
        for t in schema.tables[:5]:
            print(f"    • {t.name}  ({len(t.columns)} cols, {t.row_count} rows)")
        if len(schema.tables) > 5:
            print(f"    … and {len(schema.tables)-5} more")
    print("  ✓ Extraction OK")
    return dest_db_meta


def test_extract_single_schema():
    section("6. Extract Single Schema")
    if not _configured():
        return None
    with MetadataExtractor(**DEST_CONFIG) as ext:
        schema = ext.extract_schema(TARGET_SCHEMA)
    print(f"  Schema: {schema.name}  ({len(schema.tables)} tables)")
    print("  ✓ OK")
    return schema


def test_extract_single_table(schema):
    section("7. Extract Single Table  (PK detail)")
    if schema is None or not schema.tables:
        print("  ⚠  No schema / tables – skipping"); return
    with MetadataExtractor(**DEST_CONFIG) as ext:
        table = ext.extract_table(TARGET_SCHEMA, schema.tables[0].name)
    print(f"  {table.schema_name}.{table.name}  ({table.row_count} rows)")
    print(f"  PKs: {table.primary_keys}")
    for col in table.columns[:10]:
        pk = " [PK]" if col.is_primary_key else ""
        print(f"    • {col.name}: {col.data_type}{'  NULL' if col.is_nullable else '  NOT NULL'}{pk}")
    print("  ✓ OK")


def test_yaml_per_schema(dest_db_meta, dirs):
    section("8. YAML Generation – Per-Schema  →  ./models/")
    if dest_db_meta is None:
        print("  ⚠  No metadata – skipping"); return
    gen   = YAMLGenerator(output_dir=str(dirs["models"]))
    files = gen.generate_yaml_files(dest_db_meta)
    for f in files:
        print(f"  • {os.path.basename(f)}  ({os.path.getsize(f):,} bytes)")
    print("  ✓ Per-schema YAML OK")


def test_yaml_combined(dest_db_meta, dirs):
    section("9. YAML Generation – Combined  →  ./models/all_models.yml")
    if dest_db_meta is None:
        print("  ⚠  No metadata – skipping"); return
    filepath = YAMLGenerator(output_dir=str(dirs["models"])).generate_single_yaml(
        dest_db_meta, filename="all_models.yml"
    )
    print(f"  {os.path.basename(filepath)}  ({os.path.getsize(filepath):,} bytes)")
    print("  ✓ Combined YAML OK")


def test_documentation_gaps(dest_db_meta, dirs):
    section("10. Documentation Gap Detection")
    if dest_db_meta is None:
        print("  ⚠  No metadata – skipping"); return
    gen            = YAMLGenerator(output_dir=str(dirs["models"]))
    tables_no_desc = gen.get_tables_without_descriptions(dest_db_meta)
    cols_no_desc   = gen.get_columns_without_descriptions(dest_db_meta)
    total_t = sum(len(s.tables) for s in dest_db_meta.schemas)
    total_c = sum(len(t.columns) for s in dest_db_meta.schemas for t in s.tables)
    print(f"  Tables  : {100*(total_t-len(tables_no_desc))//max(total_t,1)}%  ({len(tables_no_desc)}/{total_t} missing)")
    print(f"  Columns : {100*(total_c-len(cols_no_desc))//max(total_c,1)}%  ({len(cols_no_desc)}/{total_c} missing)")
    print("  ✓ Gap detection OK")


def test_schema_comparison(helper, dirs, dest_db_meta):
    section("11. Schema Comparison  (source fresh  vs  destination reingested)")
    if not _configured() or dest_db_meta is None:
        print("  ⚠  Not configured or no metadata – skipping"); return None, None

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

    for schema_name in TARGET_SCHEMAS:
        dest_has = any(s.name == schema_name for s in dest_db_meta.schemas)
        if not dest_has:
            print(f"  ⚠  '{schema_name}' absent in destination snapshot – skipping")
            continue
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
        "source":      {k: v for k, v in SOURCE_CONFIG.items()},
        "destination": {k: v for k, v in DEST_CONFIG.items()},
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
    section("12. Compile Reports → PDF  →  ./reports/pdf/")
    if json_path is None:
        print("  ⚠  No JSON report – skipping"); return None
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
        subject="[Spanner Test] Schema Comparison Report",
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


def test_metadata_export(helper, dest_db_meta):
    section("14. Metadata Export + Serialization Round-Trip")
    if dest_db_meta is None:
        print("  ⚠  No metadata – skipping"); return
    meta_path = helper.reports_json_dir / "spanner_metadata.json"
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
    print(f"\n{EMOJI*20}\n  data_dictionary_builder — Spanner full feature test\n{EMOJI*20}")

    helper = DDHelper(".")
    dirs   = helper.dirs
    timer  = ExecutionTimer()

    print(f"\n  models/       → {dirs['models']}")
    print(f"  reports/json/ → {dirs['reports_json']}")
    print(f"  reports/pdf/  → {dirs['reports_pdf']}")
    print(f"\n  source instance : {SOURCE_CONFIG.get('instance_id', '—')}")
    print(f"  dest   instance : {DEST_CONFIG.get('instance_id', '—')}")

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

    timer.summary("Spanner Test Suite — Execution Summary")

    print("\n" + "✅ " * 30)
    print("  All Spanner feature tests completed!")
    print("✅ " * 30 + "\n")
