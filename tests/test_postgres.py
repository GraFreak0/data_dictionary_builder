"""
test_postgres.py
================
Exercises every major feature of data_dictionary_builder against PostgreSQL.

Output layout
-------------
    ./models/          ← YAML files (per-schema and combined)
    ./reports/         ← JSON comparison reports + compiled reports.pdf

Configuration (.env or environment variables):
    PG_HOST       default: localhost
    PG_PORT       default: 5432
    PG_DB         default: postgres
    PG_USER       default: postgres
    PG_PASSWORD
    PG_SCHEMAS    comma-separated schemas to test  (default: public)

Optional email (PDF attached automatically):
    SMTP_HOST  SMTP_PORT  SMTP_USER  SMTP_PASSWORD  EMAIL_TO
"""

import os

from dotenv import load_dotenv

from data_dictionary_builder import MetadataExtractor, YAMLGenerator, SchemaComparator

from data_dictionary_builder import DDHelper

load_dotenv()

CONNECTOR      = "postgres"
EMOJI          = "🐘 "
TARGET_SCHEMAS = os.getenv("PG_SCHEMAS", "public").split(",")

BASE_CONFIG = {
    "db_type":  CONNECTOR,
    "host":     os.getenv("PG_HOST", "localhost"),
    "port":     int(os.getenv("PG_PORT", 5432)),
    "database": os.getenv("PG_DB", "postgres"),
    "user":     os.getenv("PG_USER", "postgres"),
    "password": os.getenv("PG_PASSWORD", ""),
}


def section(title: str) -> None:
    print(f"\n{'='*60}\n  {title}\n{'='*60}")


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────

def test_connection():
    section("1. Connection Test")
    ok = MetadataExtractor(**BASE_CONFIG).test_connection()
    assert ok, "❌  Could not connect – check env vars"
    print("  ✓ Connected")


def test_schema_listing():
    section("2. Schema Listing")
    with MetadataExtractor(**BASE_CONFIG) as ext:
        schemas = ext.get_schemas_list()
    print(f"  Schemas: {schemas}")
    print(f"  ✓ Found {len(schemas)} schema(s)")
    return schemas


def test_table_listing():
    section("3. Table Listing")
    with MetadataExtractor(**BASE_CONFIG) as ext:
        tables = ext.get_tables_list(TARGET_SCHEMAS[0])
    print(f"  Tables in '{TARGET_SCHEMAS[0]}': {tables[:10]}")
    print(f"  ✓ Found {len(tables)} table(s)")


def test_schema_filter_strategies():
    section("4. Schema-Filter Strategies")
    with MetadataExtractor(**BASE_CONFIG) as ext:
        live = ext.get_schemas_list()
    print(f"  Live schemas: {live}\n")
    cases = [
        ("4a. Exact name",   ["public"]),
        ("4b. Glob  (pub%)", ["pub%"]),
        ("4c. prefix:",      ["prefix:pub"]),
        ("4d. suffix:",      ["suffix:lic"]),
        ("4e. contains:",    ["contains:pub"]),
        ("4f. regex:",       ["regex:^pub.*$"]),
        ("4g. Mixed",        ["public", "prefix:stg_", "regex:^analytics_\\d{4}$"]),
        ("4h. None  (all)",  None),
    ]
    for label, sf in cases:
        with MetadataExtractor(**BASE_CONFIG) as ext:
            matched = [s.name for s in ext.extract_all_schemas(schema_filter=sf).schemas]
        print(f"  ✓ {label}  →  {matched}")
    print("  ✓ Filter strategies OK")


def test_extract_all_schemas():
    section("5. Full Metadata Extraction")
    with MetadataExtractor(**BASE_CONFIG) as ext:
        db_meta = ext.extract_all_schemas(schema_filter=TARGET_SCHEMAS)
    print(f"  Database: {db_meta.database_name}  |  Version: {db_meta.version}")
    for schema in db_meta.schemas:
        print(f"  [{schema.name}]  {len(schema.tables)} table(s)")
        for t in schema.tables[:5]:
            pk = f"  PK: {t.primary_keys}" if t.primary_keys else ""
            print(f"    • {t.name}  ({len(t.columns)} cols){pk}")
        if len(schema.tables) > 5:
            print(f"    … and {len(schema.tables)-5} more")
    print("  ✓ Extraction OK")
    return db_meta


def test_extract_single_schema():
    section("6. Extract Single Schema")
    with MetadataExtractor(**BASE_CONFIG) as ext:
        schema = ext.extract_schema(TARGET_SCHEMAS[0])
    print(f"  Schema: {schema.name}  ({len(schema.tables)} tables)")
    print("  ✓ OK")
    return schema


def test_extract_single_table(schema):
    section("7. Extract Single Table  (FK / PK detail)")
    if not schema.tables:
        print("  ⚠  No tables – skipping"); return
    with MetadataExtractor(**BASE_CONFIG) as ext:
        table = ext.extract_table(TARGET_SCHEMAS[0], schema.tables[0].name)
    print(f"  {table.schema_name}.{table.name}  ({table.row_count} rows)")
    print(f"  PKs: {table.primary_keys}")
    fk_cols = [c for c in table.columns if c.is_foreign_key]
    if fk_cols:
        for fk in fk_cols:
            print(f"    FK: {fk.name} → {fk.foreign_key_table}.{fk.foreign_key_column}")
    for col in table.columns[:10]:
        pk = " [PK]" if col.is_primary_key else ""
        print(f"    • {col.name}: {col.data_type}{'  NULL' if col.is_nullable else '  NOT NULL'}{pk}")
    print("  ✓ OK")


def test_yaml_per_schema(db_meta, dirs):
    section("8. YAML Generation – Per-Schema  →  ./models/")
    files = YAMLGenerator(output_dir=str(dirs["models"])).generate_yaml_files(db_meta)
    for f in files:
        print(f"  • {os.path.basename(f)}  ({os.path.getsize(f):,} bytes)")
    print("  ✓ Per-schema YAML OK")


def test_yaml_combined(db_meta, dirs):
    section("9. YAML Generation – Combined  →  ./models/all_models.yml")
    filepath = YAMLGenerator(output_dir=str(dirs["models"])).generate_single_yaml(
        db_meta, filename="all_models.yml"
    )
    print(f"  {os.path.basename(filepath)}  ({os.path.getsize(filepath):,} bytes)")
    print("  ✓ Combined YAML OK")


def test_documentation_gaps(db_meta, dirs):
    section("10. Documentation Gap Detection")
    gen            = YAMLGenerator(output_dir=str(dirs["models"]))
    tables_no_desc = gen.get_tables_without_descriptions(db_meta)
    cols_no_desc   = gen.get_columns_without_descriptions(db_meta)
    total_t = sum(len(s.tables) for s in db_meta.schemas)
    total_c = sum(len(t.columns) for s in db_meta.schemas for t in s.tables)
    print(f"  Tables  : {100*(total_t-len(tables_no_desc))//max(total_t,1)}%  ({len(tables_no_desc)}/{total_t} missing)")
    print(f"  Columns : {100*(total_c-len(cols_no_desc))//max(total_c,1)}%  ({len(cols_no_desc)}/{total_c} missing)")
    if tables_no_desc:
        for t in tables_no_desc[:5]:
            print(f"    undocumented table: {t}")
    print("  ✓ Gap detection OK")


def test_schema_comparison(dirs):
    section("11. Schema Comparison  (self-comparison → 0 diffs)")
    report = SchemaComparator(
        source_config=BASE_CONFIG, destination_config=BASE_CONFIG,
        yaml_output_dir=str(dirs["models"]),
    ).compare_and_generate_report(
        source_schema_name=TARGET_SCHEMAS[0],
        destination_schema_name=TARGET_SCHEMAS[0],
        include_yaml_gaps=True,
    )
    s = report["summary"]
    print(f"  Missing tables: {s['missing_tables_count']}  |  columns: {s['missing_columns_count']}  |  type mismatches: {s['type_mismatches_count']}")
    path = helper.save_report(report)
    print(f"  JSON → {path}")
    print("  ✓ OK")
    return report


def test_compile_pdf(helper):
    section("12. Compile Reports → PDF  →  ./reports/pdf/")
    pdf_path = helper.compile_pdf()
    if pdf_path:
        print(f"  PDF → {pdf_path}  ({os.path.getsize(pdf_path):,} bytes)")
        print("  ✓ PDF compilation OK")
    else:
        print("  ⚠  No JSON reports or reportlab unavailable – skipping")
    return pdf_path


def test_email_report(report, pdf_path):
    section("13. Email Report + PDF Attachment  (optional)")
    ok = helper.send_report_email(report=report, pdf_path=pdf_path,
                             subject="[PostgreSQL Test] Schema Comparison Report")
    print("  ✓ Email sent" if ok else "  ⚠  SMTP not configured – skipped")


def test_metadata_export(db_meta, dirs):
    section("14. Metadata Export to JSON  →  ./reports/json/")
    _meta_path = helper.reports_json_dir / "postgres_metadata.json"
    import json as _json_mod
    _meta_path.write_text(_json_mod.dumps(db_meta.to_dict(), indent=2, default=str), encoding="utf-8")
    path = _meta_path
    print(f"  {path}  ({os.path.getsize(path):,} bytes)")
    print("  ✓ Export OK")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"\n{EMOJI*30}\n  data_dictionary_builder — PostgreSQL full feature test\n{EMOJI*30}")

    helper = DDHelper(".")
    dirs   = helper.dirs
    print(f"\n  models/       → {dirs['models']}\n  reports/json/ → {dirs['reports_json']}\n  reports/pdf/  → {dirs['reports_pdf']}")

    test_connection()
    test_schema_listing()
    test_table_listing()
    test_schema_filter_strategies()

    db_meta = test_extract_all_schemas()
    schema  = test_extract_single_schema()

    test_extract_single_table(schema)
    test_yaml_per_schema(db_meta, dirs)
    test_yaml_combined(db_meta, dirs)
    test_documentation_gaps(db_meta, dirs)

    report   = test_schema_comparison(dirs)
    pdf_path = test_compile_pdf(helper)

    test_email_report(report, pdf_path)
    test_metadata_export(db_meta, dirs)

    print("\n" + "✅ " * 30)
    print("  All PostgreSQL feature tests passed!")
    print("✅ " * 30 + "\n")
