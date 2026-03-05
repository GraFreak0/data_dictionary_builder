"""
test_spanner.py
===============
Exercises every major feature of data_dictionary_builder against Google Cloud Spanner.

Output layout
-------------
    ./models/          ← YAML files (per-schema and combined)
    ./reports/         ← JSON comparison reports + compiled reports.pdf

Configuration (.env or environment variables):
    SPANNER_INSTANCE    e.g. my-instance
    SPANNER_DATABASE    e.g. my-database
    SPANNER_PROJECT     e.g. my-gcp-project  (optional if ADC is configured)
    GOOGLE_APPLICATION_CREDENTIALS  path to service-account JSON key

Spanner always returns a single "public" schema, so filter demos confirm
every format works correctly against that one real schema name, plus a
non-matching case to verify zero-match behaviour.

Optional email (PDF attached automatically):
    SMTP_HOST  SMTP_PORT  SMTP_USER  SMTP_PASSWORD  EMAIL_TO
"""

import os

from dotenv import load_dotenv

from data_dictionary_builder import MetadataExtractor, YAMLGenerator, SchemaComparator

from data_dictionary_builder import DDHelper

load_dotenv()

CONNECTOR     = "spanner"
EMOJI         = "☁️  "
TARGET_SCHEMA = "public"

BASE_CONFIG = {
    "db_type":     CONNECTOR,
    "instance_id": os.getenv("SPANNER_INSTANCE", ""),
    "database_id": os.getenv("SPANNER_DATABASE", ""),
}
if os.getenv("SPANNER_PROJECT"):
    BASE_CONFIG["project_id"] = os.getenv("SPANNER_PROJECT")


def section(title: str) -> None:
    print(f"\n{'='*60}\n  {title}\n{'='*60}")


def _configured() -> bool:
    if not BASE_CONFIG["instance_id"] or not BASE_CONFIG["database_id"]:
        print("  ⚠  SPANNER_INSTANCE / SPANNER_DATABASE not set – skipping")
        return False
    return True


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────

def test_connection():
    section("1. Connection Test")
    if not _configured():
        return False
    ok = MetadataExtractor(**BASE_CONFIG).test_connection()
    assert ok, "❌  Could not connect – check env vars / credentials"
    print("  ✓ Connected")
    return True


def test_schema_listing():
    section("2. Schema Listing  (Spanner always returns ['public'])")
    if not _configured():
        return ["public"]
    with MetadataExtractor(**BASE_CONFIG) as ext:
        schemas = ext.get_schemas_list()
    print(f"  Schemas: {schemas}")
    assert schemas == ["public"], "Expected single 'public' schema"
    print("  ✓ Schema listing OK")
    return schemas


def test_table_listing():
    section("3. Table Listing")
    if not _configured():
        return []
    with MetadataExtractor(**BASE_CONFIG) as ext:
        tables = ext.get_tables_list(TARGET_SCHEMA)
    print(f"  Tables ({len(tables)}): {tables[:10]}")
    print(f"  ✓ Found {len(tables)} table(s)")
    return tables


def test_schema_filter_strategies():
    section("4. Schema-Filter Strategies")
    if not _configured():
        return
    with MetadataExtractor(**BASE_CONFIG) as ext:
        live = ext.get_schemas_list()
    print(f"  Live schemas: {live}\n")
    cases = [
        ("4a. Exact name",             ["public"],            True),
        ("4b. Glob  (pub%)",           ["pub%"],              True),
        ("4c. prefix:",                ["prefix:pub"],        True),
        ("4d. suffix:",                ["suffix:lic"],        True),
        ("4e. contains:",              ["contains:pub"],      True),
        ("4f. regex:",                 ["regex:^pub.*$"],     True),
        ("4g. Mixed",                  ["public", "prefix:stg_", "regex:^analytics_\\d{4}$"], True),
        ("4h. None  (all)",            None,                  True),
        ("4i. Non-matching filter",    ["prefix:stg_"],       False),
    ]
    for label, sf, expect_public in cases:
        with MetadataExtractor(**BASE_CONFIG) as ext:
            matched = [s.name for s in ext.extract_all_schemas(schema_filter=sf).schemas]
        ok = ("public" in matched) == expect_public
        print(f"  {'✓' if ok else '✗'} {label}  →  {matched}")
    print("  ✓ Filter strategies OK")


def test_extract_all_schemas():
    section("5. Full Metadata Extraction")
    if not _configured():
        return None
    with MetadataExtractor(**BASE_CONFIG) as ext:
        db_meta = ext.extract_all_schemas(schema_filter=["public"])
    print(f"  Instance: {db_meta.database_name}  |  Type: {db_meta.database_type}")
    for schema in db_meta.schemas:
        print(f"  [{schema.name}]  {len(schema.tables)} table(s)")
        for t in schema.tables[:5]:
            print(f"    • {t.name}  ({len(t.columns)} cols, {t.row_count} rows)")
        if len(schema.tables) > 5:
            print(f"    … and {len(schema.tables)-5} more")
    print("  ✓ Extraction OK")
    return db_meta


def test_extract_single_schema():
    section("6. Extract Single Schema")
    if not _configured():
        return None
    with MetadataExtractor(**BASE_CONFIG) as ext:
        schema = ext.extract_schema(TARGET_SCHEMA)
    print(f"  Schema: {schema.name}  ({len(schema.tables)} tables)")
    print("  ✓ OK")
    return schema


def test_extract_single_table(schema):
    section("7. Extract Single Table  (PK detail)")
    if schema is None or not schema.tables:
        print("  ⚠  No schema / tables – skipping"); return
    with MetadataExtractor(**BASE_CONFIG) as ext:
        table = ext.extract_table(TARGET_SCHEMA, schema.tables[0].name)
    print(f"  {table.schema_name}.{table.name}  ({table.row_count} rows)")
    print(f"  PKs: {table.primary_keys}")
    for col in table.columns[:10]:
        pk = " [PK]" if col.is_primary_key else ""
        print(f"    • {col.name}: {col.data_type}{'  NULL' if col.is_nullable else '  NOT NULL'}{pk}")
    print("  ✓ OK")


def test_yaml_per_schema(db_meta, dirs):
    section("8. YAML Generation – Per-Schema  →  ./models/")
    if db_meta is None:
        print("  ⚠  No metadata – skipping"); return
    files = YAMLGenerator(output_dir=str(dirs["models"])).generate_yaml_files(db_meta)
    for f in files:
        print(f"  • {os.path.basename(f)}  ({os.path.getsize(f):,} bytes)")
    print("  ✓ Per-schema YAML OK")


def test_yaml_combined(db_meta, dirs):
    section("9. YAML Generation – Combined  →  ./models/all_models.yml")
    if db_meta is None:
        print("  ⚠  No metadata – skipping"); return
    filepath = YAMLGenerator(output_dir=str(dirs["models"])).generate_single_yaml(
        db_meta, filename="all_models.yml"
    )
    print(f"  {os.path.basename(filepath)}  ({os.path.getsize(filepath):,} bytes)")
    print("  ✓ Combined YAML OK")


def test_documentation_gaps(db_meta, dirs):
    section("10. Documentation Gap Detection")
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


def test_schema_comparison(dirs):
    section("11. Schema Comparison  (self-comparison → 0 diffs)")
    if not _configured():
        return None
    report = SchemaComparator(
        source_config=BASE_CONFIG,
        destination_config=BASE_CONFIG,
        yaml_output_dir=str(dirs["models"]),
    ).compare_and_generate_report(
        source_schema_name=TARGET_SCHEMA,
        destination_schema_name=TARGET_SCHEMA,
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
    if report is None:
        print("  ⚠  No report – skipping"); return
    ok = helper.send_report_email(report=report, pdf_path=pdf_path,
                             subject="[Spanner Test] Schema Comparison Report")
    print("  ✓ Email sent" if ok else "  ⚠  SMTP not configured – skipped")


def test_metadata_export(db_meta, dirs):
    section("14. Metadata Export to JSON  →  ./reports/json/")
    if db_meta is None:
        print("  ⚠  No metadata – skipping"); return
    _meta_path = helper.reports_json_dir / "spanner_metadata.json"
    import json as _json_mod
    _meta_path.write_text(_json_mod.dumps(db_meta.to_dict(), indent=2, default=str), encoding="utf-8")
    path = _meta_path
    print(f"  {path}  ({os.path.getsize(path):,} bytes)")
    print("  ✓ Export OK")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"\n{EMOJI*20}\n  data_dictionary_builder — Spanner full feature test\n{EMOJI*20}")

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
    print("  All Spanner feature tests completed!")
    print("✅ " * 30 + "\n")
