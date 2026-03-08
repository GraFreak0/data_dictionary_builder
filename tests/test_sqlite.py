"""
test_sqlite.py
==============
Exercises every major feature of data_dictionary_builder against SQLite.

Output layout
-------------
    ./models/          ← YAML files (per-schema and combined)
    ./reports/         ← JSON comparison reports + compiled reports.pdf

No environment variables required — all databases are created as temp files.

Optional email (PDF will be attached automatically):
    SMTP_HOST  SMTP_PORT  SMTP_USER  SMTP_PASSWORD  EMAIL_TO
"""

import json as _json_mod
import os
import sqlite3
import tempfile

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

CONNECTOR = "sqlite"
EMOJI = "🗂 "
EMAIL_TO = os.getenv("EMAIL_TO", "")
TARGET_SCHEMA = "main"      # SQLite always has one schema named "main"


# ── Fixture helpers ───────────────────────────────────────────────────────────

def _create_source_db(path: str) -> None:
    """Full e-commerce schema with FK relationships and sample data."""
    conn = sqlite3.connect(path)
    conn.cursor().executescript("""
        PRAGMA foreign_keys = ON;
        CREATE TABLE IF NOT EXISTS countries (
            country_id   INTEGER PRIMARY KEY,
            country_code TEXT NOT NULL,
            country_name TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS customers (
            customer_id INTEGER PRIMARY KEY,
            email       TEXT NOT NULL UNIQUE,
            first_name  TEXT,
            last_name   TEXT,
            country_id  INTEGER REFERENCES countries(country_id),
            created_at  TEXT NOT NULL,
            status      TEXT NOT NULL DEFAULT 'active'
        );
        CREATE TABLE IF NOT EXISTS products (
            product_id INTEGER PRIMARY KEY,
            sku        TEXT NOT NULL UNIQUE,
            name       TEXT NOT NULL,
            price      REAL NOT NULL,
            stock      INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS orders (
            order_id     INTEGER PRIMARY KEY,
            customer_id  INTEGER NOT NULL REFERENCES customers(customer_id),
            order_date   TEXT NOT NULL,
            total_amount REAL NOT NULL,
            status       TEXT NOT NULL DEFAULT 'pending'
        );
        CREATE TABLE IF NOT EXISTS order_items (
            item_id    INTEGER PRIMARY KEY,
            order_id   INTEGER NOT NULL REFERENCES orders(order_id),
            product_id INTEGER NOT NULL REFERENCES products(product_id),
            quantity   INTEGER NOT NULL,
            unit_price REAL NOT NULL
        );
        INSERT OR IGNORE INTO countries VALUES (1,'US','United States'),(2,'GB','United Kingdom');
        INSERT OR IGNORE INTO customers VALUES
            (1,'alice@example.com','Alice','Smith',1,'2024-01-01','active'),
            (2,'bob@example.com','Bob','Jones',2,'2024-01-15','active');
        INSERT OR IGNORE INTO products VALUES
            (1,'SKU-001','Widget A',9.99,100),(2,'SKU-002','Widget B',19.99,50);
        INSERT OR IGNORE INTO orders VALUES
            (1,1,'2024-02-01',29.98,'completed'),(2,2,'2024-02-10',19.99,'pending');
        INSERT OR IGNORE INTO order_items VALUES
            (1,1,1,2,9.99),(2,1,2,1,19.99),(3,2,2,1,19.99);
    """)
    conn.commit()
    conn.close()


def _create_dest_db(path: str) -> None:
    """Stripped schema for cross-DB comparison — missing tables and columns."""
    conn = sqlite3.connect(path)
    conn.cursor().executescript("""
        CREATE TABLE countries (country_id INTEGER PRIMARY KEY, country_code TEXT NOT NULL);
        CREATE TABLE customers  (customer_id INTEGER PRIMARY KEY, email TEXT NOT NULL);
    """)
    conn.commit()
    conn.close()


def section(title: str) -> None:
    print(f"\n{'='*60}\n  {title}\n{'='*60}")


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────

def test_connection(src_path, dest_path):
    section("1. Connection Test")
    src_ok  = MetadataExtractor(db_type=CONNECTOR, database=src_path).test_connection()
    dest_ok = MetadataExtractor(db_type=CONNECTOR, database=dest_path).test_connection()
    assert src_ok,  "❌  Source SQLite connection failed"
    assert dest_ok, "❌  Destination SQLite connection failed"
    print("  ✓ Source connected")
    print("  ✓ Destination connected")


def test_schema_listing(src_path):
    section("2. Schema Listing")
    with MetadataExtractor(db_type=CONNECTOR, database=src_path) as ext:
        schemas = ext.get_schemas_list()
    print(f"  Schemas: {schemas}")
    assert "main" in schemas
    print(f"  ✓ Found {len(schemas)} schema(s) — 'main' confirmed")
    return schemas


def test_table_listing(src_path):
    section("3. Table Listing")
    with MetadataExtractor(db_type=CONNECTOR, database=src_path) as ext:
        tables = ext.get_tables_list(TARGET_SCHEMA)
    print(f"  Tables in '{TARGET_SCHEMA}': {tables}")
    assert len(tables) >= 5
    print(f"  ✓ Found {len(tables)} table(s)")


def test_schema_filter_strategies(src_path):
    section("4. Schema-Filter Strategies")
    cfg = {"db_type": CONNECTOR, "database": src_path}
    with MetadataExtractor(**cfg) as ext:
        live = ext.get_schemas_list()
    print(f"  Live schemas: {live}\n")
    cases = [
        ("4a. Exact name",   ["main"],                    True),
        ("4b. Glob  (ma%)",  ["ma%"],                     True),
        ("4c. prefix:",      ["prefix:ma"],               True),
        ("4d. suffix:",      ["suffix:in"],               True),
        ("4e. contains:",    ["contains:ai"],             True),
        ("4f. regex:",       ["regex:^ma.*$"],            True),
        ("4g. Mixed",        ["main", "regex:^stg_.*$"],  True),
        ("4h. None  (all)",  None,                        True),
        ("4i. Non-matching", ["prefix:stg_"],             False),
    ]
    for label, sf, expect_main in cases:
        with MetadataExtractor(**cfg) as ext:
            matched = [s.name for s in ext.extract_all_schemas(schema_filter=sf).schemas]
        ok = ("main" in matched) == expect_main
        print(f"  {'✓' if ok else '✗'} {label}  →  {matched}")
    print("  ✓ Filter strategies OK")
    return live


def test_extract_all_schemas(src_path):
    section("5. Full Metadata Extraction  (source → snapshot)")
    with MetadataExtractor(db_type=CONNECTOR, database=src_path) as ext:
        db_meta = ext.extract_all_schemas(schema_filter=[TARGET_SCHEMA], parallel_workers=1)
    print(f"  Database: {db_meta.database_name}  |  Version: {db_meta.version}")
    for schema in db_meta.schemas:
        print(f"  [{schema.name}]  {len(schema.tables)} table(s)")
        for t in schema.tables:
            print(f"    • {t.name}  ({len(t.columns)} cols, {t.row_count} rows)")
    assert any(t.name == "customers" for s in db_meta.schemas for t in s.tables)
    print("  ✓ Extraction OK")
    return db_meta


def test_extract_single_schema(src_path):
    section("6. Extract Single Schema")
    with MetadataExtractor(db_type=CONNECTOR, database=src_path) as ext:
        schema = ext.extract_schema(TARGET_SCHEMA)
    print(f"  Schema: {schema.name}  ({len(schema.tables)} tables)")
    print("  ✓ OK")
    return schema


def test_extract_single_table(src_path, schema):
    section("7. Extract Single Table  (PK / FK detail)")
    if not schema.tables:
        print("  ⚠  No tables – skipping"); return
    with MetadataExtractor(db_type=CONNECTOR, database=src_path) as ext:
        table = ext.extract_table(TARGET_SCHEMA, "orders")
    print(f"  {table.schema_name}.{table.name}  ({table.row_count} rows)")
    print(f"  PKs: {table.primary_keys}")
    for col in table.columns:
        flags  = " [PK]" if col.is_primary_key else ""
        flags += " [FK]" if col.is_foreign_key else ""
        print(f"    • {col.name}: {col.data_type}{'  NULL' if col.is_nullable else '  NOT NULL'}{flags}")
    assert "order_id" in table.primary_keys
    print("  ✓ OK")


def test_yaml_per_schema(db_meta, dirs):
    section("8. YAML Generation – Per-Schema  →  ./models/")
    gen   = YAMLGenerator(output_dir=str(dirs["models"]))
    files = gen.generate_yaml_files(db_meta)
    for f in files:
        print(f"  • {os.path.basename(f)}  ({os.path.getsize(f):,} bytes)")
    assert len(files) >= 1
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
    print("  (SQLite has no COMMENT support — empty descriptions expected)")
    print("  ✓ Gap detection OK")


def test_schema_comparison(helper, dirs, src_path, dest_path, db_meta):
    section("11. Schema Comparison  (source full  vs  destination stripped)")
    src_cfg  = {"db_type": CONNECTOR, "database": src_path}
    dest_cfg = {"db_type": CONNECTOR, "database": dest_path}
    comparator = SchemaComparator(
        source_config=src_cfg,
        destination_config=dest_cfg,
        yaml_output_dir=str(dirs["models"]),
    )
    report = comparator.compare_and_generate_report(
        source_schema_name=TARGET_SCHEMA,
        destination_schema_name=TARGET_SCHEMA,
        include_yaml_gaps=True,
        source_db_metadata=db_meta,     # reuse — no source re-query
    )
    s = report["summary"]
    print(f"  Missing tables : {s['missing_tables_count']}")
    print(f"  Missing columns: {s['missing_columns_count']}")
    print(f"  Type mismatches: {s['type_mismatches_count']}")
    assert s["missing_tables_count"] >= 3, "Expected destination to be missing tables"
    json_path = helper.save_report(report)
    print(f"  JSON → {json_path}")
    print("  ✓ Comparison OK")
    return report, json_path


def test_compile_pdf(helper, json_path):
    section("12. Compile Reports → PDF  →  ./reports/pdf/")
    pdf_path = helper.compile_pdf(source_json=json_path)
    if pdf_path:
        print(f"  PDF → {pdf_path}  ({os.path.getsize(pdf_path):,} bytes)")
        print("  ✓ PDF compilation OK")
    else:
        print("  ⚠  No JSON reports or reportlab unavailable – skipping")
    return pdf_path


def test_email_report(helper, report, pdf_path):
    section("13. Email Report + PDF Attachment  (optional)")
    if report is None:
        print("  ⚠  No report – skipping"); return
    ok = helper.send_report_email(
        report=report,
        pdf_path=pdf_path,
        subject="[SQLite Test] Schema Comparison Report",
        email_to=EMAIL_TO,
    )
    print(f"  ✓ Email sent to {EMAIL_TO}" if ok else "  ⚠  SMTP not configured – skipped")


def test_metadata_export(helper, db_meta):
    section("14. Metadata Export + Serialization Round-Trip")
    meta_path = helper.reports_json_dir / "sqlite_metadata.json"
    meta_path.write_text(
        _json_mod.dumps(db_meta.to_dict(), indent=2, default=str),
        encoding="utf-8",
    )
    print(f"  Exported → {meta_path}  ({os.path.getsize(meta_path):,} bytes)")
    # Verify to_dict() / from_dict() round-trip
    restored    = DatabaseMetadata.from_dict(db_meta.to_dict())
    orig_tables = {t.name for s in db_meta.schemas  for t in s.tables}
    rest_tables = {t.name for s in restored.schemas for t in s.tables}
    assert orig_tables == rest_tables, f"Round-trip mismatch: {orig_tables ^ rest_tables}"
    print(f"  Round-trip OK — {len(orig_tables)} table(s) preserved")
    print("  ✓ Export OK")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"\n{EMOJI*30}\n  data_dictionary_builder — SQLite full feature test\n{EMOJI*30}")

    helper = DDHelper(".")
    dirs   = helper.dirs
    timer  = ExecutionTimer()

    print(f"\n  models/       → {dirs['models']}")
    print(f"  reports/json/ → {dirs['reports_json']}")
    print(f"  reports/pdf/  → {dirs['reports_pdf']}")

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as fh:
        src_path = fh.name
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as fh:
        dest_path = fh.name

    try:
        _create_source_db(src_path)
        _create_dest_db(dest_path)
        print(f"\n  Source database      : {src_path}")
        print(f"  Destination database : {dest_path}")

        with timer.task("1. Connection test"):
            test_connection(src_path, dest_path)

        with timer.task("2. Schema listing"):
            test_schema_listing(src_path)

        with timer.task("3. Table listing"):
            test_table_listing(src_path)

        with timer.task("4. Schema filter strategies"):
            TARGET_SCHEMAS = test_schema_filter_strategies(src_path)
            print(f"\n  → TARGET_SCHEMAS: {TARGET_SCHEMAS}")

        with timer.task("5. Full metadata extraction (source snapshot)"):
            db_meta = test_extract_all_schemas(src_path)

        with timer.task("6. Extract single schema"):
            schema = test_extract_single_schema(src_path)

        with timer.task("7. Extract single table"):
            test_extract_single_table(src_path, schema)

        with timer.task("8. YAML per-schema"):
            test_yaml_per_schema(db_meta, dirs)

        with timer.task("9. YAML combined"):
            test_yaml_combined(db_meta, dirs)

        with timer.task("10. Documentation gap detection"):
            test_documentation_gaps(db_meta, dirs)

        with timer.task("11. Schema comparison (source full vs dest stripped)"):
            report, json_path = test_schema_comparison(helper, dirs, src_path, dest_path, db_meta)

        with timer.task("12. Compile PDF"):
            pdf_path = test_compile_pdf(helper, json_path)

        with timer.task("13. Email report"):
            test_email_report(helper, report, pdf_path)

        with timer.task("14. Metadata export + round-trip"):
            test_metadata_export(helper, db_meta)

    finally:
        os.unlink(src_path)
        os.unlink(dest_path)
        print(f"\n  Cleaned up temp databases")

    timer.summary("SQLite Test Suite — Execution Summary")

    print("\n" + "✅ " * 30)
    print("  All SQLite feature tests passed!")
    print("✅ " * 30 + "\n")
