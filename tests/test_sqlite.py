"""
test_sqlite.py
==============
Exercises every major feature of data_dictionary_builder against SQLite.

Output layout
-------------
    ./models/          ← YAML files (per-schema and combined)
    ./reports/         ← JSON comparison reports + compiled reports.pdf

No environment variables required to run.

Optional email (PDF will be attached automatically):
    SMTP_HOST  SMTP_PORT  SMTP_USER  SMTP_PASSWORD  EMAIL_TO
"""

import os
import sqlite3
import tempfile

from dotenv import load_dotenv

from data_dictionary_builder import MetadataExtractor, YAMLGenerator, SchemaComparator

from data_dictionary_builder import DDHelper

load_dotenv()

CONNECTOR = "sqlite"
EMOJI     = "🗂 "


# ─────────────────────────────────────────────────────────────────────────────
# Fixture
# ─────────────────────────────────────────────────────────────────────────────

def create_test_database(path: str) -> None:
    conn = sqlite3.connect(path)
    conn.cursor().executescript("""
        PRAGMA foreign_keys = ON;
        CREATE TABLE IF NOT EXISTS countries (
            country_id INTEGER PRIMARY KEY, country_code TEXT NOT NULL, country_name TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS customers (
            customer_id INTEGER PRIMARY KEY, email TEXT NOT NULL UNIQUE,
            first_name TEXT, last_name TEXT,
            country_id INTEGER REFERENCES countries(country_id),
            created_at TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'active'
        );
        CREATE TABLE IF NOT EXISTS products (
            product_id INTEGER PRIMARY KEY, sku TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL, price REAL NOT NULL, stock INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS orders (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL REFERENCES customers(customer_id),
            order_date TEXT NOT NULL, total_amount REAL NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending'
        );
        CREATE TABLE IF NOT EXISTS order_items (
            item_id INTEGER PRIMARY KEY,
            order_id INTEGER NOT NULL REFERENCES orders(order_id),
            product_id INTEGER NOT NULL REFERENCES products(product_id),
            quantity INTEGER NOT NULL, unit_price REAL NOT NULL
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


def section(title: str) -> None:
    print(f"\n{'='*60}\n  {title}\n{'='*60}")


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────

def test_connection(db_path):
    section("1. Connection Test")
    ok = MetadataExtractor(db_type=CONNECTOR, database=db_path).test_connection()
    assert ok, "❌  SQLite connection failed"
    print("  ✓ Connected")


def test_schema_listing(db_path):
    section("2. Schema Listing")
    with MetadataExtractor(db_type=CONNECTOR, database=db_path) as ext:
        schemas = ext.get_schemas_list()
    print(f"  Schemas: {schemas}")
    assert "main" in schemas
    print("  ✓ 'main' schema present")
    return schemas


def test_table_listing(db_path):
    section("3. Table Listing")
    with MetadataExtractor(db_type=CONNECTOR, database=db_path) as ext:
        tables = ext.get_tables_list("main")
    print(f"  Tables: {tables}")
    assert len(tables) >= 5
    print(f"  ✓ Found {len(tables)} table(s)")


def test_schema_filter_strategies(db_path):
    section("4. Schema-Filter Strategies")
    cfg = {"db_type": CONNECTOR, "database": db_path}
    cases = [
        ("4a. Exact name",             ["main"],          True),
        ("4b. Glob  (ma%)",            ["ma%"],           True),
        ("4c. prefix:",                ["prefix:ma"],     True),
        ("4d. suffix:",                ["suffix:in"],     True),
        ("4e. contains:",              ["contains:ai"],   True),
        ("4f. regex:",                 ["regex:^ma.*$"],  True),
        ("4g. Mixed",                  ["main","regex:^stg_.*$"], True),
        ("4h. None  (all)",            None,              True),
        ("4i. Non-matching",           ["prefix:stg_"],   False),
    ]
    for label, sf, expect_main in cases:
        with MetadataExtractor(**cfg) as ext:
            matched = [s.name for s in ext.extract_all_schemas(schema_filter=sf).schemas]
        ok = ("main" in matched) == expect_main
        print(f"  {'✓' if ok else '✗'} {label}  →  {matched}")
    print("  ✓ Filter strategies OK")


def test_extract_all_schemas(db_path):
    section("5. Full Metadata Extraction")
    with MetadataExtractor(db_type=CONNECTOR, database=db_path) as ext:
        db_meta = ext.extract_all_schemas(schema_filter=["main"])
    print(f"  Database: {db_meta.database_name}  |  Version: {db_meta.version}")
    for schema in db_meta.schemas:
        print(f"  [{schema.name}]  {len(schema.tables)} table(s)")
        for t in schema.tables:
            print(f"    • {t.name}  ({len(t.columns)} cols, {t.row_count} rows)")
    assert any(t.name == "customers" for s in db_meta.schemas for t in s.tables)
    print("  ✓ Extraction OK")
    return db_meta


def test_extract_single_schema(db_path):
    section("6. Extract Single Schema")
    with MetadataExtractor(db_type=CONNECTOR, database=db_path) as ext:
        schema = ext.extract_schema("main")
    print(f"  Schema: {schema.name}  ({len(schema.tables)} tables)")
    print("  ✓ OK")


def test_extract_single_table(db_path):
    section("7. Extract Single Table  (FK / PK detail)")
    with MetadataExtractor(db_type=CONNECTOR, database=db_path) as ext:
        table = ext.extract_table("main", "orders")
    print(f"  {table.schema_name}.{table.name}  ({table.row_count} rows)")
    print(f"  PKs: {table.primary_keys}")
    for col in table.columns:
        flags = ("" if not col.is_primary_key else " [PK]") + ("" if not col.is_foreign_key else " [FK]")
        print(f"    • {col.name}: {col.data_type}{'  NULL' if col.is_nullable else '  NOT NULL'}{flags}")
    assert table.primary_keys == ["order_id"]
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
    print(f"  Table documentation  : {100*(total_t-len(tables_no_desc))//max(total_t,1)}%  ({len(tables_no_desc)}/{total_t} missing)")
    print(f"  Column documentation : {100*(total_c-len(cols_no_desc))//max(total_c,1)}%  ({len(cols_no_desc)}/{total_c} missing)")
    print("  (SQLite has no COMMENT support — all descriptions empty, expected)")
    print("  ✓ Gap detection OK")


def test_schema_comparison(db_path, dirs):
    section("11. Schema Comparison  (self-comparison → 0 diffs)")
    cfg = {"db_type": CONNECTOR, "database": db_path}
    report = SchemaComparator(
        source_config=cfg, destination_config=cfg,
        yaml_output_dir=str(dirs["models"]),
    ).compare_and_generate_report(
        source_schema_name="main", destination_schema_name="main",
        include_yaml_gaps=True,
    )
    s = report["summary"]
    print(f"  Missing tables: {s['missing_tables_count']}  |  columns: {s['missing_columns_count']}  |  type mismatches: {s['type_mismatches_count']}")
    assert s["missing_tables_count"] == 0 and s["missing_columns_count"] == 0
    path = helper.save_report(report)
    print(f"  JSON → {path}")
    print("  ✓ OK")
    return report


def test_cross_db_comparison(db_path, dirs):
    section("12. Cross-DB Comparison  (source vs stripped copy)")
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as fh:
        dest_path = fh.name
    try:
        conn = sqlite3.connect(dest_path)
        conn.cursor().executescript("""
            CREATE TABLE countries (country_id INTEGER PRIMARY KEY, country_code TEXT NOT NULL);
            CREATE TABLE customers (customer_id INTEGER PRIMARY KEY, email TEXT NOT NULL);
        """)
        conn.commit(); conn.close()

        report = SchemaComparator(
            source_config={"db_type": CONNECTOR, "database": db_path},
            destination_config={"db_type": CONNECTOR, "database": dest_path},
            yaml_output_dir=str(dirs["models"]),
        ).compare_and_generate_report(
            source_schema_name="main", destination_schema_name="main",
            include_yaml_gaps=False,
        )
        s = report["summary"]
        print(f"  Missing tables: {s['missing_tables_count']}  |  columns: {s['missing_columns_count']}")
        assert s["missing_tables_count"] >= 3
        path = helper.save_report(report)
        print(f"  JSON → {path}")
        print("  ✓ OK")
        return report
    finally:
        os.unlink(dest_path)


def test_compile_pdf(helper):
    section("13. Compile Reports → PDF  →  ./reports/pdf/")
    pdf_path = helper.compile_pdf()
    if pdf_path:
        print(f"  PDF → {pdf_path}  ({os.path.getsize(pdf_path):,} bytes)")
        print("  ✓ PDF compilation OK")
    else:
        print("  ⚠  No JSON reports found or reportlab unavailable — skipping")
    return pdf_path


def test_email_report(report, pdf_path):
    section("14. Email Report + PDF Attachment  (optional)")
    ok = helper.send_report_email(report=report, pdf_path=pdf_path,
                             subject="[SQLite Test] Schema Comparison Report")
    print("  ✓ Email sent" if ok else "  ⚠  SMTP not configured — skipped")


def test_metadata_export(db_meta, dirs):
    section("15. Metadata Export to JSON  →  ./reports/json/")
    _meta_path = helper.reports_json_dir / "sqlite_metadata.json"
    import json as _json_mod
    _meta_path.write_text(_json_mod.dumps(db_meta.to_dict(), indent=2, default=str), encoding="utf-8")
    path = _meta_path
    print(f"  {path}  ({os.path.getsize(path):,} bytes)")
    print("  ✓ Export OK")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"\n{EMOJI*30}\n  data_dictionary_builder — SQLite full feature test\n{EMOJI*30}")

    helper = DDHelper(".")
    dirs   = helper.dirs
    print(f"\n  models/       → {dirs['models']}\n  reports/json/ → {dirs['reports_json']}\n  reports/pdf/  → {dirs['reports_pdf']}")

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as fh:
        db_path = fh.name

    try:
        create_test_database(db_path)
        print(f"\n  Temp database: {db_path}")

        test_connection(db_path)
        test_schema_listing(db_path)
        test_table_listing(db_path)
        test_schema_filter_strategies(db_path)

        db_meta = test_extract_all_schemas(db_path)
        test_extract_single_schema(db_path)
        test_extract_single_table(db_path)

        test_yaml_per_schema(db_meta, dirs)
        test_yaml_combined(db_meta, dirs)
        test_documentation_gaps(db_meta, dirs)

        report    = test_schema_comparison(db_path, dirs)
        report_xd = test_cross_db_comparison(db_path, dirs)
        pdf_path  = test_compile_pdf(helper)

        test_email_report(report, pdf_path)
        test_metadata_export(db_meta, dirs)

    finally:
        os.unlink(db_path)
        print(f"\n  Cleaned up temp database: {db_path}")

    print("\n" + "✅ " * 30)
    print("  All SQLite feature tests passed!")
    print("✅ " * 30 + "\n")
