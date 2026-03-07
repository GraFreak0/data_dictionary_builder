"""
Server-Level Metadata Extraction Examples

Extract metadata from ALL databases on a server when database parameter is omitted.
"""

from data_dictionary_builder import MetadataExtractor, YAMLGenerator
from dotenv import load_dotenv
import os

load_dotenv()

print("=" * 70)
print("Server-Level Metadata Extraction Examples")
print("=" * 70)

# =============================================================================
# Example 1: Extract from ALL PostgreSQL databases on a server
# =============================================================================

print("\n" + "=" * 70)
print("Example 1: PostgreSQL - Extract ALL databases on server")
print("=" * 70)

# Notice: 'database' parameter is omitted!
clickhouse_config = {
    'db_type': 'clickhouse',
    'host': os.getenv('clickhouse_host'),
    'port': int(os.getenv('clickhouse_port', 9440)),
    # 'database': os.getenv('clickhouse_db'),
    'user': os.getenv('clickhouse_user'),
    'password': os.getenv('clickhouse_password'),
    'secure': True,
    'verify': False,
}

print("\nExtracting from ALL databases on PostgreSQL server...")
print("This will find: production, analytics, staging, etc.")

with MetadataExtractor(**postgres_config) as extractor:
    # This extracts from ALL databases on the server!
    db_metadata = extractor.extract_all_schemas()
    
    print(f"\n✓ Found {len(db_metadata.schemas)} databases on server:")
    for schema in db_metadata.schemas:
        print(f"  - {schema.name}: {len(schema.tables)} tables")

# Generate YAML files
yaml_gen = YAMLGenerator(output_dir='./dbt_models/all_postgres_dbs')
files = yaml_gen.generate_yaml_files(db_metadata)

print(f"\n✓ Generated {len(files)} YAML files (one per database)")
print("\nOutput structure:")
print("  ./dbt_models/all_postgres_dbs/")
print("  ├── schema_production.yml")
print("  ├── schema_analytics.yml")
print("  └── schema_staging.yml")

# =============================================================================
# Example 2: Extract from ALL MySQL databases on a server
# =============================================================================

print("\n" + "=" * 70)
print("Example 2: MySQL - Extract ALL databases on server")
print("=" * 70)

mysql_config = {
    'db_type': 'mysql',
    'host': 'localhost',
    'port': 3306,
    # 'database': 'app_db',  # ← OMITTED!
    'user': 'root',
    'password': 'password'
}

print("\nExtracting from ALL databases on MySQL server...")

with MetadataExtractor(**mysql_config) as extractor:
    db_metadata = extractor.extract_all_schemas()
    
    print(f"\n✓ Found {len(db_metadata.schemas)} databases on server:")
    for schema in db_metadata.schemas:
        print(f"  - {schema.name}: {len(schema.tables)} tables")

yaml_gen = YAMLGenerator(output_dir='./dbt_models/all_mysql_dbs')
files = yaml_gen.generate_yaml_files(db_metadata)

print(f"\n✓ Generated {len(files)} YAML files")

# =============================================================================
# Example 3: Filter specific databases from server
# =============================================================================

print("\n" + "=" * 70)
print("Example 3: Extract only SPECIFIC databases from server")
print("=" * 70)

postgres_config = {
    'db_type': 'postgres',
    'host': 'localhost',
    'port': 5432,
    # 'database' omitted - server mode
    'user': 'postgres',
    'password': 'password'
}

print("\nExtracting only 'production' and 'analytics' from server...")

with MetadataExtractor(**postgres_config) as extractor:
    # Use schema_filter to specify which databases to extract
    db_metadata = extractor.extract_all_schemas(
        schema_filter=['production', 'analytics']
    )
    
    print(f"\n✓ Extracted {len(db_metadata.schemas)} databases:")
    for schema in db_metadata.schemas:
        print(f"  - {schema.name}: {len(schema.tables)} tables")

yaml_gen = YAMLGenerator(output_dir='./dbt_models/filtered_postgres')
files = yaml_gen.generate_yaml_files(db_metadata)

print(f"\n✓ Generated {len(files)} YAML files")

# =============================================================================
# Example 4: Compare server-mode vs single-database mode
# =============================================================================

print("\n" + "=" * 70)
print("Example 4: Comparison - Server Mode vs Single Database Mode")
print("=" * 70)

# Mode 1: Single Database (traditional)
print("\nMode 1: Single Database Mode")
print("-" * 70)

single_db_config = {
    'db_type': 'postgres',
    'host': 'localhost',
    'port': 5432,
    'database': 'production',  # ← Specified
    'user': 'postgres',
    'password': 'password'
}

with MetadataExtractor(**single_db_config) as extractor:
    db_metadata = extractor.extract_all_schemas()
    print(f"✓ Single database 'production'")
    print(f"  Schemas found: {[s.name for s in db_metadata.schemas]}")
    print(f"  (These are schemas WITHIN the 'production' database)")

# Mode 2: Server Mode (new!)
print("\nMode 2: Server Mode")
print("-" * 70)

server_config = {
    'db_type': 'postgres',
    'host': 'localhost',
    'port': 5432,
    # 'database': omitted!  # ← NOT Specified
    'user': 'postgres',
    'password': 'password'
}

with MetadataExtractor(**server_config) as extractor:
    db_metadata = extractor.extract_all_schemas()
    print(f"✓ Server mode - ALL databases")
    print(f"  Databases found: {[s.name for s in db_metadata.schemas]}")
    print(f"  (These are DATABASES on the server)")

# =============================================================================
# Example 5: Multi-server extraction (combine with multi-database)
# =============================================================================

print("\n" + "=" * 70)
print("Example 5: Extract from MULTIPLE servers")
print("=" * 70)

servers = {
    'postgres_prod_server': {
        'db_type': 'postgres',
        'host': 'prod-server.example.com',
        'port': 5432,
        # No database - extract all!
        'user': 'readonly',
        'password': 'password1'
    },
    'mysql_app_server': {
        'db_type': 'mysql',
        'host': 'mysql-server.example.com',
        'port': 3306,
        # No database - extract all!
        'user': 'readonly',
        'password': 'password2'
    },
    'clickhouse_server': {
        'db_type': 'clickhouse',
        'host': 'clickhouse.example.com',
        'port': 9000,
        'database': None,  # Explicitly None - extract all
        'user': 'default',
        'password': 'password3'
    }
}

for server_name, config in servers.items():
    print(f"\nProcessing server: {server_name}")
    
    with MetadataExtractor(**config) as extractor:
        db_metadata = extractor.extract_all_schemas()
        print(f"  ✓ Found {len(db_metadata.schemas)} databases")
    
    yaml_gen = YAMLGenerator(output_dir=f'./dbt_models/{server_name}')
    files = yaml_gen.generate_yaml_files(db_metadata)
    print(f"  ✓ Generated {len(files)} YAML files")

print("\n" + "=" * 70)
print("Summary")
print("=" * 70)
print("""
Server Mode Benefits:
✅ Extract from ALL databases on a server with one config
✅ No need to know database names in advance
✅ Automatic discovery of all databases
✅ Filter specific databases with schema_filter parameter
✅ Perfect for servers with many databases

Usage:
1. Omit 'database' parameter (or set to None)
2. Run extractor - it finds all databases automatically
3. Optionally filter: extract_all_schemas(schema_filter=['db1', 'db2'])
4. Generate YAML - one file per database

Traditional Mode (still works!):
✅ Specify 'database' parameter for single database
✅ Extracts schemas WITHIN that database
✅ Use when you only need one specific database
""")

print("\n✨ Done! Check ./dbt_models/ for generated files.")