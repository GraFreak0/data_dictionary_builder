from data_dictionary_builder import MetadataExtractor, YAMLGenerator
from dotenv import load_dotenv
import os

load_dotenv()

# Step 1: Configure your database connection
databases = {
    'clickhouse_default': {
        'db_type': 'clickhouse',
        'host': os.getenv('clickhouse_host'),
        'port': int(os.getenv('clickhouse_port', 9440)),
        # 'database': os.getenv('clickhouse_db'),
        'user': os.getenv('clickhouse_user'),
        'password': os.getenv('clickhouse_password'),
        'secure': True,
        'verify': False,
    }
}

# Step 2: Extract metadata
for db_name, config in databases.items():
    print("Extracting metadata...")
    with MetadataExtractor(**config) as extractor:
        # Extract all schemas (or use schema_filter=['public'] for specific ones)
        db_metadata = extractor.extract_all_schemas(
            schema_filter=['default', 'system']
        )
        
        print(f"✓ Extracted {len(db_metadata.schemas)} schemas")
        for schema in db_metadata.schemas:
            print(f"  - {schema.name}: {len(schema.tables)} tables")

    # Step 3: Generate YAML files for dbt
    print("\nGenerating YAML files...")
    yaml_gen = YAMLGenerator(output_dir='./dbt_models')
    files = yaml_gen.generate_yaml_files(db_metadata)

    print(f"✓ Generated {len(files)} YAML files:")
    for file_path in files:
        print(f"  - {file_path} \n\n")

print("\n✨ Done! Your YAML files are ready for dbt.")