"""
Standalone usage examples for the DB Metadata Generator library.

This file demonstrates how to use the library outside of Airflow.
"""

import sys
sys.path.insert(0, '../src')

from data_dictionary_builder import (
    MetadataExtractor,
    YAMLGenerator,
    SchemaComparator,
    EmailSender
)


def example_1_extract_and_generate_yaml():
    """
    Example 1: Extract metadata from a PostgreSQL database and generate YAML files.
    """
    print("=" * 60)
    print("Example 1: Extract Metadata and Generate YAML Files")
    print("=" * 60)
    
    # Configure database connection
    db_config = {
        'db_type': 'postgres',
        'host': 'localhost',
        'port': 5432,
        'database': 'mydb',
        'user': 'postgres',
        'password': 'password'
    }
    
    # Extract metadata
    with MetadataExtractor(**db_config) as extractor:
        # Option 1: Extract all schemas
        db_metadata = extractor.extract_all_schemas()
        
        # Option 2: Extract specific schemas only
        # db_metadata = extractor.extract_all_schemas(schema_filter=['public', 'analytics'])
        
        print(f"Extracted {len(db_metadata.schemas)} schemas")
        for schema in db_metadata.schemas:
            print(f"  - {schema.name}: {len(schema.tables)} tables")
    
    # Generate YAML files
    yaml_generator = YAMLGenerator(output_dir='./output')
    generated_files = yaml_generator.generate_yaml_files(db_metadata)
    
    print(f"\nGenerated {len(generated_files)} YAML files:")
    for file_path in generated_files:
        print(f"  - {file_path}")


def example_2_sqlite_extraction():
    """
    Example 2: Extract metadata from a SQLite database.
    """
    print("\n" + "=" * 60)
    print("Example 2: SQLite Database Extraction")
    print("=" * 60)
    
    # Configure SQLite connection
    db_config = {
        'db_type': 'sqlite',
        'database': './example.db'  # Path to SQLite file
    }
    
    with MetadataExtractor(**db_config) as extractor:
        # Extract metadata
        db_metadata = extractor.extract_all_schemas()
        
        print(f"Database: {db_metadata.database_name}")
        print(f"Version: {db_metadata.version}")
        
        for schema in db_metadata.schemas:
            print(f"\nSchema: {schema.name}")
            for table in schema.tables:
                print(f"  Table: {table.name} ({len(table.columns)} columns, {table.row_count} rows)")


def example_3_mysql_extraction():
    """
    Example 3: Extract metadata from a MySQL database.
    """
    print("\n" + "=" * 60)
    print("Example 3: MySQL Database Extraction")
    print("=" * 60)
    
    # Configure MySQL connection
    db_config = {
        'db_type': 'mysql',
        'host': 'localhost',
        'port': 3306,
        'database': 'mydb',
        'user': 'root',
        'password': 'password'
    }
    
    with MetadataExtractor(**db_config) as extractor:
        # Test connection first
        if extractor.test_connection():
            print("✓ Connection successful")
        
        # Get list of schemas
        schemas = extractor.get_schemas_list()
        print(f"Available schemas: {', '.join(schemas)}")
        
        # Extract specific schema
        schema_metadata = extractor.extract_schema('mydb')
        print(f"\nExtracted schema: {schema_metadata.name}")
        print(f"Total tables: {len(schema_metadata.tables)}")


def example_4_clickhouse_extraction():
    """
    Example 4: Extract metadata from a ClickHouse database.
    """
    print("\n" + "=" * 60)
    print("Example 4: ClickHouse Database Extraction")
    print("=" * 60)
    
    # Configure ClickHouse connection
    db_config = {
        'db_type': 'clickhouse',
        'host': 'localhost',
        'port': 9000,  # Native protocol port
        'database': 'default',
        'user': 'default',
        'password': ''
    }
    
    with MetadataExtractor(**db_config) as extractor:
        db_metadata = extractor.extract_all_schemas()
        
        for schema in db_metadata.schemas:
            print(f"\nDatabase: {schema.name}")
            for table in schema.tables:
                print(f"  - {table.name} ({table.table_type})")
                for col in table.columns[:3]:  # Show first 3 columns
                    print(f"    • {col.name}: {col.data_type}")


def example_5_spanner_extraction():
    """
    Example 5: Extract metadata from a Google Cloud Spanner database.
    """
    print("\n" + "=" * 60)
    print("Example 5: Google Cloud Spanner Extraction")
    print("=" * 60)
    
    # Configure Spanner connection
    db_config = {
        'db_type': 'spanner',
        'instance_id': 'my-instance',
        'database_id': 'my-database',
        'project_id': 'my-gcp-project'  # Optional if using default credentials
    }
    
    with MetadataExtractor(**db_config) as extractor:
        db_metadata = extractor.extract_all_schemas()
        
        print(f"Database: {db_metadata.database_name}")
        for schema in db_metadata.schemas:
            print(f"\nSchema: {schema.name}")
            print(f"Tables: {len(schema.tables)}")


def example_6_schema_comparison():
    """
    Example 6: Compare schemas between source and destination databases.
    """
    print("\n" + "=" * 60)
    print("Example 6: Schema Comparison")
    print("=" * 60)
    
    # Source database configuration
    source_config = {
        'db_type': 'postgres',
        'host': 'source-db.example.com',
        'port': 5432,
        'database': 'source_db',
        'user': 'user',
        'password': 'password'
    }
    
    # Destination database configuration
    destination_config = {
        'db_type': 'postgres',
        'host': 'dest-db.example.com',
        'port': 5432,
        'database': 'dest_db',
        'user': 'user',
        'password': 'password'
    }
    
    # Create comparator
    comparator = SchemaComparator(
        source_config=source_config,
        destination_config=destination_config,
        yaml_output_dir='./output'
    )
    
    # Compare specific schema
    report = comparator.compare_and_generate_report(
        source_schema_name='public',
        include_yaml_gaps=True
    )
    
    # Print summary
    print("\nComparison Summary:")
    print(f"  Missing Tables: {report['summary']['missing_tables_count']}")
    print(f"  Missing Columns: {report['summary']['missing_columns_count']}")
    print(f"  Type Mismatches: {report['summary']['type_mismatches_count']}")
    print(f"  Tables Without Descriptions: {report['summary'].get('tables_without_descriptions_count', 0)}")
    print(f"  Columns Without Descriptions: {report['summary'].get('columns_without_descriptions_count', 0)}")
    
    # Show missing tables
    if report['comparison']['missing_tables']:
        print("\nMissing Tables:")
        for table in report['comparison']['missing_tables'][:5]:
            print(f"  - {table['schema']}.{table['table']}")


def example_7_send_email_report():
    """
    Example 7: Compare schemas and send email report.
    """
    print("\n" + "=" * 60)
    print("Example 7: Compare and Send Email Report")
    print("=" * 60)
    
    # Database configurations
    source_config = {
        'db_type': 'postgres',
        'host': 'localhost',
        'port': 5432,
        'database': 'source_db',
        'user': 'user',
        'password': 'password'
    }
    
    destination_config = {
        'db_type': 'postgres',
        'host': 'localhost',
        'port': 5432,
        'database': 'dest_db',
        'user': 'user',
        'password': 'password'
    }
    
    # Compare schemas
    comparator = SchemaComparator(
        source_config=source_config,
        destination_config=destination_config
    )
    
    report = comparator.compare_and_generate_report(
        source_schema_name='public',
        include_yaml_gaps=True
    )
    
    # Send email
    email_sender = EmailSender(
        smtp_host='smtp.gmail.com',
        smtp_port=587,
        sender_email='your-email@gmail.com',
        sender_password='your-app-password',
        use_tls=True
    )
    
    success = email_sender.send_comparison_report(
        recipient_emails=['team@example.com'],
        report=report,
        subject='Database Schema Comparison Report'
    )
    
    if success:
        print("✓ Email sent successfully!")
    else:
        print("✗ Failed to send email")


def example_8_single_table_extraction():
    """
    Example 8: Extract metadata for a single table.
    """
    print("\n" + "=" * 60)
    print("Example 8: Single Table Extraction")
    print("=" * 60)
    
    db_config = {
        'db_type': 'postgres',
        'host': 'localhost',
        'port': 5432,
        'database': 'mydb',
        'user': 'postgres',
        'password': 'password'
    }
    
    with MetadataExtractor(**db_config) as extractor:
        # Extract single table
        table_metadata = extractor.extract_table('public', 'users')
        
        print(f"Table: {table_metadata.name}")
        print(f"Schema: {table_metadata.schema_name}")
        print(f"Row Count: {table_metadata.row_count}")
        print(f"Primary Keys: {', '.join(table_metadata.primary_keys)}")
        
        print("\nColumns:")
        for col in table_metadata.columns:
            pk_marker = " (PK)" if col.is_primary_key else ""
            fk_marker = f" (FK -> {col.foreign_key_table}.{col.foreign_key_column})" if col.is_foreign_key else ""
            print(f"  - {col.name}: {col.data_type}{pk_marker}{fk_marker}")


def example_9_generate_single_yaml():
    """
    Example 9: Generate a single combined YAML file instead of per-schema files.
    """
    print("\n" + "=" * 60)
    print("Example 9: Generate Single Combined YAML File")
    print("=" * 60)
    
    db_config = {
        'db_type': 'postgres',
        'host': 'localhost',
        'port': 5432,
        'database': 'mydb',
        'user': 'postgres',
        'password': 'password'
    }
    
    with MetadataExtractor(**db_config) as extractor:
        db_metadata = extractor.extract_all_schemas(schema_filter=['public'])
    
    # Generate single YAML file
    yaml_generator = YAMLGenerator(output_dir='./output')
    filepath = yaml_generator.generate_single_yaml(db_metadata, filename='all_models.yml')
    
    print(f"Generated combined YAML file: {filepath}")


if __name__ == '__main__':
    """
    Run examples individually or all at once.
    
    To run a specific example:
        python standalone_usage.py
    
    Then uncomment the example you want to run below.
    """
    
    print("DB Metadata Generator - Standalone Usage Examples\n")
    
    # Uncomment the example you want to run:
    
    # example_1_extract_and_generate_yaml()
    # example_2_sqlite_extraction()
    # example_3_mysql_extraction()
    # example_4_clickhouse_extraction()
    # example_5_spanner_extraction()
    # example_6_schema_comparison()
    # example_7_send_email_report()
    # example_8_single_table_extraction()
    # example_9_generate_single_yaml()
    
    print("\nNote: Uncomment the example you want to run in the main section.")
    print("Make sure to update the database connection details before running.")
