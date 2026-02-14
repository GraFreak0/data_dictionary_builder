"""
Example Airflow DAG for database metadata extraction and comparison.

This DAG demonstrates how to:
1. Extract metadata from a source database
2. Generate YAML files for dbt
3. Compare source and destination schemas
4. Send email reports with comparison results
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Import the library modules
import sys
sys.path.insert(0, '/path/to/db_metadata_generator/src')

from db_metadata_generator import (
    MetadataExtractor,
    YAMLGenerator,
    SchemaComparator,
    EmailSender
)


# Configuration
SOURCE_DB_CONFIG = {
    'db_type': 'postgres',
    'host': 'source-db.example.com',
    'port': 5432,
    'database': 'source_db',
    'user': 'db_user',
    'password': 'db_password'
}

DESTINATION_DB_CONFIG = {
    'db_type': 'postgres',
    'host': 'dest-db.example.com',
    'port': 5432,
    'database': 'dest_db',
    'user': 'db_user',
    'password': 'db_password'
}

EMAIL_CONFIG = {
    'smtp_host': 'smtp.gmail.com',
    'smtp_port': 587,
    'sender_email': 'your-email@example.com',
    'sender_password': 'your-app-password',
    'use_tls': True
}

RECIPIENT_EMAILS = ['team@example.com', 'data-team@example.com']

YAML_OUTPUT_DIR = '/opt/airflow/dbt/models'
SCHEMAS_TO_EXTRACT = ['public', 'analytics', 'staging']


def extract_metadata(**context):
    """Task to extract metadata from source database."""
    print("Extracting metadata from source database...")
    
    with MetadataExtractor(**SOURCE_DB_CONFIG) as extractor:
        # Extract metadata for specified schemas
        db_metadata = extractor.extract_all_schemas(schema_filter=SCHEMAS_TO_EXTRACT)
        
        # Store metadata in XCom for next tasks
        context['task_instance'].xcom_push(
            key='db_metadata',
            value=db_metadata.to_dict()
        )
        
        print(f"Extracted {len(db_metadata.schemas)} schemas with "
              f"{sum(len(s.tables) for s in db_metadata.schemas)} tables")
    
    return "Metadata extraction completed"


def generate_yaml_files(**context):
    """Task to generate YAML files for dbt."""
    print("Generating YAML files...")
    
    # Retrieve metadata from previous task
    db_metadata_dict = context['task_instance'].xcom_pull(
        task_ids='extract_metadata',
        key='db_metadata'
    )
    
    # Note: In production, you'd reconstruct the DatabaseMetadata object from the dict
    # For simplicity, we'll re-extract here
    with MetadataExtractor(**SOURCE_DB_CONFIG) as extractor:
        db_metadata = extractor.extract_all_schemas(schema_filter=SCHEMAS_TO_EXTRACT)
    
    # Generate YAML files
    yaml_generator = YAMLGenerator(output_dir=YAML_OUTPUT_DIR)
    generated_files = yaml_generator.generate_yaml_files(db_metadata)
    
    print(f"Generated {len(generated_files)} YAML files:")
    for file_path in generated_files:
        print(f"  - {file_path}")
    
    return "YAML generation completed"


def compare_schemas(**context):
    """Task to compare source and destination schemas."""
    print("Comparing schemas between source and destination...")
    
    comparator = SchemaComparator(
        source_config=SOURCE_DB_CONFIG,
        destination_config=DESTINATION_DB_CONFIG,
        yaml_output_dir=YAML_OUTPUT_DIR
    )
    
    # Compare each schema
    all_reports = {}
    for schema_name in SCHEMAS_TO_EXTRACT:
        print(f"Comparing schema: {schema_name}")
        report = comparator.compare_and_generate_report(
            source_schema_name=schema_name,
            include_yaml_gaps=True
        )
        all_reports[schema_name] = report
    
    # Store reports in XCom for email task
    context['task_instance'].xcom_push(
        key='comparison_reports',
        value=all_reports
    )
    
    # Calculate total differences
    total_missing_tables = sum(r['summary']['missing_tables_count'] for r in all_reports.values())
    total_missing_columns = sum(r['summary']['missing_columns_count'] for r in all_reports.values())
    
    print(f"Comparison completed. Total missing tables: {total_missing_tables}, "
          f"Total missing columns: {total_missing_columns}")
    
    return "Schema comparison completed"


def send_comparison_email(**context):
    """Task to send email with comparison results."""
    print("Sending comparison report email...")
    
    # Retrieve comparison reports from previous task
    all_reports = context['task_instance'].xcom_pull(
        task_ids='compare_schemas',
        key='comparison_reports'
    )
    
    # Create combined report
    combined_report = {
        'summary': {
            'missing_tables_count': sum(r['summary']['missing_tables_count'] for r in all_reports.values()),
            'missing_columns_count': sum(r['summary']['missing_columns_count'] for r in all_reports.values()),
            'type_mismatches_count': sum(r['summary']['type_mismatches_count'] for r in all_reports.values()),
            'tables_without_descriptions_count': sum(r['summary'].get('tables_without_descriptions_count', 0) for r in all_reports.values()),
            'columns_without_descriptions_count': sum(r['summary'].get('columns_without_descriptions_count', 0) for r in all_reports.values()),
        },
        'comparison': {
            'missing_tables': [],
            'missing_columns': [],
            'type_mismatches': []
        },
        'yaml_gaps': {
            'tables_without_descriptions': [],
            'columns_without_descriptions': []
        }
    }
    
    # Combine all schema reports
    for schema_name, report in all_reports.items():
        combined_report['comparison']['missing_tables'].extend(report['comparison']['missing_tables'])
        combined_report['comparison']['missing_columns'].extend(report['comparison']['missing_columns'])
        combined_report['comparison']['type_mismatches'].extend(report['comparison']['type_mismatches'])
        
        if 'yaml_gaps' in report:
            combined_report['yaml_gaps']['tables_without_descriptions'].extend(report['yaml_gaps']['tables_without_descriptions'])
            combined_report['yaml_gaps']['columns_without_descriptions'].extend(report['yaml_gaps']['columns_without_descriptions'])
    
    # Send email
    email_sender = EmailSender(**EMAIL_CONFIG)
    success = email_sender.send_comparison_report(
        recipient_emails=RECIPIENT_EMAILS,
        report=combined_report,
        subject=f"Database Schema Comparison Report - {datetime.now().strftime('%Y-%m-%d')}"
    )
    
    if success:
        print("Email sent successfully!")
        return "Email sent"
    else:
        print("Failed to send email")
        raise Exception("Email sending failed")


# Define the DAG
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email': RECIPIENT_EMAILS,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'db_metadata_extraction_and_comparison',
    default_args=default_args,
    description='Extract database metadata, generate YAML files, and compare schemas',
    schedule_interval='0 2 * * *',  # Run daily at 2 AM
    start_date=days_ago(1),
    catchup=False,
    tags=['database', 'metadata', 'dbt'],
)

# Define tasks
task_extract = PythonOperator(
    task_id='extract_metadata',
    python_callable=extract_metadata,
    dag=dag,
)

task_generate_yaml = PythonOperator(
    task_id='generate_yaml_files',
    python_callable=generate_yaml_files,
    dag=dag,
)

task_compare = PythonOperator(
    task_id='compare_schemas',
    python_callable=compare_schemas,
    dag=dag,
)

task_send_email = PythonOperator(
    task_id='send_comparison_email',
    python_callable=send_comparison_email,
    dag=dag,
)

# Define task dependencies
task_extract >> task_generate_yaml >> task_compare >> task_send_email
