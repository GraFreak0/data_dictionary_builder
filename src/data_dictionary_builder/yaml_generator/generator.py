"""
YAML generator for creating dbt-compatible YAML files from database metadata.
"""

import os
import yaml
import logging
from typing import Dict, Any, List, Optional
from ..metadata.models import DatabaseMetadata, SchemaMetadata, TableMetadata, ColumnMetadata

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class YAMLGenerator:
    """Generator for creating dbt-compatible YAML files from metadata."""
    
    def __init__(self, output_dir: str = "./output"):
        """
        Initialize the YAML generator.
        
        Args:
            output_dir: Directory where YAML files will be saved
        """
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
    
    def _column_to_yaml_dict(self, column: ColumnMetadata) -> Dict[str, Any]:
        """
        Convert column metadata to YAML-compatible dictionary.
        
        Args:
            column: ColumnMetadata object
            
        Returns:
            Dictionary representation for YAML
        """
        col_dict = {
            'name': column.name,
            'data_type': column.data_type,
        }
        
        # Add description if available
        if column.description:
            col_dict['description'] = column.description
        
        # Add metadata
        meta = {}
        if column.is_primary_key:
            meta['is_primary_key'] = True
        if column.is_foreign_key:
            meta['is_foreign_key'] = True
            meta['foreign_key_table'] = column.foreign_key_table
            meta['foreign_key_column'] = column.foreign_key_column
        if not column.is_nullable:
            meta['is_nullable'] = False
        
        if meta:
            col_dict['meta'] = meta
        
        # Add tests for primary keys and not null columns
        tests = []
        if column.is_primary_key:
            tests.append('unique')
            tests.append('not_null')
        elif not column.is_nullable:
            tests.append('not_null')
        
        if tests:
            col_dict['tests'] = tests
        
        return col_dict
    
    def _table_to_yaml_dict(self, table: TableMetadata) -> Dict[str, Any]:
        """
        Convert table metadata to YAML-compatible dictionary.
        
        Args:
            table: TableMetadata object
            
        Returns:
            Dictionary representation for YAML
        """
        table_dict = {
            'name': table.name,
        }
        
        # Add description if available
        if table.description:
            table_dict['description'] = table.description
        
        # Add metadata
        meta = {
            'schema': table.schema_name,
            'table_type': table.table_type,
        }
        if table.row_count is not None:
            meta['row_count'] = table.row_count
        
        table_dict['meta'] = meta
        
        # Add columns
        columns = [self._column_to_yaml_dict(col) for col in table.columns]
        if columns:
            table_dict['columns'] = columns
        
        return table_dict
    
    def _schema_to_yaml_dict(self, schema: SchemaMetadata) -> Dict[str, Any]:
        """
        Convert schema metadata to YAML-compatible dictionary.
        
        Args:
            schema: SchemaMetadata object
            
        Returns:
            Dictionary representation for YAML
        """
        schema_dict = {
            'version': 2,
            'models': []
        }
        
        # Add each table as a model
        for table in schema.tables:
            table_dict = self._table_to_yaml_dict(table)
            schema_dict['models'].append(table_dict)
        
        return schema_dict
    
    def generate_schema_yaml(self, schema: SchemaMetadata, filename: Optional[str] = None) -> str:
        """
        Generate a YAML file for a single schema.
        
        Args:
            schema: SchemaMetadata object
            filename: Optional custom filename (default: schema_<schema_name>.yml)
            
        Returns:
            Path to the generated YAML file
        """
        if filename is None:
            # Sanitize schema name for filename
            safe_name = schema.name.replace(' ', '_').replace('/', '_').replace('\\', '_')
            filename = f"schema_{safe_name}.yml"
        
        filepath = os.path.join(self.output_dir, filename)
        
        # Convert schema to YAML dictionary
        yaml_dict = self._schema_to_yaml_dict(schema)
        
        # Write to file
        with open(filepath, 'w', encoding='utf-8') as f:
            yaml.dump(yaml_dict, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
        
        logger.info(f"Generated YAML file: {filepath}")
        return filepath
    
    def generate_yaml_files(self, db_metadata: DatabaseMetadata) -> List[str]:
        """
        Generate YAML files for all schemas in the database.
        
        Args:
            db_metadata: DatabaseMetadata object
            
        Returns:
            List of paths to generated YAML files
        """
        generated_files = []
        
        for schema in db_metadata.schemas:
            try:
                filepath = self.generate_schema_yaml(schema)
                generated_files.append(filepath)
            except Exception as e:
                logger.error(f"Error generating YAML for schema {schema.name}: {e}")
                continue
        
        logger.info(f"Generated {len(generated_files)} YAML files in {self.output_dir}")
        return generated_files
    
    def generate_single_yaml(self, db_metadata: DatabaseMetadata, filename: str = "models.yml") -> str:
        """
        Generate a single YAML file containing all schemas.
        
        Args:
            db_metadata: DatabaseMetadata object
            filename: Name of the output file
            
        Returns:
            Path to the generated YAML file
        """
        filepath = os.path.join(self.output_dir, filename)
        
        # Combine all schemas into one YAML structure
        combined_dict = {
            'version': 2,
            'models': []
        }
        
        for schema in db_metadata.schemas:
            for table in schema.tables:
                table_dict = self._table_to_yaml_dict(table)
                combined_dict['models'].append(table_dict)
        
        # Write to file
        with open(filepath, 'w', encoding='utf-8') as f:
            yaml.dump(combined_dict, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
        
        logger.info(f"Generated combined YAML file: {filepath}")
        return filepath
    
    def get_tables_without_descriptions(self, db_metadata: DatabaseMetadata) -> List[str]:
        """
        Get list of tables that don't have descriptions.
        
        Args:
            db_metadata: DatabaseMetadata object
            
        Returns:
            List of table names without descriptions
        """
        tables_without_desc = []
        
        for schema in db_metadata.schemas:
            for table in schema.tables:
                if not table.description:
                    tables_without_desc.append(f"{schema.name}.{table.name}")
        
        return tables_without_desc
    
    def get_columns_without_descriptions(self, db_metadata: DatabaseMetadata) -> List[Dict[str, str]]:
        """
        Get list of columns that don't have descriptions.
        
        Args:
            db_metadata: DatabaseMetadata object
            
        Returns:
            List of dictionaries with table and column information
        """
        columns_without_desc = []
        
        for schema in db_metadata.schemas:
            for table in schema.tables:
                for column in table.columns:
                    if not column.description:
                        columns_without_desc.append({
                            'schema': schema.name,
                            'table': table.name,
                            'column': column.name
                        })
        
        return columns_without_desc
