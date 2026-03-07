"""
YAML generator for creating dbt-compatible YAML files from database metadata.
"""

import os
import yaml
import logging
from typing import Dict, Any, List, Optional, Set
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
    
    def _load_existing_yaml(self, filepath: str) -> Optional[Dict[str, Any]]:
        """
        Load existing YAML file if it exists.
        
        Args:
            filepath: Path to the YAML file
            
        Returns:
            Dictionary containing existing YAML content, or None if file doesn't exist
        """
        if not os.path.exists(filepath):
            return None
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                content = yaml.safe_load(f)
                return content if content else None
        except Exception as e:
            logger.warning(f"Failed to load existing YAML file {filepath}: {e}")
            return None
    
    def _merge_models(
        self, 
        existing_models: List[Dict[str, Any]], 
        new_models: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Merge new models with existing models.
        
        - Adds new tables
        - For existing tables, adds new columns
        - Preserves existing descriptions and metadata
        
        Args:
            existing_models: List of existing model definitions
            new_models: List of new model definitions
            
        Returns:
            Merged list of models
        """
        # Create a map of existing models by name
        existing_map = {model['name']: model for model in existing_models}
        
        # Process new models
        merged_models = []
        new_model_names = set()
        
        for new_model in new_models:
            model_name = new_model['name']
            new_model_names.add(model_name)
            
            if model_name in existing_map:
                # Merge with existing model
                existing_model = existing_map[model_name]
                merged_model = self._merge_single_model(existing_model, new_model)
                merged_models.append(merged_model)
            else:
                # Add new model
                merged_models.append(new_model)
                logger.info(f"  + Adding new table: {model_name}")
        
        # Add any existing models that weren't in new models
        for existing_model in existing_models:
            if existing_model['name'] not in new_model_names:
                merged_models.append(existing_model)
                logger.info(f"  ~ Keeping existing table: {existing_model['name']}")
        
        return merged_models
    
    def _merge_single_model(
        self, 
        existing_model: Dict[str, Any], 
        new_model: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Merge a single model (table) definition.
        
        Args:
            existing_model: Existing model definition
            new_model: New model definition
            
        Returns:
            Merged model definition
        """
        model_name = existing_model['name']
        
        # Start with existing model
        merged = existing_model.copy()
        
        # Update meta if new model has it
        if 'meta' in new_model:
            if 'meta' not in merged:
                merged['meta'] = {}
            # Update row_count and other dynamic meta fields
            if 'row_count' in new_model['meta']:
                merged['meta']['row_count'] = new_model['meta']['row_count']
        
        # Merge columns
        existing_columns = existing_model.get('columns', [])
        new_columns = new_model.get('columns', [])
        
        merged_columns = self._merge_columns(existing_columns, new_columns)
        merged['columns'] = merged_columns
        
        logger.info(f"  ~ Merged table: {model_name} ({len(merged_columns)} columns)")
        
        return merged
    
    def _merge_columns(
        self, 
        existing_columns: List[Dict[str, Any]], 
        new_columns: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Merge column definitions.
        
        Args:
            existing_columns: List of existing column definitions
            new_columns: List of new column definitions
            
        Returns:
            Merged list of columns
        """
        # Create a map of existing columns by name
        existing_map = {col['name']: col for col in existing_columns}
        
        merged_columns = []
        new_column_names = set()
        
        for new_col in new_columns:
            col_name = new_col['name']
            new_column_names.add(col_name)
            
            if col_name in existing_map:
                # Keep existing column (preserves descriptions and custom tests)
                existing_col = existing_map[col_name]
                merged_col = existing_col.copy()
                
                # Update data_type if changed
                if new_col.get('data_type') != existing_col.get('data_type'):
                    logger.info(f"    ! Type changed for {col_name}: {existing_col.get('data_type')} -> {new_col.get('data_type')}")
                    merged_col['data_type'] = new_col['data_type']
                
                # Update meta (nullable status, PK/FK changes)
                if 'meta' in new_col:
                    if 'meta' not in merged_col:
                        merged_col['meta'] = {}
                    merged_col['meta'].update(new_col['meta'])
                
                merged_columns.append(merged_col)
            else:
                # Add new column
                merged_columns.append(new_col)
                logger.info(f"    + Adding new column: {col_name}")
        
        # Add any existing columns that weren't in new columns (rare, but handle it)
        for existing_col in existing_columns:
            if existing_col['name'] not in new_column_names:
                merged_columns.append(existing_col)
                logger.warning(f"    - Column {existing_col['name']} no longer exists in database but kept in YAML")
        
        return merged_columns
    
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
        Merges with existing file if it exists.
        
        Args:
            schema: SchemaMetadata object
            filename: Optional custom filename (default: <schema_name>.yml)
            
        Returns:
            Path to the generated YAML file
        """
        if filename is None:
            # Sanitize schema name for filename
            safe_name = schema.name.replace(' ', '_').replace('/', '_').replace('\\', '_')
            filename = f"{safe_name}.yml"
        
        filepath = os.path.join(self.output_dir, filename)
        
        # Convert schema to YAML dictionary
        yaml_dict = self._schema_to_yaml_dict(schema)
        
        # Load existing YAML if it exists
        existing_yaml = self._load_existing_yaml(filepath)
        
        if existing_yaml:
            logger.info(f"Merging with existing file: {filepath}")
            # Merge models
            existing_models = existing_yaml.get('models', [])
            new_models = yaml_dict.get('models', [])
            
            merged_models = self._merge_models(existing_models, new_models)
            yaml_dict['models'] = merged_models
        else:
            logger.info(f"Creating new file: {filepath}")
        
        # Write to file with custom formatting (empty lines between models)
        self._write_yaml_with_formatting(filepath, yaml_dict)
        
        logger.info(f"Generated YAML file: {filepath}")
        return filepath
    
    def _write_yaml_with_formatting(self, filepath: str, yaml_dict: Dict[str, Any]) -> None:
        """
        Write YAML with custom formatting including empty lines between models.
        
        Args:
            filepath: Path to output file
            yaml_dict: Dictionary to write as YAML
        """
        with open(filepath, 'w', encoding='utf-8') as f:
            # Write version
            f.write(f"version: {yaml_dict.get('version', 2)}\n")
            f.write("models:\n")
            
            # Write each model with empty line after
            models = yaml_dict.get('models', [])
            for i, model in enumerate(models):
                # Indent and write model
                model_yaml = yaml.dump({'temp': [model]}, default_flow_style=False, sort_keys=False, allow_unicode=True)
                # Extract just the model part (remove 'temp:' and leading dash)
                lines = model_yaml.split('\n')
                for line in lines[1:]:  # Skip 'temp:' line
                    if line.strip():
                        f.write(line + '\n')
                
                # Add empty line between models (except after last model)
                if i < len(models) - 1:
                    f.write('\n')

    
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
        Merges with existing file if it exists.
        
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
        
        # Load existing YAML if it exists
        existing_yaml = self._load_existing_yaml(filepath)
        
        if existing_yaml:
            logger.info(f"Merging with existing file: {filepath}")
            existing_models = existing_yaml.get('models', [])
            new_models = combined_dict.get('models', [])
            
            merged_models = self._merge_models(existing_models, new_models)
            combined_dict['models'] = merged_models
        else:
            logger.info(f"Creating new file: {filepath}")
        
        # Write to file with custom formatting
        self._write_yaml_with_formatting(filepath, combined_dict)
        
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