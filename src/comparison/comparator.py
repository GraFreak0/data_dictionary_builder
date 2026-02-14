"""
Schema comparator for comparing source and destination database schemas.
"""

import logging
from typing import Dict, Any, Optional
from ..metadata.extractor import MetadataExtractor
from ..metadata.models import DatabaseMetadata, SchemaMetadata, TableMetadata, ComparisonResult
from ..yaml_generator.generator import YAMLGenerator

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SchemaComparator:
    """Compares schemas between source and destination databases."""
    
    def __init__(
        self,
        source_config: Dict[str, Any],
        destination_config: Dict[str, Any],
        yaml_output_dir: Optional[str] = "./output"
    ):
        """
        Initialize the schema comparator.
        
        Args:
            source_config: Source database connection configuration
                          Must include 'db_type' and connection parameters
            destination_config: Destination database connection configuration
                               Must include 'db_type' and connection parameters
            yaml_output_dir: Directory for YAML output files
        """
        self.source_config = source_config
        self.destination_config = destination_config
        self.yaml_output_dir = yaml_output_dir
        
        # Initialize extractors
        self.source_extractor = MetadataExtractor(**source_config)
        self.destination_extractor = MetadataExtractor(**destination_config)
        
        # Initialize YAML generator
        self.yaml_generator = YAMLGenerator(output_dir=yaml_output_dir)
    
    def compare_schemas(
        self,
        source_schema_name: str,
        destination_schema_name: Optional[str] = None
    ) -> ComparisonResult:
        """
        Compare schemas between source and destination databases.
        
        Args:
            source_schema_name: Name of the schema in source database
            destination_schema_name: Name of the schema in destination database
                                    (defaults to source_schema_name if not provided)
            
        Returns:
            ComparisonResult object with differences
        """
        if destination_schema_name is None:
            destination_schema_name = source_schema_name
        
        logger.info(f"Comparing schemas: {source_schema_name} -> {destination_schema_name}")
        
        # Connect to both databases
        self.source_extractor.connect()
        self.destination_extractor.connect()
        
        try:
            # Extract metadata from both schemas
            source_schema = self.source_extractor.extract_schema(source_schema_name)
            destination_schema = self.destination_extractor.extract_schema(destination_schema_name)
            
            # Perform comparison
            result = self._compare_schema_metadata(source_schema, destination_schema)
            
            logger.info(f"Comparison complete. Found {len(result.missing_tables)} missing tables, "
                       f"{len(result.missing_columns)} missing columns")
            
            return result
        
        finally:
            # Disconnect from databases
            self.source_extractor.disconnect()
            self.destination_extractor.disconnect()
    
    def _compare_schema_metadata(
        self,
        source_schema: SchemaMetadata,
        destination_schema: SchemaMetadata
    ) -> ComparisonResult:
        """
        Compare two schema metadata objects.
        
        Args:
            source_schema: Source schema metadata
            destination_schema: Destination schema metadata
            
        Returns:
            ComparisonResult object
        """
        result = ComparisonResult()
        
        # Create lookup dictionaries
        source_tables = {table.name: table for table in source_schema.tables}
        dest_tables = {table.name: table for table in destination_schema.tables}
        
        # Find missing tables
        for table_name, table in source_tables.items():
            if table_name not in dest_tables:
                result.missing_tables.append({
                    'schema': source_schema.name,
                    'table': table_name,
                    'column_count': len(table.columns)
                })
        
        # Compare tables that exist in both
        for table_name in source_tables.keys():
            if table_name in dest_tables:
                source_table = source_tables[table_name]
                dest_table = dest_tables[table_name]
                
                # Compare columns
                self._compare_table_columns(
                    source_schema.name,
                    source_table,
                    dest_table,
                    result
                )
        
        return result
    
    def _compare_table_columns(
        self,
        schema_name: str,
        source_table: TableMetadata,
        dest_table: TableMetadata,
        result: ComparisonResult
    ) -> None:
        """
        Compare columns between source and destination tables.
        
        Args:
            schema_name: Schema name
            source_table: Source table metadata
            dest_table: Destination table metadata
            result: ComparisonResult to update
        """
        # Create column lookup
        source_columns = {col.name: col for col in source_table.columns}
        dest_columns = {col.name: col for col in dest_table.columns}
        
        # Find missing columns
        for col_name, col in source_columns.items():
            if col_name not in dest_columns:
                result.missing_columns.append({
                    'schema': schema_name,
                    'table': source_table.name,
                    'column': col_name,
                    'data_type': col.data_type
                })
            else:
                # Check for type mismatches
                dest_col = dest_columns[col_name]
                if self._normalize_data_type(col.data_type) != self._normalize_data_type(dest_col.data_type):
                    result.type_mismatches.append({
                        'schema': schema_name,
                        'table': source_table.name,
                        'column': col_name,
                        'source_type': col.data_type,
                        'destination_type': dest_col.data_type
                    })
    
    def _normalize_data_type(self, data_type: str) -> str:
        """
        Normalize data type for comparison across different database systems.
        
        Args:
            data_type: Original data type
            
        Returns:
            Normalized data type
        """
        # Convert to lowercase
        normalized = data_type.lower()
        
        # Basic normalization rules
        type_mappings = {
            'integer': 'int',
            'int4': 'int',
            'int8': 'bigint',
            'character varying': 'varchar',
            'timestamp without time zone': 'timestamp',
            'timestamp with time zone': 'timestamptz',
            'double precision': 'double',
        }
        
        for original, normalized_type in type_mappings.items():
            if original in normalized:
                return normalized_type
        
        return normalized
    
    def compare_and_generate_report(
        self,
        source_schema_name: str,
        destination_schema_name: Optional[str] = None,
        include_yaml_gaps: bool = True
    ) -> Dict[str, Any]:
        """
        Compare schemas and generate a comprehensive report.
        
        Args:
            source_schema_name: Name of the schema in source database
            destination_schema_name: Name of the schema in destination database
            include_yaml_gaps: Whether to include tables/columns without descriptions
            
        Returns:
            Dictionary containing comparison report
        """
        # Perform comparison
        comparison_result = self.compare_schemas(source_schema_name, destination_schema_name)
        
        report = {
            'comparison': comparison_result.to_dict(),
            'summary': {
                'missing_tables_count': len(comparison_result.missing_tables),
                'missing_columns_count': len(comparison_result.missing_columns),
                'type_mismatches_count': len(comparison_result.type_mismatches),
            }
        }
        
        # Add YAML gaps if requested
        if include_yaml_gaps:
            # Extract source metadata for YAML analysis
            self.source_extractor.connect()
            try:
                source_db_metadata = DatabaseMetadata(
                    database_name=self.source_config.get('database', 'source'),
                    database_type=self.source_config['db_type']
                )
                source_schema = self.source_extractor.extract_schema(source_schema_name)
                source_db_metadata.add_schema(source_schema)
                
                # Find tables and columns without descriptions
                tables_without_desc = self.yaml_generator.get_tables_without_descriptions(source_db_metadata)
                columns_without_desc = self.yaml_generator.get_columns_without_descriptions(source_db_metadata)
                
                comparison_result.tables_without_descriptions = tables_without_desc
                comparison_result.columns_without_descriptions = columns_without_desc
                
                report['yaml_gaps'] = {
                    'tables_without_descriptions': tables_without_desc,
                    'columns_without_descriptions': columns_without_desc,
                    'tables_without_descriptions_count': len(tables_without_desc),
                    'columns_without_descriptions_count': len(columns_without_desc)
                }
                report['summary']['tables_without_descriptions_count'] = len(tables_without_desc)
                report['summary']['columns_without_descriptions_count'] = len(columns_without_desc)
            
            finally:
                self.source_extractor.disconnect()
        
        return report
    
    def extract_and_compare_all(
        self,
        source_schemas: Optional[list] = None,
        destination_schemas: Optional[list] = None
    ) -> Dict[str, ComparisonResult]:
        """
        Extract and compare all specified schemas.
        
        Args:
            source_schemas: List of source schema names (None for all)
            destination_schemas: List of destination schema names (None for all)
            
        Returns:
            Dictionary mapping schema names to ComparisonResult objects
        """
        results = {}
        
        self.source_extractor.connect()
        self.destination_extractor.connect()
        
        try:
            # Get schemas to compare
            if source_schemas is None:
                source_schemas = self.source_extractor.get_schemas_list()
            
            if destination_schemas is None:
                destination_schemas = self.destination_extractor.get_schemas_list()
            
            # Compare each schema
            for source_schema in source_schemas:
                # Find corresponding destination schema
                dest_schema = source_schema if source_schema in destination_schemas else None
                
                if dest_schema:
                    logger.info(f"Comparing schema: {source_schema}")
                    source_metadata = self.source_extractor.extract_schema(source_schema)
                    dest_metadata = self.destination_extractor.extract_schema(dest_schema)
                    
                    result = self._compare_schema_metadata(source_metadata, dest_metadata)
                    results[source_schema] = result
                else:
                    logger.warning(f"Schema {source_schema} not found in destination database")
        
        finally:
            self.source_extractor.disconnect()
            self.destination_extractor.disconnect()
        
        return results
