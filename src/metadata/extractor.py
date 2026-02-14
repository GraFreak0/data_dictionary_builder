"""
Metadata extractor for extracting database metadata.
"""

import logging
from typing import List, Optional, Dict, Any
from ..connectors import get_connector
from ..connectors.base import BaseConnector
from .models import DatabaseMetadata, SchemaMetadata

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MetadataExtractor:
    """Main class for extracting database metadata."""
    
    def __init__(self, db_type: str, **connection_params):
        """
        Initialize the metadata extractor.
        
        Args:
            db_type: Type of database ('sqlite', 'postgres', 'mysql', 'clickhouse', 'spanner')
            **connection_params: Database connection parameters
        """
        self.db_type = db_type
        self.connection_params = connection_params
        self.connector: Optional[BaseConnector] = None
        
    def _get_connector(self) -> BaseConnector:
        """Get or create database connector."""
        if self.connector is None:
            self.connector = get_connector(self.db_type, **self.connection_params)
        return self.connector
    
    def connect(self) -> None:
        """Establish database connection."""
        connector = self._get_connector()
        connector.connect()
        logger.info(f"Connected to {self.db_type} database")
    
    def disconnect(self) -> None:
        """Close database connection."""
        if self.connector:
            self.connector.disconnect()
            logger.info(f"Disconnected from {self.db_type} database")
    
    def extract_schema(self, schema_name: str) -> SchemaMetadata:
        """
        Extract metadata for a single schema.
        
        Args:
            schema_name: Name of the schema to extract
            
        Returns:
            SchemaMetadata object
        """
        connector = self._get_connector()
        
        logger.info(f"Extracting metadata for schema: {schema_name}")
        schema_metadata = connector.extract_schema_metadata(schema_name)
        logger.info(f"Extracted {len(schema_metadata.tables)} tables from schema: {schema_name}")
        
        return schema_metadata
    
    def extract_all_schemas(self, schema_filter: Optional[List[str]] = None) -> DatabaseMetadata:
        """
        Extract metadata for all schemas in the database.
        
        Args:
            schema_filter: Optional list of schema names to extract. If None, extracts all schemas.
            
        Returns:
            DatabaseMetadata object containing all schemas
        """
        connector = self._get_connector()
        
        # Get database version
        version = connector.get_database_version()
        
        # Create database metadata object
        db_metadata = DatabaseMetadata(
            database_name=self.connection_params.get('database', 'unknown'),
            database_type=self.db_type,
            version=version,
            host=self.connection_params.get('host'),
            port=self.connection_params.get('port')
        )
        
        # Get all schemas
        all_schemas = connector.get_schemas()
        logger.info(f"Found {len(all_schemas)} schemas in database")
        
        # Filter schemas if specified
        if schema_filter:
            schemas_to_extract = [s for s in all_schemas if s in schema_filter]
            logger.info(f"Filtering to {len(schemas_to_extract)} schemas: {schemas_to_extract}")
        else:
            schemas_to_extract = all_schemas
        
        # Extract metadata for each schema
        for schema_name in schemas_to_extract:
            try:
                schema_metadata = self.extract_schema(schema_name)
                db_metadata.add_schema(schema_metadata)
            except Exception as e:
                logger.error(f"Error extracting schema {schema_name}: {e}")
                continue
        
        logger.info(f"Extraction complete. Total schemas: {len(db_metadata.schemas)}")
        return db_metadata
    
    def extract_table(self, schema_name: str, table_name: str):
        """
        Extract metadata for a single table.
        
        Args:
            schema_name: Name of the schema
            table_name: Name of the table
            
        Returns:
            TableMetadata object
        """
        connector = self._get_connector()
        
        logger.info(f"Extracting metadata for table: {schema_name}.{table_name}")
        table_metadata = connector.get_table_metadata(schema_name, table_name)
        logger.info(f"Extracted {len(table_metadata.columns)} columns from table: {table_name}")
        
        return table_metadata
    
    def test_connection(self) -> bool:
        """
        Test if the database connection is working.
        
        Returns:
            True if connection is successful, False otherwise
        """
        try:
            connector = self._get_connector()
            result = connector.test_connection()
            if result:
                logger.info("Database connection test successful")
            else:
                logger.error("Database connection test failed")
            return result
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False
    
    def get_schemas_list(self) -> List[str]:
        """
        Get list of all schemas in the database.
        
        Returns:
            List of schema names
        """
        connector = self._get_connector()
        return connector.get_schemas()
    
    def get_tables_list(self, schema_name: str) -> List[str]:
        """
        Get list of all tables in a schema.
        
        Args:
            schema_name: Name of the schema
            
        Returns:
            List of table names
        """
        connector = self._get_connector()
        return connector.get_tables(schema_name)
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
