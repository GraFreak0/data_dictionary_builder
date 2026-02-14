"""
Google Cloud Spanner database connector implementation.
"""

from google.cloud import spanner
from google.cloud.spanner_v1 import param_types
from typing import List, Optional, Dict, Any
from .base import BaseConnector
from ..metadata.models import TableMetadata, ColumnMetadata


class SpannerConnector(BaseConnector):
    """Connector for Google Cloud Spanner databases."""
    
    def __init__(self, instance_id: str, database_id: str, project_id: Optional[str] = None, **kwargs):
        """
        Initialize Spanner connector.
        
        Args:
            instance_id: Spanner instance ID
            database_id: Database ID
            project_id: Google Cloud project ID (uses default credentials if not provided)
            **kwargs: Additional connection parameters
        """
        super().__init__(
            instance_id=instance_id,
            database_id=database_id,
            project_id=project_id,
            **kwargs
        )
        self.instance_id = instance_id
        self.database_id = database_id
        self.project_id = project_id
        self.db_type = "spanner"
        self.client = None
        self.instance = None
        self.database = None
    
    def connect(self) -> None:
        """Establish connection to Spanner database."""
        try:
            # Create Spanner client
            if self.project_id:
                self.client = spanner.Client(project=self.project_id)
            else:
                self.client = spanner.Client()
            
            # Get instance and database
            self.instance = self.client.instance(self.instance_id)
            self.database = self.instance.database(self.database_id)
            
            # Test connection by running a simple query
            with self.database.snapshot() as snapshot:
                results = snapshot.execute_sql("SELECT 1")
                list(results)
            
            self.connection = self.database
        except Exception as e:
            raise ConnectionError(f"Failed to connect to Spanner database: {e}")
    
    def disconnect(self) -> None:
        """Close the database connection."""
        if self.client:
            self.client.close()
            self.client = None
            self.connection = None
            self.database = None
            self.instance = None
    
    def get_schemas(self) -> List[str]:
        """
        Get list of schemas. Spanner doesn't have schemas in traditional sense.
        
        Returns:
            List with single 'public' schema
        """
        # Spanner doesn't have schemas like PostgreSQL
        # All tables are in a single namespace
        return ['public']
    
    def get_tables(self, schema_name: str = 'public') -> List[str]:
        """
        Get list of tables in the database.
        
        Args:
            schema_name: Ignored for Spanner
            
        Returns:
            List of table names
        """
        with self.database.snapshot() as snapshot:
            results = snapshot.execute_sql("""
                SELECT TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_CATALOG = '' AND TABLE_SCHEMA = ''
                ORDER BY TABLE_NAME
            """)
            tables = [row[0] for row in results]
        return tables
    
    def get_columns(self, schema_name: str, table_name: str) -> List[ColumnMetadata]:
        """
        Get column metadata for a table.
        
        Args:
            schema_name: Ignored for Spanner
            table_name: Name of the table
            
        Returns:
            List of ColumnMetadata objects
        """
        with self.database.snapshot() as snapshot:
            results = snapshot.execute_sql("""
                SELECT 
                    COLUMN_NAME,
                    SPANNER_TYPE,
                    IS_NULLABLE,
                    COLUMN_DEFAULT,
                    ORDINAL_POSITION
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = @table_name
                ORDER BY ORDINAL_POSITION
            """, params={'table_name': table_name}, param_types={'table_name': param_types.STRING})
            
            columns_info = list(results)
        
        # Get primary keys
        primary_keys = self.get_primary_keys(schema_name, table_name)
        
        columns = []
        for row in columns_info:
            col_name = row[0]
            spanner_type = row[1]
            is_nullable = row[2] == 'YES'
            default_value = row[3]
            ordinal_position = row[4]
            
            column = ColumnMetadata(
                name=col_name,
                data_type=spanner_type,
                is_nullable=is_nullable,
                is_primary_key=col_name in primary_keys,
                default_value=default_value,
                ordinal_position=ordinal_position
            )
            
            columns.append(column)
        
        return columns
    
    def get_primary_keys(self, schema_name: str, table_name: str) -> List[str]:
        """
        Get primary key columns for a table.
        
        Args:
            schema_name: Ignored for Spanner
            table_name: Name of the table
            
        Returns:
            List of primary key column names
        """
        with self.database.snapshot() as snapshot:
            results = snapshot.execute_sql("""
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.INDEX_COLUMNS
                WHERE TABLE_NAME = @table_name
                AND INDEX_NAME = 'PRIMARY_KEY'
                ORDER BY ORDINAL_POSITION
            """, params={'table_name': table_name}, param_types={'table_name': param_types.STRING})
            
            primary_keys = [row[0] for row in results]
        return primary_keys
    
    def get_foreign_keys(self, schema_name: str, table_name: str) -> List[Dict[str, str]]:
        """
        Get foreign key relationships for a table.
        
        Args:
            schema_name: Ignored for Spanner
            table_name: Name of the table
            
        Returns:
            List of foreign key dictionaries
        """
        with self.database.snapshot() as snapshot:
            results = snapshot.execute_sql("""
                SELECT
                    rc.CONSTRAINT_NAME,
                    kcu.COLUMN_NAME,
                    rc.UNIQUE_CONSTRAINT_NAME,
                    kcu2.TABLE_NAME AS REFERENCED_TABLE,
                    kcu2.COLUMN_NAME AS REFERENCED_COLUMN
                FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                    ON rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu2
                    ON rc.UNIQUE_CONSTRAINT_NAME = kcu2.CONSTRAINT_NAME
                WHERE kcu.TABLE_NAME = @table_name
            """, params={'table_name': table_name}, param_types={'table_name': param_types.STRING})
            
            foreign_keys = []
            for row in results:
                foreign_keys.append({
                    'column': row[1],
                    'referenced_table': row[3],
                    'referenced_column': row[4]
                })
        
        return foreign_keys
    
    def get_table_metadata(self, schema_name: str, table_name: str) -> TableMetadata:
        """
        Get complete metadata for a table.
        
        Args:
            schema_name: Ignored for Spanner
            table_name: Name of the table
            
        Returns:
            TableMetadata object
        """
        table_metadata = TableMetadata(
            name=table_name,
            schema_name='public',
            table_type='BASE TABLE'
        )
        
        # Get columns
        columns = self.get_columns(schema_name, table_name)
        for column in columns:
            table_metadata.add_column(column)
        
        # Get primary keys
        table_metadata.primary_keys = self.get_primary_keys(schema_name, table_name)
        
        # Get row count (Spanner doesn't provide easy way to get exact count)
        table_metadata.row_count = self.get_table_row_count(schema_name, table_name)
        
        return table_metadata
    
    def get_database_version(self) -> Optional[str]:
        """
        Get Spanner version info.
        
        Returns:
            Spanner version string
        """
        # Spanner doesn't expose version in the same way
        return "Google Cloud Spanner"
    
    def get_table_row_count(self, schema_name: str, table_name: str) -> Optional[int]:
        """
        Get approximate row count for a table.
        
        Note: This performs a full table scan and can be expensive for large tables.
        
        Args:
            schema_name: Ignored for Spanner
            table_name: Name of the table
            
        Returns:
            Row count or None
        """
        try:
            with self.database.snapshot() as snapshot:
                results = snapshot.execute_sql(f"SELECT COUNT(*) FROM {table_name}")
                for row in results:
                    return row[0]
            return None
        except Exception:
            # Return None if count fails (table might be too large)
            return None
