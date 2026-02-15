"""
PostgreSQL database connector implementation.
"""

import psycopg2
from psycopg2.extras import RealDictCursor
from typing import List, Optional, Dict, Any
from .base import BaseConnector
from ..metadata.models import TableMetadata, ColumnMetadata


class PostgresConnector(BaseConnector):
    """Connector for PostgreSQL databases."""
    
    def __init__(self, host: str, port: int, database: str = None, user: str = None, password: str = None, **kwargs):
        """
        Initialize PostgreSQL connector.
        
        Args:
            host: Database host
            port: Database port
            database: Database name (optional - if not provided, connects to all databases on server)
            user: Username
            password: Password
            **kwargs: Additional connection parameters
        """
        # If no database specified, connect to 'postgres' (default database)
        connect_database = database if database else 'postgres'
        
        super().__init__(
            host=host,
            port=port,
            database=connect_database,
            user=user,
            password=password,
            **kwargs
        )
        self.host = host
        self.port = port
        self.database = database  # Store original (may be None)
        self.connect_database = connect_database  # For connection
        self.user = user
        self.password = password
        self.db_type = "postgresql"
        self.server_mode = database is None  # Flag to indicate server-level extraction
    
    def connect(self) -> None:
        """Establish connection to PostgreSQL database."""
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.connect_database,
                user=self.user,
                password=self.password,
                **{k: v for k, v in self.connection_params.items() 
                   if k not in ['host', 'port', 'database', 'user', 'password']}
            )
        except Exception as e:
            raise ConnectionError(f"Failed to connect to PostgreSQL database: {e}")
    
    def disconnect(self) -> None:
        """Close the database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
    
    def switch_database(self, database_name: str) -> None:
        """
        Switch to a different database on the same server.
        Only works in server mode (PostgreSQL/MySQL).
        
        Args:
            database_name: Name of the database to switch to
        """
        if hasattr(self, 'server_mode') and self.server_mode:
            # Close current connection
            if self.connection:
                self.connection.close()
            
            # Update database and reconnect
            self.connect_database = database_name
            self.connect()
    
    def get_schemas(self) -> List[str]:
        """
        Get list of all schemas in the database.
        If in server mode (no database specified), returns schemas from all databases.
        
        Returns:
            List of schema names
        """
        if self.server_mode:
            # Get all databases on the server (excluding system databases)
            cursor = self.connection.cursor()
            cursor.execute("""
                SELECT datname 
                FROM pg_database 
                WHERE datistemplate = false 
                AND datname NOT IN ('postgres', 'template0', 'template1')
                ORDER BY datname
            """)
            databases = [row[0] for row in cursor.fetchall()]
            cursor.close()
            return databases
        else:
            # Get schemas in the specified database
            cursor = self.connection.cursor()
            cursor.execute("""
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                ORDER BY schema_name
            """)
            schemas = [row[0] for row in cursor.fetchall()]
            cursor.close()
            return schemas
    
    def get_tables(self, schema_name: str) -> List[str]:
        """
        Get list of tables in a schema.
        
        Args:
            schema_name: Name of the schema
            
        Returns:
            List of table names
        """
        cursor = self.connection.cursor()
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = %s 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """, (schema_name,))
        tables = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return tables
    
    def get_columns(self, schema_name: str, table_name: str) -> List[ColumnMetadata]:
        """
        Get column metadata for a table.
        
        Args:
            schema_name: Name of the schema
            table_name: Name of the table
            
        Returns:
            List of ColumnMetadata objects
        """
        cursor = self.connection.cursor(cursor_factory=RealDictCursor)
        
        # Get column information
        cursor.execute("""
            SELECT 
                column_name,
                data_type,
                is_nullable,
                column_default,
                character_maximum_length,
                numeric_precision,
                numeric_scale,
                ordinal_position,
                udt_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """, (schema_name, table_name))
        
        columns_info = cursor.fetchall()
        
        # Get primary keys
        primary_keys = self.get_primary_keys(schema_name, table_name)
        
        # Get foreign keys
        foreign_keys = self.get_foreign_keys(schema_name, table_name)
        fk_map = {fk['column']: fk for fk in foreign_keys}
        
        columns = []
        for col_info in columns_info:
            column = ColumnMetadata(
                name=col_info['column_name'],
                data_type=col_info['data_type'],
                is_nullable=col_info['is_nullable'] == 'YES',
                is_primary_key=col_info['column_name'] in primary_keys,
                default_value=col_info['column_default'],
                character_maximum_length=col_info['character_maximum_length'],
                numeric_precision=col_info['numeric_precision'],
                numeric_scale=col_info['numeric_scale'],
                ordinal_position=col_info['ordinal_position']
            )
            
            # Check if this column is a foreign key
            if col_info['column_name'] in fk_map:
                column.is_foreign_key = True
                column.foreign_key_table = fk_map[col_info['column_name']]['referenced_table']
                column.foreign_key_column = fk_map[col_info['column_name']]['referenced_column']
            
            columns.append(column)
        
        cursor.close()
        return columns
    
    def get_primary_keys(self, schema_name: str, table_name: str) -> List[str]:
        """
        Get primary key columns for a table.
        
        Args:
            schema_name: Name of the schema
            table_name: Name of the table
            
        Returns:
            List of primary key column names
        """
        cursor = self.connection.cursor()
        cursor.execute("""
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = %s::regclass
            AND i.indisprimary
        """, (f"{schema_name}.{table_name}",))
        
        primary_keys = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return primary_keys
    
    def get_foreign_keys(self, schema_name: str, table_name: str) -> List[Dict[str, str]]:
        """
        Get foreign key relationships for a table.
        
        Args:
            schema_name: Name of the schema
            table_name: Name of the table
            
        Returns:
            List of foreign key dictionaries
        """
        cursor = self.connection.cursor(cursor_factory=RealDictCursor)
        cursor.execute("""
            SELECT
                kcu.column_name,
                ccu.table_schema AS referenced_schema,
                ccu.table_name AS referenced_table,
                ccu.column_name AS referenced_column
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
                AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
                AND tc.table_schema = %s
                AND tc.table_name = %s
        """, (schema_name, table_name))
        
        foreign_keys = []
        for row in cursor.fetchall():
            foreign_keys.append({
                'column': row['column_name'],
                'referenced_table': row['referenced_table'],
                'referenced_column': row['referenced_column']
            })
        
        cursor.close()
        return foreign_keys
    
    def get_table_metadata(self, schema_name: str, table_name: str) -> TableMetadata:
        """
        Get complete metadata for a table.
        
        Args:
            schema_name: Name of the schema
            table_name: Name of the table
            
        Returns:
            TableMetadata object
        """
        cursor = self.connection.cursor(cursor_factory=RealDictCursor)
        
        # Get table type and description
        cursor.execute("""
            SELECT 
                obj_description(oid) as description
            FROM pg_class
            WHERE relname = %s
            AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = %s)
        """, (table_name, schema_name))
        
        result = cursor.fetchone()
        description = result['description'] if result else None
        
        table_metadata = TableMetadata(
            name=table_name,
            schema_name=schema_name,
            table_type='BASE TABLE',
            description=description
        )
        
        # Get columns
        columns = self.get_columns(schema_name, table_name)
        for column in columns:
            table_metadata.add_column(column)
        
        # Get primary keys
        table_metadata.primary_keys = self.get_primary_keys(schema_name, table_name)
        
        # Get row count
        table_metadata.row_count = self.get_table_row_count(schema_name, table_name)
        
        cursor.close()
        return table_metadata
    
    def get_database_version(self) -> Optional[str]:
        """
        Get PostgreSQL version.
        
        Returns:
            PostgreSQL version string
        """
        cursor = self.connection.cursor()
        cursor.execute("SELECT version()")
        version = cursor.fetchone()[0]
        cursor.close()
        return version
    
    def get_table_row_count(self, schema_name: str, table_name: str) -> Optional[int]:
        """
        Get approximate row count for a table.
        
        Args:
            schema_name: Name of the schema
            table_name: Name of the table
            
        Returns:
            Row count
        """
        try:
            cursor = self.connection.cursor()
            # Use pg_class for approximate count (faster)
            cursor.execute("""
                SELECT reltuples::bigint AS estimate
                FROM pg_class
                WHERE oid = %s::regclass
            """, (f"{schema_name}.{table_name}",))
            
            result = cursor.fetchone()
            cursor.close()
            return result[0] if result else None
        except Exception:
            return None