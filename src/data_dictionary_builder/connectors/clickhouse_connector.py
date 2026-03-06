"""
ClickHouse database connector implementation.
"""

from clickhouse_driver import Client
from typing import List, Optional, Dict, Any
from .base import BaseConnector
from ..metadata.models import SchemaMetadata, TableMetadata, ColumnMetadata


class ClickHouseConnector(BaseConnector):
    """Connector for ClickHouse databases."""
    
    def __init__(self, host: str, port: int, database: str = 'default', user: str = 'default', password: str = '', **kwargs):
        """
        Initialize ClickHouse connector.
        
        Args:
            host: Database host
            port: Database port (native protocol, typically 9000)
            database: Database name (optional - defaults to 'default')
            user: Username (default: 'default')
            password: Password
            **kwargs: Additional connection parameters
        """
        super().__init__(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            **kwargs
        )
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.db_type = "clickhouse"
        self.server_mode = database is None  # ClickHouse can extract from all databases
        self.connect_database = database if database else 'default'
    
    def connect(self) -> None:
        """Establish connection to ClickHouse database."""
        try:
            self.connection = Client(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                **{k: v for k, v in self.connection_params.items() 
                   if k not in ['host', 'port', 'database', 'user', 'password']}
            )
            # Test connection
            self.connection.execute('SELECT 1')
        except Exception as e:
            raise ConnectionError(f"Failed to connect to ClickHouse database: {e}")
    
    def disconnect(self) -> None:
        """Close the database connection."""
        if self.connection:
            self.connection.disconnect()
            self.connection = None
    
    def get_schemas(self) -> List[str]:
        """
        Get list of all databases (schemas) in ClickHouse.
        
        Returns:
            List of schema names
        """
        result = self.connection.execute("""
            SELECT name 
            FROM system.databases 
            WHERE name NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
            ORDER BY name
        """)
        schemas = [row[0] for row in result]
        return schemas if schemas else [self.database]
    
    def get_tables(self, schema_name: str) -> List[str]:
        """
        Get list of tables in a schema (database).
        
        Args:
            schema_name: Name of the schema (database)
            
        Returns:
            List of table names
        """
        result = self.connection.execute("""
            SELECT name 
            FROM system.tables 
            WHERE database = %(database)s
            AND engine NOT IN ('View', 'MaterializedView')
            ORDER BY name
        """, {'database': schema_name})
        tables = [row[0] for row in result]
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
        result = self.connection.execute("""
            SELECT 
                name,
                type,
                default_kind,
                default_expression,
                comment,
                position
            FROM system.columns
            WHERE database = %(database)s AND table = %(table)s
            ORDER BY position
        """, {'database': schema_name, 'table': table_name})
        
        # Get primary keys
        primary_keys = self.get_primary_keys(schema_name, table_name)
        
        columns = []
        for row in result:
            col_name = row[0]
            col_type = row[1]
            default_kind = row[2]
            default_expr = row[3]
            comment = row[4]
            position = row[5]
            
            # ClickHouse doesn't have traditional NULL concept, but Nullable() type wrapper
            is_nullable = 'Nullable' in col_type
            
            # Extract base type if nullable
            data_type = col_type.replace('Nullable(', '').rstrip(')') if is_nullable else col_type
            
            column = ColumnMetadata(
                name=col_name,
                data_type=data_type,
                is_nullable=is_nullable,
                is_primary_key=col_name in primary_keys,
                default_value=default_expr if default_kind else None,
                description=comment if comment else None,
                ordinal_position=position
            )
            
            columns.append(column)
        
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
        try:
            result = self.connection.execute("""
                SELECT primary_key
                FROM system.tables
                WHERE database = %(database)s AND name = %(table)s
            """, {'database': schema_name, 'table': table_name})
            
            if result and result[0][0]:
                # Primary key is returned as a string like "id" or "(id, timestamp)"
                pk_str = result[0][0].strip('()')
                # Split by comma and clean up
                primary_keys = [pk.strip() for pk in pk_str.split(',')]
                return primary_keys
            return []
        except Exception:
            return []
    
    def get_foreign_keys(self, schema_name: str, table_name: str) -> List[Dict[str, str]]:
        """
        Get foreign key relationships for a table.
        
        ClickHouse doesn't enforce foreign keys, but may have metadata.
        
        Args:
            schema_name: Name of the schema
            table_name: Name of the table
            
        Returns:
            List of foreign key dictionaries (usually empty for ClickHouse)
        """
        # ClickHouse doesn't have traditional foreign key constraints
        return []
    
    def get_table_metadata(self, schema_name: str, table_name: str) -> TableMetadata:
        """
        Get complete metadata for a table.
        
        Args:
            schema_name: Name of the schema
            table_name: Name of the table
            
        Returns:
            TableMetadata object
        """
        # Get table engine and comment
        result = self.connection.execute("""
            SELECT 
                engine,
                comment,
                total_rows
            FROM system.tables
            WHERE database = %(database)s AND name = %(table)s
        """, {'database': schema_name, 'table': table_name})
        
        engine = result[0][0] if result else 'Unknown'
        comment = result[0][1] if result and result[0][1] else None
        total_rows = result[0][2] if result else None
        
        table_metadata = TableMetadata(
            name=table_name,
            schema_name=schema_name,
            table_type=f'BASE TABLE ({engine})',
            description=comment,
            row_count=total_rows
        )
        
        # Get columns
        columns = self.get_columns(schema_name, table_name)
        for column in columns:
            table_metadata.add_column(column)
        
        # Get primary keys
        table_metadata.primary_keys = self.get_primary_keys(schema_name, table_name)
        
        return table_metadata
    
    def extract_schema_metadata(self, schema_name: str) -> SchemaMetadata:
        """
        Bulk-optimised schema extraction for ClickHouse.

        Fetches all table info and all column info for the entire schema in
        **2 queries** instead of the base-class default of 3 queries per table
        (system.tables for metadata + system.tables for PKs + system.columns).
        For a schema with N tables this reduces round-trips from 3N → 2.

        Args:
            schema_name: ClickHouse database name to extract.

        Returns:
            SchemaMetadata populated with all tables and columns.
        """
        # ── Query 1: all tables (engine, comment, row count, primary key) ──
        tables_result = self.connection.execute(
            """
            SELECT name, engine, comment, total_rows, primary_key
            FROM system.tables
            WHERE database = %(db)s
              AND engine NOT IN ('View', 'MaterializedView')
            ORDER BY name
            """,
            {"db": schema_name},
        )

        if not tables_result:
            return SchemaMetadata(name=schema_name)

        tables_info: Dict[str, Dict[str, Any]] = {}
        for name, engine, comment, total_rows, pk_str in tables_result:
            pk_list: List[str] = []
            if pk_str:
                pk_list = [p.strip() for p in pk_str.strip("()").split(",") if p.strip()]
            tables_info[name] = {
                "engine":       engine,
                "comment":      comment or None,
                "total_rows":   total_rows,
                "primary_keys": pk_list,
            }

        # ── Query 2: all columns for every table in one round-trip ──────────
        columns_result = self.connection.execute(
            """
            SELECT table, name, type, default_kind, default_expression,
                   comment, position
            FROM system.columns
            WHERE database = %(db)s
              AND table IN %(tables)s
            ORDER BY table, position
            """,
            {"db": schema_name, "tables": tuple(tables_info.keys())},
        )

        # Group column rows by table name
        cols_by_table: Dict[str, list] = {t: [] for t in tables_info}
        for row in columns_result:
            tbl = row[0]
            if tbl in cols_by_table:
                cols_by_table[tbl].append(row)

        # ── Build SchemaMetadata in-memory ───────────────────────────────────
        schema_metadata = SchemaMetadata(name=schema_name)

        for table_name, tinfo in tables_info.items():
            pk_set = set(tinfo["primary_keys"])
            table_metadata = TableMetadata(
                name=table_name,
                schema_name=schema_name,
                table_type=f'BASE TABLE ({tinfo["engine"]})',
                description=tinfo["comment"],
                row_count=tinfo["total_rows"],
            )
            table_metadata.primary_keys = tinfo["primary_keys"]

            for _, col_name, col_type, default_kind, default_expr, col_comment, position in cols_by_table.get(table_name, []):
                is_nullable = "Nullable" in col_type
                data_type = col_type.replace("Nullable(", "").rstrip(")") if is_nullable else col_type
                table_metadata.add_column(ColumnMetadata(
                    name=col_name,
                    data_type=data_type,
                    is_nullable=is_nullable,
                    is_primary_key=col_name in pk_set,
                    default_value=default_expr if default_kind else None,
                    description=col_comment or None,
                    ordinal_position=position,
                ))

            schema_metadata.add_table(table_metadata)

        return schema_metadata

    def get_database_version(self) -> Optional[str]:
        """
        Get ClickHouse version.

        Returns:
            ClickHouse version string
        """
        result = self.connection.execute("SELECT version()")
        return result[0][0] if result else None
    
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
            result = self.connection.execute("""
                SELECT total_rows
                FROM system.tables
                WHERE database = %(database)s AND name = %(table)s
            """, {'database': schema_name, 'table': table_name})
            
            return result[0][0] if result else None
        except Exception:
            return None