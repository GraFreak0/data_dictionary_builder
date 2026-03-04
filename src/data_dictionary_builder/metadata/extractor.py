"""
Metadata extractor for extracting database metadata.
"""

import fnmatch
import logging
import re
from typing import Dict, List, Optional, Union

from ..connectors import get_connector
from ..connectors.base import BaseConnector
from .models import DatabaseMetadata, SchemaMetadata

# schema_filter accepts either:
#   - None                   → extract everything
#   - List[str]              → each entry may be:
#       • an exact name      "public"
#       • a glob/wildcard    "monkeybook_%"  or  "stg_*"
#       • a prefix marker    "prefix:stg_"
#       • a suffix marker    "suffix:_prod"
#       • a regex marker     "regex:^analytics_\\d{4}$"
SchemaFilterSpec = Optional[List[str]]

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MetadataExtractor:
    """Main class for extracting database metadata."""

    # ------------------------------------------------------------------ #
    # Schema-filter resolution                                             #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _resolve_schema_filter(
        schema_filter: SchemaFilterSpec,
        available_schemas: List[str],
    ) -> List[str]:
        """
        Resolve *schema_filter* against *available_schemas* and return the
        final list of schema names to extract.

        The method is called *after* the connector has already fetched the
        full list of schemas from the database, so every match is tested
        against real schema names — no guessing.

        Filter entry formats
        --------------------
        None / omitted
            Extract every available schema (no filtering).

        Exact name  ``"public"``
            Included only when "public" is present in *available_schemas*.

        Glob / SQL-LIKE wildcard  ``"monkeybook_%"``
            ``_`` matches any single character, ``%`` or ``*`` match any
            sequence of characters (case-insensitive).
            Example: ``"stg_*"`` matches ``stg_orders``, ``stg_customers``.

        Prefix marker  ``"prefix:stg_"``
            Matches any schema whose name starts with ``stg_``.

        Suffix marker  ``"suffix:_prod"``
            Matches any schema whose name ends with ``_prod``.

        Contains marker  ``"contains:analytics"``
            Matches any schema whose name contains ``analytics``.

        Regex marker  ``"regex:^analytics_\\d{4}$"``
            Full ``re.fullmatch`` against the schema name (case-insensitive
            by default).

        Entries can be mixed freely in the same list:

            schema_filter=[
                "public",             # exact
                "monkeybook_%",       # glob
                "prefix:stg_",        # prefix
                "suffix:_prod",       # suffix
                "regex:^tmp_\\d+$",   # regex
            ]

        Parameters
        ----------
        schema_filter : list[str] | None
            The filter value passed to ``extract_all_schemas()``.
        available_schemas : list[str]
            All schema names returned by the connector for this
            database / server.

        Returns
        -------
        list[str]
            Ordered, deduplicated list of schema names that matched.
        """
        if not schema_filter:
            return available_schemas

        matched: List[str] = []
        seen: set = set()

        def _add(name: str) -> None:
            if name not in seen:
                seen.add(name)
                matched.append(name)

        for entry in schema_filter:
            entry_lower = entry.lower()

            # ── Explicit markers ────────────────────────────────────────
            if entry_lower.startswith("prefix:"):
                prefix = entry[len("prefix:"):]
                for s in available_schemas:
                    if s.lower().startswith(prefix.lower()):
                        _add(s)

            elif entry_lower.startswith("suffix:"):
                suffix = entry[len("suffix:"):]
                for s in available_schemas:
                    if s.lower().endswith(suffix.lower()):
                        _add(s)

            elif entry_lower.startswith("contains:"):
                substr = entry[len("contains:"):]
                for s in available_schemas:
                    if substr.lower() in s.lower():
                        _add(s)

            elif entry_lower.startswith("regex:"):
                pattern = entry[len("regex:"):]
                for s in available_schemas:
                    if re.fullmatch(pattern, s, re.IGNORECASE):
                        _add(s)

            # ── Glob / wildcard (_, %, *)  ───────────────────────────────
            elif any(c in entry for c in ("_", "%", "*", "?")):
                # Normalise SQL-LIKE wildcards to fnmatch style:
                #   %  →  *      (any sequence)
                #   _  →  ?      (any single char)  — only when used as wildcard
                # We convert % first, then handle _ carefully:
                # a leading/trailing _ is almost certainly a naming convention
                # character, not a wildcard; treat _ as a wildcard only when
                # the entry also contains % or *.
                glob = entry.replace("%", "*")
                if "*" in glob:
                    # entry was SQL-LIKE style — also treat _ as single-char wildcard
                    glob = glob.replace("_", "?")
                for s in available_schemas:
                    if fnmatch.fnmatchcase(s.lower(), glob.lower()):
                        _add(s)

            # ── Exact match (original behaviour) ────────────────────────
            else:
                if entry in available_schemas:
                    _add(entry)

        logger.info(
            f"schema_filter {schema_filter!r} matched "
            f"{len(matched)} schema(s): {matched}"
        )
        return matched

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
        If database was not specified in config, extracts from all databases on server.
        
        Args:
            schema_filter: Optional list of schema names to extract. If None, extracts all schemas.
            
        Returns:
            DatabaseMetadata object containing all schemas
        """
        connector = self._get_connector()
        
        # Get database version
        version = connector.get_database_version()
        
        # Check if in server mode
        server_mode = hasattr(connector, 'server_mode') and connector.server_mode
        
        if server_mode:
            # Server mode: database parameter was None, extract from all databases
            logger.info("Server mode: Extracting from all databases on server")
            
            db_metadata = DatabaseMetadata(
                database_name=f"{connector.host}:{connector.port}",
                database_type=self.db_type,
                version=version,
                host=self.connection_params.get('host'),
                port=self.connection_params.get('port')
            )
            
            # Get all databases
            all_databases = connector.get_schemas()  # In server mode, this returns databases
            logger.info(f"Found {len(all_databases)} databases on server")
            
            # Filter databases — supports exact names, globs, prefix:/suffix:/regex: markers
            databases_to_extract = self._resolve_schema_filter(schema_filter, all_databases)
            
            # Extract each database as a "schema"
            for db_name in databases_to_extract:
                try:
                    logger.info(f"Extracting database: {db_name}")
                    
                    # Switch to this database
                    connector.switch_database(db_name)
                    
                    # Get actual schemas in this database
                    actual_schemas = connector.get_schemas()
                    
                    # Create a schema metadata for this database
                    from .models import SchemaMetadata
                    db_schema = SchemaMetadata(name=db_name)
                    
                    # Extract tables from each schema in this database
                    for schema_name in actual_schemas:
                        if schema_name in ['pg_catalog', 'information_schema', 'pg_toast']:
                            continue
                        
                        tables = connector.get_tables(schema_name)
                        for table_name in tables:
                            table_metadata = connector.get_table_metadata(schema_name, table_name)
                            # Prefix table name with schema to avoid conflicts
                            table_metadata.name = f"{schema_name}.{table_name}"
                            db_schema.add_table(table_metadata)
                    
                    db_metadata.add_schema(db_schema)
                    logger.info(f"Extracted {len(db_schema.tables)} tables from database: {db_name}")
                    
                except Exception as e:
                    logger.error(f"Error extracting database {db_name}: {e}")
                    continue
            
            logger.info(f"Server extraction complete. Total databases: {len(db_metadata.schemas)}")
            return db_metadata
        
        else:
            # Normal mode: single database specified
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
            
            # Filter schemas — supports exact names, globs, prefix:/suffix:/regex: markers
            schemas_to_extract = self._resolve_schema_filter(schema_filter, all_schemas)
            
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