"""
Metadata extractor for extracting database metadata.
"""

import fnmatch
import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple, Union

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
    
    # ------------------------------------------------------------------ #
    # Parallel extraction helpers                                          #
    # ------------------------------------------------------------------ #

    def _extract_schema_worker(self, schema_name: str) -> Tuple[str, Optional[SchemaMetadata]]:
        """
        Worker executed inside a thread pool.  Each invocation creates its
        own connector so threads never share a connection.

        Returns
        -------
        (schema_name, SchemaMetadata | None)
            None is returned when extraction fails so the caller can log the
            error without crashing the whole pool.
        """
        try:
            worker_connector = get_connector(self.db_type, **self.connection_params)
            worker_connector.connect()
            try:
                logger.info(f"[worker] Extracting schema: {schema_name}")
                schema_metadata = worker_connector.extract_schema_metadata(schema_name)
                logger.info(
                    f"[worker] Done — {schema_name}: "
                    f"{len(schema_metadata.tables)} table(s)"
                )
                return schema_name, schema_metadata
            finally:
                worker_connector.disconnect()
        except Exception as exc:
            logger.error(f"[worker] Error extracting schema '{schema_name}': {exc}")
            return schema_name, None

    def _extract_server_db_worker(self, db_name: str) -> Tuple[str, Optional[SchemaMetadata]]:
        """
        Worker for server-mode: extracts all tables from a single database,
        using its own dedicated connector.

        Returns
        -------
        (db_name, SchemaMetadata | None)
        """
        try:
            worker_connector = get_connector(self.db_type, **self.connection_params)
            worker_connector.connect()
            try:
                worker_connector.switch_database(db_name)
                actual_schemas = worker_connector.get_schemas()

                db_schema = SchemaMetadata(name=db_name)
                for schema_name in actual_schemas:
                    if schema_name in ['pg_catalog', 'information_schema', 'pg_toast']:
                        continue
                    tables = worker_connector.get_tables(schema_name)
                    for table_name in tables:
                        table_metadata = worker_connector.get_table_metadata(schema_name, table_name)
                        table_metadata.name = f"{schema_name}.{table_name}"
                        db_schema.add_table(table_metadata)

                logger.info(
                    f"[worker] Done — {db_name}: {len(db_schema.tables)} table(s)"
                )
                return db_name, db_schema
            finally:
                worker_connector.disconnect()
        except Exception as exc:
            logger.error(f"[worker] Error extracting database '{db_name}': {exc}")
            return db_name, None

    def _run_parallel(
        self,
        worker_fn,
        items: List[str],
        parallel_workers: int,
    ) -> List[Tuple[str, Optional[SchemaMetadata]]]:
        """
        Submit *items* to *worker_fn* using a ThreadPoolExecutor capped at
        *parallel_workers* threads (further capped at len(items) so
        over-provisioning is harmless).

        Results are returned in the **original order** of *items*.
        """
        actual_workers = min(parallel_workers, len(items))
        logger.info(
            f"Parallel extraction: {len(items)} item(s) across "
            f"{actual_workers} worker thread(s)"
        )

        # Map future → original index so we can restore order
        index_map: Dict[object, int] = {}
        results: List[Tuple[str, Optional[SchemaMetadata]]] = [None] * len(items)

        with ThreadPoolExecutor(max_workers=actual_workers) as pool:
            for idx, item in enumerate(items):
                future = pool.submit(worker_fn, item)
                index_map[future] = idx

            for future in as_completed(index_map):
                idx = index_map[future]
                results[idx] = future.result()

        return results

    # ------------------------------------------------------------------ #
    # Public extraction API                                                #
    # ------------------------------------------------------------------ #

    def extract_all_schemas(
        self,
        schema_filter: Optional[List[str]] = None,
        parallel_workers: int = 5,
    ) -> DatabaseMetadata:
        """
        Extract metadata for all schemas in the database.

        Schemas are extracted in parallel using a ``ThreadPoolExecutor``.
        Each worker thread opens its own database connection so threads
        never share state.

        If ``database`` was not specified in the connection config the
        extractor runs in *server mode* and scans every database on the
        server, also in parallel.

        Parameters
        ----------
        schema_filter : list[str] | None
            Which schemas to extract.  Accepts exact names, globs
            (``monkeybook_%``), and ``prefix:``, ``suffix:``,
            ``contains:``, ``regex:`` markers.  Pass ``None`` to extract
            everything.
        parallel_workers : int
            Maximum number of schemas extracted concurrently.  Defaults
            to ``5``.  Set to ``1`` for sequential extraction (useful for
            debugging or connectors that do not support concurrent
            connections).  The value is automatically capped at the
            number of schemas being extracted.

        Returns
        -------
        DatabaseMetadata
        """
        connector = self._get_connector()
        version = connector.get_database_version()
        server_mode = hasattr(connector, 'server_mode') and connector.server_mode

        if server_mode:
            logger.info("Server mode: Extracting from all databases on server")

            db_metadata = DatabaseMetadata(
                database_name=f"{connector.host}:{connector.port}",
                database_type=self.db_type,
                version=version,
                host=self.connection_params.get('host'),
                port=self.connection_params.get('port'),
            )

            all_databases = connector.get_schemas()
            logger.info(f"Found {len(all_databases)} databases on server")

            # Filter databases — supports exact names, globs, prefix:/suffix:/regex: markers
            databases_to_extract = self._resolve_schema_filter(schema_filter, all_databases)

            if not databases_to_extract:
                logger.warning("No databases matched the schema_filter — returning empty result")
                return db_metadata

            results = self._run_parallel(
                self._extract_server_db_worker,
                databases_to_extract,
                parallel_workers,
            )

            for _db_name, db_schema in results:
                if db_schema is not None:
                    db_metadata.add_schema(db_schema)

            logger.info(
                f"Server extraction complete.  "
                f"Total databases: {len(db_metadata.schemas)}"
            )
            return db_metadata

        else:
            # Normal mode: single database specified
            db_metadata = DatabaseMetadata(
                database_name=self.connection_params.get('database', 'unknown'),
                database_type=self.db_type,
                version=version,
                host=self.connection_params.get('host'),
                port=self.connection_params.get('port'),
            )

            all_schemas = connector.get_schemas()
            logger.info(f"Found {len(all_schemas)} schemas in database")

            # Filter schemas — supports exact names, globs, prefix:/suffix:/regex: markers
            schemas_to_extract = self._resolve_schema_filter(schema_filter, all_schemas)

            if not schemas_to_extract:
                logger.warning("No schemas matched the schema_filter — returning empty result")
                return db_metadata

            results = self._run_parallel(
                self._extract_schema_worker,
                schemas_to_extract,
                parallel_workers,
            )

            for _schema_name, schema_metadata in results:
                if schema_metadata is not None:
                    db_metadata.add_schema(schema_metadata)

            logger.info(
                f"Extraction complete.  Total schemas: {len(db_metadata.schemas)}"
            )
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