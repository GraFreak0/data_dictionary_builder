"""
Metadata extractor for extracting database metadata.
"""

import fnmatch
import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial
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

# column_exclude accepts the same entry formats as schema_filter.
# Columns whose names match ANY entry are excluded from the output.
#   Examples:
#       ["peerdb_synced_at", "contains:peerdb", "prefix:_dlt", "suffix:_tmp"]
ColumnExcludeSpec = Optional[List[str]]

# table_exclude — same format as column_exclude, applied to table names.
#   Examples:
#       ["contains:staging", "prefix:tmp_", "regex:^_.*$"]
TableExcludeSpec = Optional[List[str]]

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

        # Pre-compile all regex patterns and prepare glob strings once so
        # the inner loop (over available_schemas) does no repeated work.
        compiled: list = []
        for entry in schema_filter:
            entry_lower = entry.lower()
            if entry_lower.startswith("prefix:"):
                compiled.append(("prefix", entry[len("prefix:"):].lower()))
            elif entry_lower.startswith("suffix:"):
                compiled.append(("suffix", entry[len("suffix:"):].lower()))
            elif entry_lower.startswith("contains:"):
                compiled.append(("contains", entry[len("contains:"):].lower()))
            elif entry_lower.startswith("regex:"):
                compiled.append(("regex", re.compile(entry[len("regex:"):], re.IGNORECASE)))
            elif any(c in entry for c in ("_", "%", "*", "?")):
                glob = entry.replace("%", "*")
                if "*" in glob:
                    glob = glob.replace("_", "?")
                compiled.append(("glob", glob.lower()))
            else:
                compiled.append(("exact", entry))

        for kind, value in compiled:
            if kind == "exact":
                if value in available_schemas:
                    _add(value)
            else:
                for s in available_schemas:
                    s_lower = s.lower()
                    if kind == "prefix":
                        if s_lower.startswith(value):
                            _add(s)
                    elif kind == "suffix":
                        if s_lower.endswith(value):
                            _add(s)
                    elif kind == "contains":
                        if value in s_lower:
                            _add(s)
                    elif kind == "regex":
                        if value.fullmatch(s):
                            _add(s)
                    elif kind == "glob":
                        if fnmatch.fnmatchcase(s_lower, value):
                            _add(s)

        logger.info(
            f"schema_filter {schema_filter!r} matched "
            f"{len(matched)} schema(s): {matched}"
        )
        return matched

    # ------------------------------------------------------------------ #
    # Column-exclusion helpers                                             #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _column_is_excluded(column_name: str, column_exclude: List[str]) -> bool:
        """
        Return True if *column_name* matches any pattern in *column_exclude*.

        Supports the same entry formats as schema_filter:
            exact name      "peerdb_synced_at"
            glob/wildcard   "peerdb_%"  or  "peerdb_*"
            prefix:         "prefix:peerdb"
            suffix:         "suffix:_at"
            contains:       "contains:peerdb"
            regex:          "regex:^_dlt_.*$"
        """
        name_lower = column_name.lower()
        for entry in column_exclude:
            entry_lower = entry.lower()
            if entry_lower.startswith("prefix:"):
                if name_lower.startswith(entry[len("prefix:"):].lower()):
                    return True
            elif entry_lower.startswith("suffix:"):
                if name_lower.endswith(entry[len("suffix:"):].lower()):
                    return True
            elif entry_lower.startswith("contains:"):
                if entry[len("contains:"):].lower() in name_lower:
                    return True
            elif entry_lower.startswith("regex:"):
                if re.fullmatch(entry[len("regex:"):], column_name, re.IGNORECASE):
                    return True
            elif any(c in entry for c in ("%", "*", "?")):
                glob = entry.replace("%", "*").lower()
                if fnmatch.fnmatchcase(name_lower, glob):
                    return True
            else:
                # Exact match (case-insensitive)
                if name_lower == entry_lower:
                    return True
        return False

    def _apply_column_exclusions(
        self,
        schema_metadata,
        column_exclude: List[str],
    ):
        """
        Remove columns matching any *column_exclude* pattern from every table
        in *schema_metadata* (mutates in place and returns the object).
        """
        excluded_total = 0
        for table in schema_metadata.tables:
            before = len(table.columns)
            table.columns = [
                col for col in table.columns
                if not self._column_is_excluded(col.name, column_exclude)
            ]
            excluded_total += before - len(table.columns)
        if excluded_total:
            logger.info(
                f"column_exclude {column_exclude!r} removed "
                f"{excluded_total} column(s) from schema '{schema_metadata.name}'"
            )
        return schema_metadata

    # ------------------------------------------------------------------ #
    # Table-exclusion helpers                                              #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _table_is_excluded(table_name: str, table_exclude: List[str]) -> bool:
        """
        Return True if *table_name* matches any pattern in *table_exclude*.

        Supports the same entry formats as schema_filter and column_exclude:
            exact name      "raw_events"
            glob/wildcard   "tmp_%"  or  "tmp_*"
            prefix:         "prefix:tmp_"
            suffix:         "suffix:_staging"
            contains:       "contains:staging"
            regex:          "regex:^_.*$"
        """
        name_lower = table_name.lower()
        for entry in table_exclude:
            entry_lower = entry.lower()
            if entry_lower.startswith("prefix:"):
                if name_lower.startswith(entry[len("prefix:"):].lower()):
                    return True
            elif entry_lower.startswith("suffix:"):
                if name_lower.endswith(entry[len("suffix:"):].lower()):
                    return True
            elif entry_lower.startswith("contains:"):
                if entry[len("contains:"):].lower() in name_lower:
                    return True
            elif entry_lower.startswith("regex:"):
                if re.fullmatch(entry[len("regex:"):], table_name, re.IGNORECASE):
                    return True
            elif any(c in entry for c in ("%", "*", "?")):
                glob = entry.replace("%", "*").lower()
                if fnmatch.fnmatchcase(name_lower, glob):
                    return True
            else:
                if name_lower == entry_lower:
                    return True
        return False

    def _apply_table_exclusions(
        self,
        schema_metadata,
        table_exclude: List[str],
    ):
        """
        Remove tables matching any *table_exclude* pattern from
        *schema_metadata* (mutates in place and returns the object).
        """
        before = len(schema_metadata.tables)
        schema_metadata.tables = [
            t for t in schema_metadata.tables
            if not self._table_is_excluded(t.name, table_exclude)
        ]
        removed = before - len(schema_metadata.tables)
        if removed:
            logger.info(
                f"table_exclude {table_exclude!r} removed "
                f"{removed} table(s) from schema '{schema_metadata.name}'"
            )
        return schema_metadata

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
    
    def extract_schema(
        self,
        schema_name: str,
        column_exclude: "ColumnExcludeSpec" = None,
        table_exclude: "TableExcludeSpec" = None,
        include_views: bool = False,
    ) -> SchemaMetadata:
        """
        Extract metadata for a single schema.

        Args:
            schema_name: Name of the schema to extract
            column_exclude: Column exclusion patterns (see ColumnExcludeSpec).
            table_exclude: Table exclusion patterns (see TableExcludeSpec).
            include_views: When True, views are included alongside base tables.

        Returns:
            SchemaMetadata object
        """
        connector = self._get_connector()

        logger.info(f"Extracting metadata for schema: {schema_name}")
        schema_metadata = connector.extract_schema_metadata(
            schema_name,
            include_views=include_views,
        )
        logger.info(f"Extracted {len(schema_metadata.tables)} tables from schema: {schema_name}")

        if table_exclude:
            self._apply_table_exclusions(schema_metadata, table_exclude)
        if column_exclude:
            self._apply_column_exclusions(schema_metadata, column_exclude)

        return schema_metadata
    
    # ------------------------------------------------------------------ #
    # Parallel extraction helpers                                          #
    # ------------------------------------------------------------------ #

    def _extract_schema_worker(
        self,
        schema_name: str,
        include_views: bool = False,
    ) -> Tuple[str, Optional[SchemaMetadata]]:
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
                schema_metadata = worker_connector.extract_schema_metadata(
                    schema_name,
                    include_views=include_views,
                )
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

    def _extract_server_db_worker(
        self,
        db_name: str,
        include_views: bool = False,
    ) -> Tuple[str, Optional[SchemaMetadata]]:
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
                    if include_views:
                        for view_name in worker_connector.get_views(schema_name):
                            view_metadata = worker_connector.get_table_metadata(schema_name, view_name)
                            view_metadata.table_type = "VIEW"
                            view_metadata.name = f"{schema_name}.{view_name}"
                            db_schema.add_table(view_metadata)

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
        column_exclude: "ColumnExcludeSpec" = None,
        table_exclude: "TableExcludeSpec" = None,
        include_views: bool = False,
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
        column_exclude : list[str] | None
            Column exclusion patterns.  Columns whose names match ANY
            entry are omitted from the output.  Accepts the same formats
            as ``schema_filter``: exact names, globs, and
            ``prefix:``, ``suffix:``, ``contains:``, ``regex:`` markers.
            Example: ``["contains:peerdb", "prefix:_dlt_", "suffix:_tmp"]``
        table_exclude : list[str] | None
            Table exclusion patterns.  Tables whose names match ANY entry
            are omitted from the output.  Accepts the same formats as
            ``schema_filter``.
            Example: ``["prefix:tmp_", "contains:staging", "suffix:_old"]``
        include_views : bool
            When ``True``, database views are extracted alongside base
            tables.  Defaults to ``False``.

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

            worker = partial(self._extract_server_db_worker, include_views=include_views)
            results = self._run_parallel(worker, databases_to_extract, parallel_workers)

            for _db_name, db_schema in results:
                if db_schema is not None:
                    if table_exclude:
                        self._apply_table_exclusions(db_schema, table_exclude)
                    if column_exclude:
                        self._apply_column_exclusions(db_schema, column_exclude)
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

            worker = partial(self._extract_schema_worker, include_views=include_views)
            results = self._run_parallel(worker, schemas_to_extract, parallel_workers)

            for _schema_name, schema_metadata in results:
                if schema_metadata is not None:
                    if table_exclude:
                        self._apply_table_exclusions(schema_metadata, table_exclude)
                    if column_exclude:
                        self._apply_column_exclusions(schema_metadata, column_exclude)
                    db_metadata.add_schema(schema_metadata)

            logger.info(
                f"Extraction complete.  Total schemas: {len(db_metadata.schemas)}"
            )
            return db_metadata
    
    def extract_table(
        self,
        schema_name: str,
        table_name: str,
        column_exclude: "ColumnExcludeSpec" = None,
    ):
        """
        Extract metadata for a single table.

        Args:
            schema_name: Name of the schema
            table_name: Name of the table
            column_exclude: Column exclusion patterns (see ColumnExcludeSpec).

        Returns:
            TableMetadata object
        """
        connector = self._get_connector()

        logger.info(f"Extracting metadata for table: {schema_name}.{table_name}")
        table_metadata = connector.get_table_metadata(schema_name, table_name)

        if column_exclude:
            table_metadata.columns = [
                col for col in table_metadata.columns
                if not self._column_is_excluded(col.name, column_exclude)
            ]

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