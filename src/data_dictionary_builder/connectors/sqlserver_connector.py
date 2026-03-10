"""
SQL Server database connector implementation.

Uses the ``pymssql`` library for pure-Python connectivity — no ODBC driver
installation required.

Install with:

    pip install data-dictionary-builder[sqlserver]
    # or
    pip install pymssql
"""

import pymssql
from typing import List, Optional, Dict, Any

from .base import BaseConnector
from ..metadata.models import SchemaMetadata, TableMetadata, ColumnMetadata


class SQLServerConnector(BaseConnector):
    """Connector for Microsoft SQL Server (and Azure SQL Database)."""

    def __init__(
        self,
        host: str,
        port: int = 1433,
        database: str = None,
        user: str = None,
        password: str = None,
        **kwargs,
    ):
        """
        Initialise the SQL Server connector.

        Args:
            host:     SQL Server hostname, IP, or ``host\\instance`` string.
            port:     TCP port.  Default: 1433.
            database: Database name.  Omit (or pass ``None``) for server-mode
                      scanning (lists all user databases on the server).
            user:     Login name.
            password: Login password.
            **kwargs: Extra keyword arguments forwarded to ``pymssql.connect()``,
                      e.g. ``tds_version``, ``charset``.
        """
        connect_database = database if database else "master"
        super().__init__(
            host=host, port=port, database=connect_database,
            user=user, password=password, **kwargs,
        )
        self.host             = host
        self.port             = port
        self.database         = database          # original (may be None)
        self.connect_database = connect_database  # used for connection
        self.user             = user
        self.password         = password
        self.db_type          = "sqlserver"
        self.server_mode      = database is None
        self._extra_kwargs = {
            k: v for k, v in kwargs.items()
            if k not in ("host", "port", "database", "user", "password")
        }

    # ── Connection lifecycle ──────────────────────────────────────────────────

    def connect(self) -> None:
        """Open a connection to SQL Server."""
        try:
            self.connection = pymssql.connect(
                server=self.host,
                port=self.port,
                database=self.connect_database,
                user=self.user,
                password=self.password,
                **self._extra_kwargs,
            )
        except Exception as e:
            raise ConnectionError(f"Failed to connect to SQL Server: {e}")

    def disconnect(self) -> None:
        """Close the connection."""
        if self.connection:
            self.connection.close()
            self.connection = None

    def switch_database(self, database_name: str) -> None:
        """Reconnect to a different database on the same server (server mode)."""
        if self.server_mode and self.connection:
            self.connection.close()
        self.connect_database = database_name
        self.connect()

    # ── Schema / table listing ────────────────────────────────────────────────

    def get_schemas(self) -> List[str]:
        """
        Return schema names.

        * **Server mode** (``database=None``): returns all user databases on
          the server (excludes ``master``, ``tempdb``, ``model``, ``msdb``).
        * **Database mode**: returns all schemas in the current database,
          excluding ``sys``, ``INFORMATION_SCHEMA``, ``guest``, and
          ``db_*`` fixed-database roles.
        """
        cursor = self.connection.cursor()
        if self.server_mode:
            cursor.execute(
                "SELECT name FROM sys.databases "
                "WHERE name NOT IN ('master','tempdb','model','msdb') "
                "  AND state_desc = 'ONLINE' "
                "ORDER BY name"
            )
            schemas = [row[0] for row in cursor.fetchall()]
        else:
            cursor.execute(
                "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA "
                "WHERE SCHEMA_NAME NOT IN ('sys','INFORMATION_SCHEMA','guest') "
                "  AND SCHEMA_NAME NOT LIKE 'db[_]%' "
                "ORDER BY SCHEMA_NAME"
            )
            schemas = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return schemas

    def get_tables(self, schema_name: str) -> List[str]:
        """Return all base-table names in *schema_name*."""
        cursor = self.connection.cursor()
        cursor.execute(
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE' "
            "ORDER BY TABLE_NAME",
            (schema_name,),
        )
        tables = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return tables

    # ── Column / key metadata ─────────────────────────────────────────────────

    def get_columns(self, schema_name: str, table_name: str) -> List[ColumnMetadata]:
        """Return column metadata for a single table."""
        cursor = self.connection.cursor(as_dict=True)
        cursor.execute(
            """
            SELECT
                c.COLUMN_NAME,
                c.DATA_TYPE,
                c.IS_NULLABLE,
                c.COLUMN_DEFAULT,
                c.CHARACTER_MAXIMUM_LENGTH,
                c.NUMERIC_PRECISION,
                c.NUMERIC_SCALE,
                c.ORDINAL_POSITION,
                CAST(ep.value AS NVARCHAR(MAX)) AS description
            FROM INFORMATION_SCHEMA.COLUMNS c
            LEFT JOIN sys.extended_properties ep
                ON ep.major_id   = OBJECT_ID(c.TABLE_SCHEMA + '.' + c.TABLE_NAME)
               AND ep.minor_id   = c.ORDINAL_POSITION
               AND ep.name       = 'MS_Description'
               AND ep.class_desc = 'OBJECT_OR_COLUMN'
            WHERE c.TABLE_SCHEMA = %s AND c.TABLE_NAME = %s
            ORDER BY c.ORDINAL_POSITION
            """,
            (schema_name, table_name),
        )
        primary_keys = set(self.get_primary_keys(schema_name, table_name))
        foreign_keys = {fk["column"]: fk for fk in self.get_foreign_keys(schema_name, table_name)}

        columns = []
        for row in cursor.fetchall():
            col_name = row["COLUMN_NAME"]
            col = ColumnMetadata(
                name=col_name,
                data_type=row["DATA_TYPE"],
                is_nullable=row["IS_NULLABLE"] == "YES",
                is_primary_key=col_name in primary_keys,
                default_value=row["COLUMN_DEFAULT"],
                character_maximum_length=row["CHARACTER_MAXIMUM_LENGTH"],
                numeric_precision=row["NUMERIC_PRECISION"],
                numeric_scale=row["NUMERIC_SCALE"],
                ordinal_position=row["ORDINAL_POSITION"],
                description=row.get("description") or None,
            )
            if col_name in foreign_keys:
                fk = foreign_keys[col_name]
                col.is_foreign_key       = True
                col.foreign_key_table    = fk["referenced_table"]
                col.foreign_key_column   = fk["referenced_column"]
            columns.append(col)
        cursor.close()
        return columns

    def get_primary_keys(self, schema_name: str, table_name: str) -> List[str]:
        """Return primary-key column names for *table_name*."""
        cursor = self.connection.cursor()
        cursor.execute(
            """
            SELECT kcu.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS  tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE   kcu
                ON kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
               AND kcu.TABLE_SCHEMA    = tc.TABLE_SCHEMA
               AND kcu.TABLE_NAME      = tc.TABLE_NAME
            WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
              AND tc.TABLE_SCHEMA    = %s
              AND tc.TABLE_NAME      = %s
            ORDER BY kcu.ORDINAL_POSITION
            """,
            (schema_name, table_name),
        )
        pks = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return pks

    def get_foreign_keys(self, schema_name: str, table_name: str) -> List[Dict[str, str]]:
        """Return foreign-key relationships for *table_name*."""
        cursor = self.connection.cursor()
        cursor.execute(
            """
            SELECT
                kcu.COLUMN_NAME,
                kcu2.TABLE_NAME  AS referenced_table,
                kcu2.COLUMN_NAME AS referenced_column
            FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                ON kcu.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
               AND kcu.TABLE_SCHEMA    = %s
               AND kcu.TABLE_NAME      = %s
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu2
                ON kcu2.CONSTRAINT_NAME  = rc.UNIQUE_CONSTRAINT_NAME
               AND kcu2.ORDINAL_POSITION = kcu.ORDINAL_POSITION
            ORDER BY kcu.ORDINAL_POSITION
            """,
            (schema_name, table_name),
        )
        fks = [
            {"column": row[0], "referenced_table": row[1], "referenced_column": row[2]}
            for row in cursor.fetchall()
        ]
        cursor.close()
        return fks

    # ── Single-table extraction ───────────────────────────────────────────────

    def get_table_metadata(self, schema_name: str, table_name: str) -> TableMetadata:
        """Return complete metadata for a single table."""
        cursor = self.connection.cursor()
        cursor.execute(
            """
            SELECT CAST(ep.value AS NVARCHAR(MAX))
            FROM sys.extended_properties ep
            WHERE ep.major_id   = OBJECT_ID(%s + '.' + %s)
              AND ep.minor_id   = 0
              AND ep.name       = 'MS_Description'
              AND ep.class_desc = 'OBJECT_OR_COLUMN'
            """,
            (schema_name, table_name),
        )
        row = cursor.fetchone()
        description = row[0] if row else None
        cursor.close()

        table_meta = TableMetadata(
            name=table_name,
            schema_name=schema_name,
            table_type="BASE TABLE",
            description=description,
            row_count=self.get_table_row_count(schema_name, table_name),
        )
        for col in self.get_columns(schema_name, table_name):
            table_meta.add_column(col)
        table_meta.primary_keys = self.get_primary_keys(schema_name, table_name)
        return table_meta

    # ── Bulk schema extraction ────────────────────────────────────────────────

    def extract_schema_metadata(self, schema_name: str) -> SchemaMetadata:
        """
        Bulk-optimised schema extraction for SQL Server.

        Issues **5 queries** covering the entire schema regardless of table count:
          1. Table list + ``MS_Description`` extended properties (table level)
          2. Approximate row counts from ``sys.dm_db_partition_stats``
          3. All columns + column-level ``MS_Description`` extended properties
          4. Primary keys
          5. Foreign keys
        """
        cursor = self.connection.cursor(as_dict=True)

        # ── Query 1: tables + table-level descriptions ────────────────────────
        cursor.execute(
            """
            SELECT
                t.name AS table_name,
                CAST(ep.value AS NVARCHAR(MAX)) AS description
            FROM sys.tables t
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            LEFT JOIN sys.extended_properties ep
                ON ep.major_id   = t.object_id
               AND ep.minor_id   = 0
               AND ep.name       = 'MS_Description'
               AND ep.class_desc = 'OBJECT_OR_COLUMN'
            WHERE s.name = %s
            ORDER BY t.name
            """,
            (schema_name,),
        )
        tables_rows = cursor.fetchall()
        if not tables_rows:
            cursor.close()
            return SchemaMetadata(name=schema_name)

        table_names = [r["table_name"] for r in tables_rows]

        # pymssql does not support array binding; use a temporary table approach
        # or build the IN clause from string formatting (table names are from
        # sys.tables — a trusted source, so inlining is safe here).
        in_clause = ",".join(f"'{t}'" for t in table_names)

        # ── Query 2: row counts ───────────────────────────────────────────────
        cursor.execute(
            f"""
            SELECT t.name AS table_name, SUM(p.row_count) AS row_count
            FROM sys.dm_db_partition_stats p
            JOIN sys.objects               o ON o.object_id = p.object_id
            JOIN sys.schemas               s ON s.schema_id = o.schema_id
            JOIN sys.tables                t ON t.object_id = o.object_id
            WHERE s.name      = %s
              AND t.name      IN ({in_clause})
              AND p.index_id  < 2
            GROUP BY t.name
            """,
            (schema_name,),
        )
        row_counts: Dict[str, int] = {r["table_name"]: r["row_count"] for r in cursor.fetchall()}

        # ── Query 3: all columns ──────────────────────────────────────────────
        cursor.execute(
            f"""
            SELECT
                c.TABLE_NAME,
                c.COLUMN_NAME,
                c.DATA_TYPE,
                c.IS_NULLABLE,
                c.COLUMN_DEFAULT,
                c.CHARACTER_MAXIMUM_LENGTH,
                c.NUMERIC_PRECISION,
                c.NUMERIC_SCALE,
                c.ORDINAL_POSITION,
                CAST(ep.value AS NVARCHAR(MAX)) AS description
            FROM INFORMATION_SCHEMA.COLUMNS c
            LEFT JOIN sys.extended_properties ep
                ON ep.major_id   = OBJECT_ID(c.TABLE_SCHEMA + '.' + c.TABLE_NAME)
               AND ep.minor_id   = c.ORDINAL_POSITION
               AND ep.name       = 'MS_Description'
               AND ep.class_desc = 'OBJECT_OR_COLUMN'
            WHERE c.TABLE_SCHEMA = %s
              AND c.TABLE_NAME   IN ({in_clause})
            ORDER BY c.TABLE_NAME, c.ORDINAL_POSITION
            """,
            (schema_name,),
        )
        cols_by_table: Dict[str, list] = {}
        for row in cursor.fetchall():
            cols_by_table.setdefault(row["TABLE_NAME"], []).append(row)

        # ── Query 4: primary keys ─────────────────────────────────────────────
        cursor.execute(
            f"""
            SELECT kcu.TABLE_NAME, kcu.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS  tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE   kcu
                ON kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
               AND kcu.TABLE_SCHEMA    = tc.TABLE_SCHEMA
            WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
              AND tc.TABLE_SCHEMA    = %s
              AND tc.TABLE_NAME      IN ({in_clause})
            ORDER BY kcu.TABLE_NAME, kcu.ORDINAL_POSITION
            """,
            (schema_name,),
        )
        pks_by_table: Dict[str, set] = {}
        for row in cursor.fetchall():
            pks_by_table.setdefault(row["TABLE_NAME"], set()).add(row["COLUMN_NAME"])

        # ── Query 5: foreign keys ─────────────────────────────────────────────
        cursor.execute(
            f"""
            SELECT
                kcu.TABLE_NAME,
                kcu.COLUMN_NAME,
                kcu2.TABLE_NAME  AS referenced_table,
                kcu2.COLUMN_NAME AS referenced_column
            FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                ON kcu.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
               AND kcu.TABLE_SCHEMA    = %s
               AND kcu.TABLE_NAME      IN ({in_clause})
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu2
                ON kcu2.CONSTRAINT_NAME  = rc.UNIQUE_CONSTRAINT_NAME
               AND kcu2.ORDINAL_POSITION = kcu.ORDINAL_POSITION
            ORDER BY kcu.TABLE_NAME, kcu.ORDINAL_POSITION
            """,
            (schema_name,),
        )
        fks_by_table: Dict[str, Dict[str, Any]] = {}
        for row in cursor.fetchall():
            fks_by_table.setdefault(row["TABLE_NAME"], {})[row["COLUMN_NAME"]] = {
                "referenced_table":  row["referenced_table"],
                "referenced_column": row["referenced_column"],
            }

        cursor.close()

        # ── Assemble SchemaMetadata ───────────────────────────────────────────
        schema_metadata = SchemaMetadata(name=schema_name)

        for trow in tables_rows:
            table_name = trow["table_name"]
            pk_set  = pks_by_table.get(table_name, set())
            fk_map  = fks_by_table.get(table_name, {})

            table_meta = TableMetadata(
                name=table_name,
                schema_name=schema_name,
                table_type="BASE TABLE",
                description=trow.get("description") or None,
                row_count=row_counts.get(table_name),
            )
            table_meta.primary_keys = list(pk_set)

            for col in cols_by_table.get(table_name, []):
                col_name = col["COLUMN_NAME"]
                cm = ColumnMetadata(
                    name=col_name,
                    data_type=col["DATA_TYPE"],
                    is_nullable=col["IS_NULLABLE"] == "YES",
                    is_primary_key=col_name in pk_set,
                    default_value=col["COLUMN_DEFAULT"],
                    character_maximum_length=col["CHARACTER_MAXIMUM_LENGTH"],
                    numeric_precision=col["NUMERIC_PRECISION"],
                    numeric_scale=col["NUMERIC_SCALE"],
                    ordinal_position=col["ORDINAL_POSITION"],
                    description=col.get("description") or None,
                )
                if col_name in fk_map:
                    fk = fk_map[col_name]
                    cm.is_foreign_key       = True
                    cm.foreign_key_table    = fk["referenced_table"]
                    cm.foreign_key_column   = fk["referenced_column"]
                table_meta.add_column(cm)

            schema_metadata.add_table(table_meta)

        return schema_metadata

    # ── Utility ───────────────────────────────────────────────────────────────

    def get_database_version(self) -> Optional[str]:
        """Return the SQL Server version string."""
        cursor = self.connection.cursor()
        cursor.execute("SELECT @@VERSION")
        row = cursor.fetchone()
        cursor.close()
        return row[0].split("\n")[0] if row else None

    def get_table_row_count(self, schema_name: str, table_name: str) -> Optional[int]:
        """Return approximate row count from ``sys.dm_db_partition_stats``."""
        try:
            cursor = self.connection.cursor()
            cursor.execute(
                """
                SELECT SUM(p.row_count)
                FROM sys.dm_db_partition_stats p
                JOIN sys.objects o ON o.object_id = p.object_id
                JOIN sys.schemas s ON s.schema_id = o.schema_id
                WHERE s.name = %s AND o.name = %s AND p.index_id < 2
                """,
                (schema_name, table_name),
            )
            row = cursor.fetchone()
            cursor.close()
            return row[0] if row else None
        except Exception:
            return None
