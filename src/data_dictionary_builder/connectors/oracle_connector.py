"""
Oracle Database connector implementation.

Uses the ``oracledb`` library (python-oracledb) in thin mode — no Oracle
Instant Client installation required.

Install with:

    pip install data-dictionary-builder[oracle]
    # or
    pip install oracledb
"""

import oracledb
from typing import List, Optional, Dict, Any

from .base import BaseConnector
from ..metadata.models import SchemaMetadata, TableMetadata, ColumnMetadata


class OracleConnector(BaseConnector):
    """
    Connector for Oracle Database.

    Terminology mapping
    -------------------
    In Oracle, a *schema* is owned by a *user* — they are the same object.
    The ``database`` parameter maps to Oracle's ``service_name`` (the logical
    database name you connect to).  Schemas (users/owners) are extracted
    within that service.

    Server mode
    -----------
    Omit ``schema`` (handled by MetadataExtractor) to extract **all**
    accessible schemas from ``ALL_USERS``.
    """

    def __init__(
        self,
        host: str,
        port: int = 1521,
        database: str = None,       # maps to Oracle service_name
        user: str = None,
        password: str = None,
        **kwargs,
    ):
        """
        Initialise the Oracle connector.

        Args:
            host:     Oracle server hostname or IP.
            port:     Listener port.  Default: 1521.
            database: Oracle service name (e.g. ``"ORCL"``, ``"XEPDB1"``).
            user:     Oracle username.
            password: Oracle password.
            **kwargs: Extra keyword arguments forwarded to
                      ``oracledb.connect()``, e.g. ``ssl_context``.
        """
        super().__init__(
            host=host, port=port, database=database,
            user=user, password=password, **kwargs,
        )
        self.host         = host
        self.port         = port
        self.service_name = database       # Oracle uses service_name
        self.database     = database
        self.user         = user
        self.password     = password
        self.db_type      = "oracle"
        self.server_mode  = database is None
        self._extra_kwargs = {
            k: v for k, v in kwargs.items()
            if k not in ("host", "port", "database", "user", "password")
        }

    # ── Connection lifecycle ──────────────────────────────────────────────────

    def connect(self) -> None:
        """Open a thin-mode connection to the Oracle server."""
        try:
            self.connection = oracledb.connect(
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
                service_name=self.service_name,
                **self._extra_kwargs,
            )
        except Exception as e:
            raise ConnectionError(f"Failed to connect to Oracle: {e}")

    def disconnect(self) -> None:
        """Close the connection."""
        if self.connection:
            self.connection.close()
            self.connection = None

    # ── Schema listing ────────────────────────────────────────────────────────

    def get_schemas(self) -> List[str]:
        """
        Return accessible schema (user/owner) names, excluding Oracle
        system accounts.
        """
        _system_accounts = {
            "SYS", "SYSTEM", "OUTLN", "DBSNMP", "APPQOSSYS", "DBSFWUSER",
            "GGSYS", "ANONYMOUS", "CTXSYS", "DVSYS", "DVF", "GSMADMIN_INTERNAL",
            "MDSYS", "OLAPSYS", "ORDPLUGINS", "ORDSYS", "SI_INFORMTN_SCHEMA",
            "WMSYS", "XDB", "ORDDATA", "LBACSYS", "OJVMSYS", "ORACLE_OCM",
            "REMOTE_SCHEDULER_AGENT", "SYS$UMF", "SYSBACKUP", "SYSDG",
            "SYSKM", "SYSRAC", "XS$NULL", "FLOWS_FILES", "HR", "OE", "PM",
            "IX", "SH", "BI",
        }
        cursor = self.connection.cursor()
        cursor.execute(
            "SELECT username FROM all_users ORDER BY username"
        )
        schemas = [
            row[0] for row in cursor.fetchall()
            if row[0] not in _system_accounts
        ]
        cursor.close()
        return schemas if schemas else [self.user.upper()]

    # ── Table listing ─────────────────────────────────────────────────────────

    def get_tables(self, schema_name: str) -> List[str]:
        """Return all base-table names owned by *schema_name*."""
        cursor = self.connection.cursor()
        cursor.execute(
            "SELECT table_name FROM all_tables "
            "WHERE owner = :owner "
            "AND (iot_type IS NULL OR iot_type != 'IOT_OVERFLOW') "
            "ORDER BY table_name",
            {"owner": schema_name.upper()},
        )
        tables = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return tables

    # ── Column / key metadata ─────────────────────────────────────────────────

    def get_columns(self, schema_name: str, table_name: str) -> List[ColumnMetadata]:
        """Return column metadata for a single table."""
        cursor = self.connection.cursor()
        cursor.execute(
            """
            SELECT c.column_name, c.data_type, c.nullable,
                   c.data_default, c.char_length,
                   c.data_precision, c.data_scale, c.column_id,
                   cc.comments
            FROM all_tab_columns c
            LEFT JOIN all_col_comments cc
                   ON cc.owner       = c.owner
                  AND cc.table_name  = c.table_name
                  AND cc.column_name = c.column_name
            WHERE c.owner      = :owner
              AND c.table_name = :tbl
            ORDER BY c.column_id
            """,
            {"owner": schema_name.upper(), "tbl": table_name.upper()},
        )
        primary_keys = set(self.get_primary_keys(schema_name, table_name))
        foreign_keys = {fk["column"]: fk for fk in self.get_foreign_keys(schema_name, table_name)}

        columns = []
        for col_name, data_type, nullable, default, char_len, precision, scale, col_id, comment in cursor.fetchall():
            col = ColumnMetadata(
                name=col_name,
                data_type=data_type,
                is_nullable=nullable == "Y",
                is_primary_key=col_name in primary_keys,
                default_value=default.strip() if default else None,
                character_maximum_length=char_len,
                numeric_precision=precision,
                numeric_scale=scale,
                ordinal_position=col_id,
                description=comment or None,
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
            SELECT acc.column_name
            FROM all_constraints  ac
            JOIN all_cons_columns acc
                ON acc.constraint_name = ac.constraint_name
               AND acc.owner           = ac.owner
            WHERE ac.owner           = :owner
              AND ac.table_name      = :tbl
              AND ac.constraint_type = 'P'
            ORDER BY acc.position
            """,
            {"owner": schema_name.upper(), "tbl": table_name.upper()},
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
                acc.column_name,
                rcc.table_name  AS referenced_table,
                rcc.column_name AS referenced_column
            FROM all_constraints  ac
            JOIN all_cons_columns acc
                ON acc.constraint_name = ac.constraint_name
               AND acc.owner           = ac.owner
            JOIN all_constraints  rc
                ON rc.constraint_name = ac.r_constraint_name
               AND rc.owner           = ac.r_owner
            JOIN all_cons_columns rcc
                ON rcc.constraint_name = rc.constraint_name
               AND rcc.owner           = rc.owner
               AND rcc.position        = acc.position
            WHERE ac.owner           = :owner
              AND ac.table_name      = :tbl
              AND ac.constraint_type = 'R'
            ORDER BY acc.position
            """,
            {"owner": schema_name.upper(), "tbl": table_name.upper()},
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
            SELECT t.num_rows, tc.comments
            FROM all_tables      t
            LEFT JOIN all_tab_comments tc
                   ON tc.owner      = t.owner
                  AND tc.table_name = t.table_name
            WHERE t.owner      = :owner
              AND t.table_name = :tbl
            """,
            {"owner": schema_name.upper(), "tbl": table_name.upper()},
        )
        row = cursor.fetchone()
        cursor.close()
        num_rows = row[0] if row else None
        description = row[1] if row else None

        table_meta = TableMetadata(
            name=table_name,
            schema_name=schema_name,
            table_type="BASE TABLE",
            description=description,
            row_count=num_rows,
        )
        for col in self.get_columns(schema_name, table_name):
            table_meta.add_column(col)
        table_meta.primary_keys = self.get_primary_keys(schema_name, table_name)
        return table_meta

    # ── Bulk schema extraction ────────────────────────────────────────────────

    def extract_schema_metadata(self, schema_name: str) -> SchemaMetadata:
        """
        Bulk-optimised schema extraction for Oracle.

        Issues **4 queries** covering the entire schema regardless of table count:
          1. ``ALL_TABLES``  + ``ALL_TAB_COMMENTS``  — table list, row counts, descriptions
          2. ``ALL_TAB_COLUMNS`` + ``ALL_COL_COMMENTS`` — all columns with descriptions
          3. ``ALL_CONSTRAINTS`` + ``ALL_CONS_COLUMNS`` — primary keys
          4. Same views joined for referential (foreign key) constraints
        """
        owner = schema_name.upper()
        cursor = self.connection.cursor()

        # ── Query 1: tables ───────────────────────────────────────────────────
        cursor.execute(
            """
            SELECT t.table_name, t.num_rows, tc.comments
            FROM all_tables t
            LEFT JOIN all_tab_comments tc
                   ON tc.owner = t.owner AND tc.table_name = t.table_name
            WHERE t.owner = :owner
              AND (t.iot_type IS NULL OR t.iot_type != 'IOT_OVERFLOW')
            ORDER BY t.table_name
            """,
            {"owner": owner},
        )
        tables_rows = cursor.fetchall()
        if not tables_rows:
            cursor.close()
            return SchemaMetadata(name=schema_name)

        table_names = [r[0] for r in tables_rows]
        # Oracle bind lists require a slightly different approach
        placeholders = ",".join([f":n{i}" for i in range(len(table_names))])
        bind_map     = {f"n{i}": n for i, n in enumerate(table_names)}
        bind_map["owner"] = owner

        # ── Query 2: all columns ──────────────────────────────────────────────
        cursor.execute(
            f"""
            SELECT c.table_name, c.column_name, c.data_type, c.nullable,
                   c.data_default, c.char_length,
                   c.data_precision, c.data_scale, c.column_id,
                   cc.comments
            FROM all_tab_columns c
            LEFT JOIN all_col_comments cc
                   ON cc.owner       = c.owner
                  AND cc.table_name  = c.table_name
                  AND cc.column_name = c.column_name
            WHERE c.owner IN (:owner)
              AND c.table_name IN ({placeholders})
            ORDER BY c.table_name, c.column_id
            """,
            bind_map,
        )
        cols_by_table: Dict[str, list] = {}
        for row in cursor.fetchall():
            cols_by_table.setdefault(row[0], []).append(row)

        # ── Query 3: primary keys ─────────────────────────────────────────────
        cursor.execute(
            f"""
            SELECT acc.table_name, acc.column_name
            FROM all_constraints  ac
            JOIN all_cons_columns acc
                ON acc.constraint_name = ac.constraint_name
               AND acc.owner           = ac.owner
            WHERE ac.owner            = :owner
              AND ac.table_name       IN ({placeholders})
              AND ac.constraint_type  = 'P'
            ORDER BY acc.table_name, acc.position
            """,
            bind_map,
        )
        pks_by_table: Dict[str, set] = {}
        for tbl, col in cursor.fetchall():
            pks_by_table.setdefault(tbl, set()).add(col)

        # ── Query 4: foreign keys ─────────────────────────────────────────────
        cursor.execute(
            f"""
            SELECT
                acc.table_name,
                acc.column_name,
                rcc.table_name  AS ref_table,
                rcc.column_name AS ref_column
            FROM all_constraints  ac
            JOIN all_cons_columns acc
                ON acc.constraint_name = ac.constraint_name
               AND acc.owner           = ac.owner
            JOIN all_constraints  rc
                ON rc.constraint_name = ac.r_constraint_name
               AND rc.owner           = ac.r_owner
            JOIN all_cons_columns rcc
                ON rcc.constraint_name = rc.constraint_name
               AND rcc.owner           = rc.owner
               AND rcc.position        = acc.position
            WHERE ac.owner           = :owner
              AND ac.table_name      IN ({placeholders})
              AND ac.constraint_type = 'R'
            ORDER BY acc.table_name, acc.position
            """,
            bind_map,
        )
        fks_by_table: Dict[str, Dict[str, Any]] = {}
        for tbl, col, ref_tbl, ref_col in cursor.fetchall():
            fks_by_table.setdefault(tbl, {})[col] = {
                "referenced_table": ref_tbl,
                "referenced_column": ref_col,
            }

        cursor.close()

        # ── Assemble SchemaMetadata ───────────────────────────────────────────
        schema_metadata = SchemaMetadata(name=schema_name)

        for table_name, num_rows, tbl_comment in tables_rows:
            pk_set  = pks_by_table.get(table_name, set())
            fk_map  = fks_by_table.get(table_name, {})

            table_meta = TableMetadata(
                name=table_name,
                schema_name=schema_name,
                table_type="BASE TABLE",
                description=tbl_comment or None,
                row_count=num_rows,
            )
            table_meta.primary_keys = list(pk_set)

            for (_, col_name, data_type, nullable, default, char_len,
                 precision, scale, col_id, col_comment) in cols_by_table.get(table_name, []):
                col = ColumnMetadata(
                    name=col_name,
                    data_type=data_type,
                    is_nullable=nullable == "Y",
                    is_primary_key=col_name in pk_set,
                    default_value=default.strip() if default else None,
                    character_maximum_length=char_len,
                    numeric_precision=precision,
                    numeric_scale=scale,
                    ordinal_position=col_id,
                    description=col_comment or None,
                )
                if col_name in fk_map:
                    fk = fk_map[col_name]
                    col.is_foreign_key       = True
                    col.foreign_key_table    = fk["referenced_table"]
                    col.foreign_key_column   = fk["referenced_column"]
                table_meta.add_column(col)

            schema_metadata.add_table(table_meta)

        return schema_metadata

    # ── Utility ───────────────────────────────────────────────────────────────

    def get_database_version(self) -> Optional[str]:
        """Return the Oracle server version string."""
        cursor = self.connection.cursor()
        cursor.execute("SELECT banner FROM v$version WHERE ROWNUM = 1")
        row = cursor.fetchone()
        cursor.close()
        return row[0] if row else None

    def get_table_row_count(self, schema_name: str, table_name: str) -> Optional[int]:
        """Return the last-analysed row count from ``ALL_TABLES``."""
        try:
            cursor = self.connection.cursor()
            cursor.execute(
                "SELECT num_rows FROM all_tables "
                "WHERE owner = :owner AND table_name = :tbl",
                {"owner": schema_name.upper(), "tbl": table_name.upper()},
            )
            row = cursor.fetchone()
            cursor.close()
            return row[0] if row else None
        except Exception:
            return None
