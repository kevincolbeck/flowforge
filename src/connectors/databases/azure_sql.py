"""
Azure SQL Database Connector

Connect to Azure SQL Database for data operations.
"""

from typing import Any
from ..base import BaseConnector, ConnectorResult


class AzureSQLConnector(BaseConnector):
    """Connector for Azure SQL Database."""

    def __init__(self, credentials: dict[str, Any]):
        super().__init__(credentials)
        self.server = credentials.get("server")  # yourserver.database.windows.net
        self.database = credentials.get("database")
        self.username = credentials.get("username")
        self.password = credentials.get("password")
        self.port = credentials.get("port", 1433)
        self._connection = None

    async def _get_connection(self):
        """Get or create database connection."""
        if self._connection is None:
            try:
                import aioodbc
                dsn = (
                    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                    f"SERVER={self.server},{self.port};"
                    f"DATABASE={self.database};"
                    f"UID={self.username};"
                    f"PWD={self.password};"
                    f"Encrypt=yes;TrustServerCertificate=no;"
                )
                self._connection = await aioodbc.connect(dsn=dsn)
            except ImportError:
                # Fallback to pyodbc if aioodbc not available
                import pyodbc
                dsn = (
                    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                    f"SERVER={self.server},{self.port};"
                    f"DATABASE={self.database};"
                    f"UID={self.username};"
                    f"PWD={self.password};"
                    f"Encrypt=yes;TrustServerCertificate=no;"
                )
                self._connection = pyodbc.connect(dsn)
        return self._connection

    @classmethod
    def get_actions(cls) -> dict[str, dict[str, Any]]:
        return {
            "insert": {
                "description": "Insert a record into a table",
                "parameters": {
                    "table": {"type": "string", "description": "Table name", "required": True},
                    "data": {"type": "object", "description": "Column-value pairs to insert", "required": True},
                },
            },
            "insert_many": {
                "description": "Insert multiple records into a table",
                "parameters": {
                    "table": {"type": "string", "description": "Table name", "required": True},
                    "records": {"type": "array", "description": "Array of records to insert", "required": True},
                },
            },
            "update": {
                "description": "Update records in a table",
                "parameters": {
                    "table": {"type": "string", "description": "Table name", "required": True},
                    "data": {"type": "object", "description": "Column-value pairs to update", "required": True},
                    "where": {"type": "string", "description": "WHERE clause (without WHERE keyword)", "required": True},
                },
            },
            "delete": {
                "description": "Delete records from a table",
                "parameters": {
                    "table": {"type": "string", "description": "Table name", "required": True},
                    "where": {"type": "string", "description": "WHERE clause (without WHERE keyword)", "required": True},
                },
            },
            "query": {
                "description": "Execute a SELECT query",
                "parameters": {
                    "sql": {"type": "string", "description": "SQL SELECT query", "required": True},
                    "params": {"type": "array", "description": "Query parameters", "required": False},
                },
            },
            "execute": {
                "description": "Execute a stored procedure or custom SQL",
                "parameters": {
                    "sql": {"type": "string", "description": "SQL to execute", "required": True},
                    "params": {"type": "array", "description": "Query parameters", "required": False},
                },
            },
            "upsert": {
                "description": "Insert or update a record (MERGE)",
                "parameters": {
                    "table": {"type": "string", "description": "Table name", "required": True},
                    "data": {"type": "object", "description": "Column-value pairs", "required": True},
                    "key_columns": {"type": "array", "description": "Columns to match on", "required": True},
                },
            },
            "bulk_insert": {
                "description": "Bulk insert from CSV or JSON data",
                "parameters": {
                    "table": {"type": "string", "description": "Table name", "required": True},
                    "data": {"type": "array", "description": "Array of records", "required": True},
                    "batch_size": {"type": "integer", "description": "Batch size (default 1000)", "required": False},
                },
            },
            "list_tables": {
                "description": "List all tables in the database",
                "parameters": {},
            },
            "describe_table": {
                "description": "Get table schema information",
                "parameters": {
                    "table": {"type": "string", "description": "Table name", "required": True},
                },
            },
        }

    async def execute(self, action: str, params: dict[str, Any]) -> ConnectorResult:
        try:
            if action == "insert":
                return await self._insert(params["table"], params["data"])
            elif action == "insert_many":
                return await self._insert_many(params["table"], params["records"])
            elif action == "update":
                return await self._update(params["table"], params["data"], params["where"])
            elif action == "delete":
                return await self._delete(params["table"], params["where"])
            elif action == "query":
                return await self._query(params["sql"], params.get("params", []))
            elif action == "execute":
                return await self._execute_sql(params["sql"], params.get("params", []))
            elif action == "upsert":
                return await self._upsert(params["table"], params["data"], params["key_columns"])
            elif action == "bulk_insert":
                return await self._bulk_insert(params["table"], params["data"], params.get("batch_size", 1000))
            elif action == "list_tables":
                return await self._list_tables()
            elif action == "describe_table":
                return await self._describe_table(params["table"])
            else:
                return ConnectorResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ConnectorResult(success=False, error=str(e))

    async def _insert(self, table: str, data: dict) -> ConnectorResult:
        conn = await self._get_connection()
        columns = ", ".join(f"[{k}]" for k in data.keys())
        placeholders = ", ".join("?" for _ in data)
        sql = f"INSERT INTO [{table}] ({columns}) VALUES ({placeholders})"

        cursor = await conn.cursor() if hasattr(conn, 'cursor') else conn.cursor()
        if hasattr(cursor, '__aenter__'):
            async with cursor:
                await cursor.execute(sql, list(data.values()))
                await conn.commit()
                return ConnectorResult(success=True, data={"inserted": 1})
        else:
            cursor.execute(sql, list(data.values()))
            conn.commit()
            return ConnectorResult(success=True, data={"inserted": 1})

    async def _insert_many(self, table: str, records: list[dict]) -> ConnectorResult:
        if not records:
            return ConnectorResult(success=True, data={"inserted": 0})

        conn = await self._get_connection()
        columns = ", ".join(f"[{k}]" for k in records[0].keys())
        placeholders = ", ".join("?" for _ in records[0])
        sql = f"INSERT INTO [{table}] ({columns}) VALUES ({placeholders})"

        cursor = await conn.cursor() if hasattr(conn, 'cursor') else conn.cursor()
        values = [list(r.values()) for r in records]

        if hasattr(cursor, '__aenter__'):
            async with cursor:
                await cursor.executemany(sql, values)
                await conn.commit()
        else:
            cursor.executemany(sql, values)
            conn.commit()

        return ConnectorResult(success=True, data={"inserted": len(records)})

    async def _update(self, table: str, data: dict, where: str) -> ConnectorResult:
        conn = await self._get_connection()
        set_clause = ", ".join(f"[{k}] = ?" for k in data.keys())
        sql = f"UPDATE [{table}] SET {set_clause} WHERE {where}"

        cursor = await conn.cursor() if hasattr(conn, 'cursor') else conn.cursor()
        if hasattr(cursor, '__aenter__'):
            async with cursor:
                await cursor.execute(sql, list(data.values()))
                rows_affected = cursor.rowcount
                await conn.commit()
        else:
            cursor.execute(sql, list(data.values()))
            rows_affected = cursor.rowcount
            conn.commit()

        return ConnectorResult(success=True, data={"updated": rows_affected})

    async def _delete(self, table: str, where: str) -> ConnectorResult:
        conn = await self._get_connection()
        sql = f"DELETE FROM [{table}] WHERE {where}"

        cursor = await conn.cursor() if hasattr(conn, 'cursor') else conn.cursor()
        if hasattr(cursor, '__aenter__'):
            async with cursor:
                await cursor.execute(sql)
                rows_affected = cursor.rowcount
                await conn.commit()
        else:
            cursor.execute(sql)
            rows_affected = cursor.rowcount
            conn.commit()

        return ConnectorResult(success=True, data={"deleted": rows_affected})

    async def _query(self, sql: str, params: list) -> ConnectorResult:
        conn = await self._get_connection()
        cursor = await conn.cursor() if hasattr(conn, 'cursor') else conn.cursor()

        if hasattr(cursor, '__aenter__'):
            async with cursor:
                await cursor.execute(sql, params)
                columns = [col[0] for col in cursor.description]
                rows = await cursor.fetchall()
        else:
            cursor.execute(sql, params)
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()

        results = [dict(zip(columns, row)) for row in rows]
        return ConnectorResult(success=True, data={"rows": results, "count": len(results)})

    async def _execute_sql(self, sql: str, params: list) -> ConnectorResult:
        conn = await self._get_connection()
        cursor = await conn.cursor() if hasattr(conn, 'cursor') else conn.cursor()

        if hasattr(cursor, '__aenter__'):
            async with cursor:
                await cursor.execute(sql, params)
                rows_affected = cursor.rowcount
                await conn.commit()
        else:
            cursor.execute(sql, params)
            rows_affected = cursor.rowcount
            conn.commit()

        return ConnectorResult(success=True, data={"rows_affected": rows_affected})

    async def _upsert(self, table: str, data: dict, key_columns: list[str]) -> ConnectorResult:
        conn = await self._get_connection()

        # Build MERGE statement
        columns = list(data.keys())
        source_cols = ", ".join(f"? AS [{c}]" for c in columns)
        match_cond = " AND ".join(f"target.[{k}] = source.[{k}]" for k in key_columns)
        update_cols = ", ".join(f"target.[{c}] = source.[{c}]" for c in columns if c not in key_columns)
        insert_cols = ", ".join(f"[{c}]" for c in columns)
        insert_vals = ", ".join(f"source.[{c}]" for c in columns)

        sql = f"""
        MERGE [{table}] AS target
        USING (SELECT {source_cols}) AS source
        ON {match_cond}
        WHEN MATCHED THEN UPDATE SET {update_cols}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals});
        """

        cursor = await conn.cursor() if hasattr(conn, 'cursor') else conn.cursor()
        if hasattr(cursor, '__aenter__'):
            async with cursor:
                await cursor.execute(sql, list(data.values()))
                await conn.commit()
        else:
            cursor.execute(sql, list(data.values()))
            conn.commit()

        return ConnectorResult(success=True, data={"upserted": 1})

    async def _bulk_insert(self, table: str, data: list[dict], batch_size: int) -> ConnectorResult:
        if not data:
            return ConnectorResult(success=True, data={"inserted": 0})

        total = 0
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            result = await self._insert_many(table, batch)
            if not result.success:
                return ConnectorResult(
                    success=False,
                    error=f"Batch insert failed at batch {i // batch_size}: {result.error}",
                    data={"inserted_before_failure": total}
                )
            total += len(batch)

        return ConnectorResult(success=True, data={"inserted": total})

    async def _list_tables(self) -> ConnectorResult:
        sql = """
        SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
        FROM INFORMATION_SCHEMA.TABLES
        ORDER BY TABLE_SCHEMA, TABLE_NAME
        """
        return await self._query(sql, [])

    async def _describe_table(self, table: str) -> ConnectorResult:
        sql = """
        SELECT
            COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH,
            IS_NULLABLE, COLUMN_DEFAULT
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
        """
        return await self._query(sql, [table])

    async def close(self):
        """Close the database connection."""
        if self._connection:
            if hasattr(self._connection, 'close'):
                await self._connection.close() if hasattr(self._connection.close, '__call__') else self._connection.close()
            self._connection = None
