"""
SQLite Connector

Connect to SQLite databases for local data storage.
"""

from typing import Any
from ..base import BaseConnector, ConnectorResult


class SQLiteConnector(BaseConnector):
    """Connector for SQLite databases."""

    def __init__(self, credentials: dict[str, Any]):
        super().__init__(credentials)
        self.database_path = credentials.get("database_path", ":memory:")
        self._connection = None

    async def _get_connection(self):
        """Get database connection."""
        if self._connection is None:
            import aiosqlite
            self._connection = await aiosqlite.connect(self.database_path)
            self._connection.row_factory = aiosqlite.Row
        return self._connection

    @classmethod
    def get_actions(cls) -> dict[str, dict[str, Any]]:
        return {
            "query": {
                "description": "Execute a SELECT query",
                "parameters": {
                    "sql": {"type": "string", "description": "SQL query", "required": True},
                    "params": {"type": "array", "description": "Query parameters", "required": False},
                },
            },
            "execute": {
                "description": "Execute SQL statement",
                "parameters": {
                    "sql": {"type": "string", "description": "SQL to execute", "required": True},
                    "params": {"type": "array", "description": "Parameters", "required": False},
                },
            },
            "insert": {
                "description": "Insert data into a table",
                "parameters": {
                    "table": {"type": "string", "description": "Table name", "required": True},
                    "data": {"type": "object", "description": "Column-value pairs", "required": True},
                },
            },
            "insert_many": {
                "description": "Insert multiple rows",
                "parameters": {
                    "table": {"type": "string", "description": "Table name", "required": True},
                    "records": {"type": "array", "description": "Array of records", "required": True},
                },
            },
            "update": {
                "description": "Update rows",
                "parameters": {
                    "table": {"type": "string", "description": "Table name", "required": True},
                    "data": {"type": "object", "description": "Column-value pairs", "required": True},
                    "where": {"type": "string", "description": "WHERE clause", "required": True},
                    "params": {"type": "array", "description": "WHERE parameters", "required": False},
                },
            },
            "delete": {
                "description": "Delete rows",
                "parameters": {
                    "table": {"type": "string", "description": "Table name", "required": True},
                    "where": {"type": "string", "description": "WHERE clause", "required": True},
                    "params": {"type": "array", "description": "WHERE parameters", "required": False},
                },
            },
            "upsert": {
                "description": "Insert or replace",
                "parameters": {
                    "table": {"type": "string", "description": "Table name", "required": True},
                    "data": {"type": "object", "description": "Column-value pairs", "required": True},
                },
            },
            "list_tables": {
                "description": "List all tables",
                "parameters": {},
            },
            "describe_table": {
                "description": "Get table schema",
                "parameters": {
                    "table": {"type": "string", "description": "Table name", "required": True},
                },
            },
            "create_table": {
                "description": "Create a new table",
                "parameters": {
                    "table": {"type": "string", "description": "Table name", "required": True},
                    "columns": {"type": "object", "description": "Column definitions", "required": True},
                },
            },
        }

    async def execute(self, action: str, params: dict[str, Any]) -> ConnectorResult:
        try:
            if action == "query":
                return await self._query(params["sql"], params.get("params", []))
            elif action == "execute":
                return await self._execute_sql(params["sql"], params.get("params", []))
            elif action == "insert":
                return await self._insert(params["table"], params["data"])
            elif action == "insert_many":
                return await self._insert_many(params["table"], params["records"])
            elif action == "update":
                return await self._update(
                    params["table"], params["data"],
                    params["where"], params.get("params", [])
                )
            elif action == "delete":
                return await self._delete(params["table"], params["where"], params.get("params", []))
            elif action == "upsert":
                return await self._upsert(params["table"], params["data"])
            elif action == "list_tables":
                return await self._list_tables()
            elif action == "describe_table":
                return await self._describe_table(params["table"])
            elif action == "create_table":
                return await self._create_table(params["table"], params["columns"])
            else:
                return ConnectorResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ConnectorResult(success=False, error=str(e))

    async def _query(self, sql: str, params: list) -> ConnectorResult:
        conn = await self._get_connection()
        async with conn.execute(sql, params) as cursor:
            rows = await cursor.fetchall()
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            results = [dict(zip(columns, row)) for row in rows]
            return ConnectorResult(success=True, data={"rows": results, "count": len(results)})

    async def _execute_sql(self, sql: str, params: list) -> ConnectorResult:
        conn = await self._get_connection()
        async with conn.execute(sql, params) as cursor:
            await conn.commit()
            return ConnectorResult(success=True, data={"rows_affected": cursor.rowcount})

    async def _insert(self, table: str, data: dict) -> ConnectorResult:
        conn = await self._get_connection()
        columns = ", ".join(f'"{k}"' for k in data.keys())
        placeholders = ", ".join("?" for _ in data)
        sql = f'INSERT INTO "{table}" ({columns}) VALUES ({placeholders})'

        async with conn.execute(sql, list(data.values())) as cursor:
            await conn.commit()
            return ConnectorResult(
                success=True,
                data={"inserted": 1, "last_insert_id": cursor.lastrowid}
            )

    async def _insert_many(self, table: str, records: list[dict]) -> ConnectorResult:
        if not records:
            return ConnectorResult(success=True, data={"inserted": 0})

        conn = await self._get_connection()
        columns = ", ".join(f'"{k}"' for k in records[0].keys())
        placeholders = ", ".join("?" for _ in records[0])
        sql = f'INSERT INTO "{table}" ({columns}) VALUES ({placeholders})'

        await conn.executemany(sql, [list(r.values()) for r in records])
        await conn.commit()
        return ConnectorResult(success=True, data={"inserted": len(records)})

    async def _update(self, table: str, data: dict, where: str, where_params: list) -> ConnectorResult:
        conn = await self._get_connection()
        set_parts = [f'"{k}" = ?' for k in data.keys()]
        sql = f'UPDATE "{table}" SET {", ".join(set_parts)} WHERE {where}'

        async with conn.execute(sql, list(data.values()) + where_params) as cursor:
            await conn.commit()
            return ConnectorResult(success=True, data={"updated": cursor.rowcount})

    async def _delete(self, table: str, where: str, params: list) -> ConnectorResult:
        conn = await self._get_connection()
        sql = f'DELETE FROM "{table}" WHERE {where}'

        async with conn.execute(sql, params) as cursor:
            await conn.commit()
            return ConnectorResult(success=True, data={"deleted": cursor.rowcount})

    async def _upsert(self, table: str, data: dict) -> ConnectorResult:
        conn = await self._get_connection()
        columns = ", ".join(f'"{k}"' for k in data.keys())
        placeholders = ", ".join("?" for _ in data)
        sql = f'INSERT OR REPLACE INTO "{table}" ({columns}) VALUES ({placeholders})'

        async with conn.execute(sql, list(data.values())) as cursor:
            await conn.commit()
            return ConnectorResult(success=True, data={"upserted": 1})

    async def _list_tables(self) -> ConnectorResult:
        sql = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        return await self._query(sql, [])

    async def _describe_table(self, table: str) -> ConnectorResult:
        sql = f'PRAGMA table_info("{table}")'
        return await self._query(sql, [])

    async def _create_table(self, table: str, columns: dict) -> ConnectorResult:
        col_defs = []
        for name, definition in columns.items():
            if isinstance(definition, str):
                col_defs.append(f'"{name}" {definition}')
            else:
                col_defs.append(f'"{name}" {definition.get("type", "TEXT")}')

        sql = f'CREATE TABLE IF NOT EXISTS "{table}" ({", ".join(col_defs)})'
        return await self._execute_sql(sql, [])

    async def close(self):
        if self._connection:
            await self._connection.close()
            self._connection = None
