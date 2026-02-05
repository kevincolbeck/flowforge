"""
MySQL Connector

Connect to MySQL and MariaDB compatible databases.
"""

from typing import Any
from ..base import BaseConnector, ConnectorResult


class MySQLConnector(BaseConnector):
    """Connector for MySQL databases."""

    def __init__(self, credentials: dict[str, Any]):
        super().__init__(credentials)
        self.host = credentials.get("host", "localhost")
        self.port = credentials.get("port", 3306)
        self.database = credentials.get("database")
        self.user = credentials.get("user")
        self.password = credentials.get("password")
        self.ssl = credentials.get("ssl", False)
        self._pool = None

    async def _get_pool(self):
        """Get or create connection pool."""
        if self._pool is None:
            import aiomysql
            self._pool = await aiomysql.create_pool(
                host=self.host,
                port=self.port,
                db=self.database,
                user=self.user,
                password=self.password,
                ssl=self.ssl,
                minsize=1,
                maxsize=10,
                autocommit=True,
            )
        return self._pool

    @classmethod
    def get_actions(cls) -> dict[str, dict[str, Any]]:
        return {
            "insert": {
                "description": "Insert a record into a table",
                "parameters": {
                    "table": {"type": "string", "description": "Table name", "required": True},
                    "data": {"type": "object", "description": "Column-value pairs", "required": True},
                },
            },
            "insert_many": {
                "description": "Insert multiple records",
                "parameters": {
                    "table": {"type": "string", "description": "Table name", "required": True},
                    "records": {"type": "array", "description": "Array of records", "required": True},
                },
            },
            "update": {
                "description": "Update records",
                "parameters": {
                    "table": {"type": "string", "description": "Table name", "required": True},
                    "data": {"type": "object", "description": "Column-value pairs", "required": True},
                    "where": {"type": "object", "description": "WHERE conditions", "required": True},
                },
            },
            "delete": {
                "description": "Delete records",
                "parameters": {
                    "table": {"type": "string", "description": "Table name", "required": True},
                    "where": {"type": "object", "description": "WHERE conditions", "required": True},
                },
            },
            "query": {
                "description": "Execute a SELECT query",
                "parameters": {
                    "sql": {"type": "string", "description": "SQL query", "required": True},
                    "params": {"type": "array", "description": "Query parameters", "required": False},
                },
            },
            "execute": {
                "description": "Execute arbitrary SQL",
                "parameters": {
                    "sql": {"type": "string", "description": "SQL to execute", "required": True},
                    "params": {"type": "array", "description": "Query parameters", "required": False},
                },
            },
            "upsert": {
                "description": "Insert or update on duplicate key",
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
                return await self._upsert(params["table"], params["data"])
            elif action == "list_tables":
                return await self._list_tables()
            elif action == "describe_table":
                return await self._describe_table(params["table"])
            else:
                return ConnectorResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ConnectorResult(success=False, error=str(e))

    async def _insert(self, table: str, data: dict) -> ConnectorResult:
        pool = await self._get_pool()
        columns = ", ".join(f"`{k}`" for k in data.keys())
        placeholders = ", ".join("%s" for _ in data)
        sql = f"INSERT INTO `{table}` ({columns}) VALUES ({placeholders})"

        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(sql, list(data.values()))
                return ConnectorResult(
                    success=True,
                    data={"inserted": 1, "last_insert_id": cursor.lastrowid}
                )

    async def _insert_many(self, table: str, records: list[dict]) -> ConnectorResult:
        if not records:
            return ConnectorResult(success=True, data={"inserted": 0})

        pool = await self._get_pool()
        columns = ", ".join(f"`{k}`" for k in records[0].keys())
        placeholders = ", ".join("%s" for _ in records[0])
        sql = f"INSERT INTO `{table}` ({columns}) VALUES ({placeholders})"

        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.executemany(sql, [list(r.values()) for r in records])
                return ConnectorResult(success=True, data={"inserted": len(records)})

    async def _update(self, table: str, data: dict, where: dict) -> ConnectorResult:
        pool = await self._get_pool()

        set_parts = [f"`{k}` = %s" for k in data.keys()]
        where_parts = [f"`{k}` = %s" for k in where.keys()]
        values = list(data.values()) + list(where.values())

        sql = f"UPDATE `{table}` SET {', '.join(set_parts)} WHERE {' AND '.join(where_parts)}"

        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(sql, values)
                return ConnectorResult(success=True, data={"updated": cursor.rowcount})

    async def _delete(self, table: str, where: dict) -> ConnectorResult:
        pool = await self._get_pool()

        where_parts = [f"`{k}` = %s" for k in where.keys()]
        sql = f"DELETE FROM `{table}` WHERE {' AND '.join(where_parts)}"

        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(sql, list(where.values()))
                return ConnectorResult(success=True, data={"deleted": cursor.rowcount})

    async def _query(self, sql: str, params: list) -> ConnectorResult:
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(sql, params)
                columns = [col[0] for col in cursor.description] if cursor.description else []
                rows = await cursor.fetchall()
                results = [dict(zip(columns, row)) for row in rows]
                return ConnectorResult(success=True, data={"rows": results, "count": len(results)})

    async def _execute_sql(self, sql: str, params: list) -> ConnectorResult:
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(sql, params)
                return ConnectorResult(success=True, data={"rows_affected": cursor.rowcount})

    async def _upsert(self, table: str, data: dict) -> ConnectorResult:
        pool = await self._get_pool()

        columns = ", ".join(f"`{k}`" for k in data.keys())
        placeholders = ", ".join("%s" for _ in data)
        updates = ", ".join(f"`{k}` = VALUES(`{k}`)" for k in data.keys())

        sql = f"""
        INSERT INTO `{table}` ({columns}) VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE {updates}
        """

        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(sql, list(data.values()))
                return ConnectorResult(success=True, data={"upserted": 1})

    async def _list_tables(self) -> ConnectorResult:
        return await self._query("SHOW TABLES", [])

    async def _describe_table(self, table: str) -> ConnectorResult:
        return await self._query(f"DESCRIBE `{table}`", [])

    async def close(self):
        if self._pool:
            self._pool.close()
            await self._pool.wait_closed()
            self._pool = None
