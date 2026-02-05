"""
ClickHouse Connector

Connect to ClickHouse for high-performance analytics.
"""

from typing import Any
from ..base import BaseConnector, ConnectorResult


class ClickHouseConnector(BaseConnector):
    """Connector for ClickHouse."""

    def __init__(self, credentials: dict[str, Any]):
        super().__init__(credentials)
        self.host = credentials.get("host", "localhost")
        self.port = credentials.get("port", 8123)  # HTTP port
        self.database = credentials.get("database", "default")
        self.user = credentials.get("user", "default")
        self.password = credentials.get("password", "")
        self.secure = credentials.get("secure", False)
        self._client = None

    async def _get_client(self):
        """Get ClickHouse client."""
        if self._client is None:
            from clickhouse_driver import Client

            self._client = Client(
                host=self.host,
                port=credentials.get("native_port", 9000),  # Native port for driver
                database=self.database,
                user=self.user,
                password=self.password,
                secure=self.secure,
            )
        return self._client

    @classmethod
    def get_actions(cls) -> dict[str, dict[str, Any]]:
        return {
            "query": {
                "description": "Execute a SELECT query",
                "parameters": {
                    "sql": {"type": "string", "description": "SQL query", "required": True},
                    "params": {"type": "object", "description": "Query parameters", "required": False},
                },
            },
            "execute": {
                "description": "Execute SQL statement",
                "parameters": {
                    "sql": {"type": "string", "description": "SQL to execute", "required": True},
                    "params": {"type": "object", "description": "Parameters", "required": False},
                },
            },
            "insert": {
                "description": "Insert data into a table",
                "parameters": {
                    "table": {"type": "string", "description": "Table name", "required": True},
                    "data": {"type": "array", "description": "Rows to insert", "required": True},
                    "columns": {"type": "array", "description": "Column names", "required": True},
                },
            },
            "insert_dataframe": {
                "description": "Insert from pandas DataFrame",
                "parameters": {
                    "table": {"type": "string", "description": "Table name", "required": True},
                    "data": {"type": "array", "description": "Array of row objects", "required": True},
                },
            },
            "list_tables": {
                "description": "List tables in database",
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
                    "engine": {"type": "string", "description": "Table engine (e.g., MergeTree)", "required": True},
                    "order_by": {"type": "array", "description": "ORDER BY columns", "required": False},
                },
            },
            "optimize": {
                "description": "Optimize a table",
                "parameters": {
                    "table": {"type": "string", "description": "Table name", "required": True},
                    "final": {"type": "boolean", "description": "FINAL optimization", "required": False},
                },
            },
            "truncate": {
                "description": "Truncate a table",
                "parameters": {
                    "table": {"type": "string", "description": "Table name", "required": True},
                },
            },
        }

    async def execute(self, action: str, params: dict[str, Any]) -> ConnectorResult:
        try:
            client = await self._get_client()

            if action == "query":
                return await self._query(client, params["sql"], params.get("params", {}))
            elif action == "execute":
                return await self._execute_sql(client, params["sql"], params.get("params", {}))
            elif action == "insert":
                return await self._insert(client, params["table"], params["data"], params["columns"])
            elif action == "insert_dataframe":
                return await self._insert_dataframe(client, params["table"], params["data"])
            elif action == "list_tables":
                return await self._list_tables(client)
            elif action == "describe_table":
                return await self._describe_table(client, params["table"])
            elif action == "create_table":
                return await self._create_table(
                    client, params["table"], params["columns"],
                    params["engine"], params.get("order_by")
                )
            elif action == "optimize":
                return await self._optimize(client, params["table"], params.get("final", False))
            elif action == "truncate":
                return await self._truncate(client, params["table"])
            else:
                return ConnectorResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ConnectorResult(success=False, error=str(e))

    async def _query(self, client, sql: str, params: dict) -> ConnectorResult:
        result = client.execute(sql, params, with_column_types=True)
        rows, columns = result
        col_names = [col[0] for col in columns]
        data = [dict(zip(col_names, row)) for row in rows]
        return ConnectorResult(success=True, data={"rows": data, "count": len(data)})

    async def _execute_sql(self, client, sql: str, params: dict) -> ConnectorResult:
        client.execute(sql, params)
        return ConnectorResult(success=True, data={"executed": True})

    async def _insert(self, client, table: str, data: list, columns: list) -> ConnectorResult:
        col_str = ", ".join(columns)
        sql = f"INSERT INTO {table} ({col_str}) VALUES"
        client.execute(sql, data)
        return ConnectorResult(success=True, data={"inserted": len(data)})

    async def _insert_dataframe(self, client, table: str, data: list[dict]) -> ConnectorResult:
        if not data:
            return ConnectorResult(success=True, data={"inserted": 0})

        columns = list(data[0].keys())
        rows = [tuple(row[col] for col in columns) for row in data]

        col_str = ", ".join(columns)
        sql = f"INSERT INTO {table} ({col_str}) VALUES"
        client.execute(sql, rows)
        return ConnectorResult(success=True, data={"inserted": len(data)})

    async def _list_tables(self, client) -> ConnectorResult:
        result = client.execute(f"SHOW TABLES FROM {self.database}")
        tables = [row[0] for row in result]
        return ConnectorResult(success=True, data={"tables": tables})

    async def _describe_table(self, client, table: str) -> ConnectorResult:
        result = client.execute(f"DESCRIBE TABLE {table}", with_column_types=True)
        rows, _ = result
        columns = [{"name": row[0], "type": row[1], "default_type": row[2], "default_expression": row[3]} for row in rows]
        return ConnectorResult(success=True, data={"columns": columns})

    async def _create_table(self, client, table: str, columns: dict, engine: str, order_by: list | None) -> ConnectorResult:
        col_defs = [f"{name} {col_type}" for name, col_type in columns.items()]
        sql = f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(col_defs)}) ENGINE = {engine}"

        if order_by:
            sql += f" ORDER BY ({', '.join(order_by)})"

        client.execute(sql)
        return ConnectorResult(success=True, data={"created": table})

    async def _optimize(self, client, table: str, final: bool) -> ConnectorResult:
        sql = f"OPTIMIZE TABLE {table}"
        if final:
            sql += " FINAL"
        client.execute(sql)
        return ConnectorResult(success=True, data={"optimized": table})

    async def _truncate(self, client, table: str) -> ConnectorResult:
        client.execute(f"TRUNCATE TABLE {table}")
        return ConnectorResult(success=True, data={"truncated": table})

    async def close(self):
        if self._client:
            self._client.disconnect()
            self._client = None
