"""
Snowflake Connector

Connect to Snowflake data warehouse for analytics and data operations.
"""

from typing import Any
from ..base import BaseConnector, ConnectorResult


class SnowflakeConnector(BaseConnector):
    """Connector for Snowflake data warehouse."""

    def __init__(self, credentials: dict[str, Any]):
        super().__init__(credentials)
        self.account = credentials.get("account")
        self.user = credentials.get("user")
        self.password = credentials.get("password")
        self.warehouse = credentials.get("warehouse")
        self.database = credentials.get("database")
        self.schema = credentials.get("schema", "PUBLIC")
        self.role = credentials.get("role")
        self._connection = None

    async def _get_connection(self):
        """Get database connection."""
        if self._connection is None:
            import snowflake.connector
            self._connection = snowflake.connector.connect(
                account=self.account,
                user=self.user,
                password=self.password,
                warehouse=self.warehouse,
                database=self.database,
                schema=self.schema,
                role=self.role,
            )
        return self._connection

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
            "merge": {
                "description": "Merge (upsert) data into a table",
                "parameters": {
                    "table": {"type": "string", "description": "Target table", "required": True},
                    "data": {"type": "object", "description": "Data to merge", "required": True},
                    "key_columns": {"type": "array", "description": "Match columns", "required": True},
                },
            },
            "copy_into": {
                "description": "Copy data from stage into table",
                "parameters": {
                    "table": {"type": "string", "description": "Target table", "required": True},
                    "stage": {"type": "string", "description": "Stage name", "required": True},
                    "file_format": {"type": "string", "description": "File format", "required": False},
                },
            },
            "list_tables": {
                "description": "List tables in schema",
                "parameters": {
                    "schema": {"type": "string", "description": "Schema name", "required": False},
                },
            },
            "describe_table": {
                "description": "Get table schema",
                "parameters": {
                    "table": {"type": "string", "description": "Table name", "required": True},
                },
            },
            "list_warehouses": {
                "description": "List available warehouses",
                "parameters": {},
            },
            "list_databases": {
                "description": "List available databases",
                "parameters": {},
            },
        }

    async def execute(self, action: str, params: dict[str, Any]) -> ConnectorResult:
        try:
            if action == "query":
                return await self._query(params["sql"], params.get("params", {}))
            elif action == "execute":
                return await self._execute_sql(params["sql"], params.get("params", {}))
            elif action == "insert":
                return await self._insert(params["table"], params["data"])
            elif action == "insert_many":
                return await self._insert_many(params["table"], params["records"])
            elif action == "merge":
                return await self._merge(params["table"], params["data"], params["key_columns"])
            elif action == "copy_into":
                return await self._copy_into(params["table"], params["stage"], params.get("file_format"))
            elif action == "list_tables":
                return await self._list_tables(params.get("schema", self.schema))
            elif action == "describe_table":
                return await self._describe_table(params["table"])
            elif action == "list_warehouses":
                return await self._query("SHOW WAREHOUSES", {})
            elif action == "list_databases":
                return await self._query("SHOW DATABASES", {})
            else:
                return ConnectorResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ConnectorResult(success=False, error=str(e))

    async def _query(self, sql: str, params: dict) -> ConnectorResult:
        conn = await self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(sql, params)
            columns = [col[0] for col in cursor.description] if cursor.description else []
            rows = cursor.fetchall()
            results = [dict(zip(columns, row)) for row in rows]
            return ConnectorResult(success=True, data={"rows": results, "count": len(results)})
        finally:
            cursor.close()

    async def _execute_sql(self, sql: str, params: dict) -> ConnectorResult:
        conn = await self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(sql, params)
            return ConnectorResult(success=True, data={"rows_affected": cursor.rowcount})
        finally:
            cursor.close()

    async def _insert(self, table: str, data: dict) -> ConnectorResult:
        columns = ", ".join(data.keys())
        placeholders = ", ".join(f"%({k})s" for k in data.keys())
        sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        return await self._execute_sql(sql, data)

    async def _insert_many(self, table: str, records: list[dict]) -> ConnectorResult:
        if not records:
            return ConnectorResult(success=True, data={"inserted": 0})

        conn = await self._get_connection()
        cursor = conn.cursor()
        try:
            columns = list(records[0].keys())
            col_str = ", ".join(columns)
            placeholders = ", ".join("%s" for _ in columns)
            sql = f"INSERT INTO {table} ({col_str}) VALUES ({placeholders})"

            values = [tuple(r[c] for c in columns) for r in records]
            cursor.executemany(sql, values)
            return ConnectorResult(success=True, data={"inserted": len(records)})
        finally:
            cursor.close()

    async def _merge(self, table: str, data: dict, key_columns: list) -> ConnectorResult:
        columns = list(data.keys())
        source_cols = ", ".join(f"%({c})s AS {c}" for c in columns)
        match_cond = " AND ".join(f"target.{k} = source.{k}" for k in key_columns)
        update_cols = ", ".join(f"target.{c} = source.{c}" for c in columns if c not in key_columns)
        insert_cols = ", ".join(columns)
        insert_vals = ", ".join(f"source.{c}" for c in columns)

        sql = f"""
        MERGE INTO {table} AS target
        USING (SELECT {source_cols}) AS source
        ON {match_cond}
        WHEN MATCHED THEN UPDATE SET {update_cols}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """
        return await self._execute_sql(sql, data)

    async def _copy_into(self, table: str, stage: str, file_format: str | None) -> ConnectorResult:
        sql = f"COPY INTO {table} FROM @{stage}"
        if file_format:
            sql += f" FILE_FORMAT = (FORMAT_NAME = '{file_format}')"
        return await self._execute_sql(sql, {})

    async def _list_tables(self, schema: str) -> ConnectorResult:
        return await self._query(f"SHOW TABLES IN SCHEMA {schema}", {})

    async def _describe_table(self, table: str) -> ConnectorResult:
        return await self._query(f"DESCRIBE TABLE {table}", {})

    async def close(self):
        if self._connection:
            self._connection.close()
            self._connection = None
