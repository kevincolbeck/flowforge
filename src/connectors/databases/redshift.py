"""
Amazon Redshift Connector

Connect to Amazon Redshift data warehouse.
"""

from typing import Any
from ..base import BaseConnector, ConnectorResult


class RedshiftConnector(BaseConnector):
    """Connector for Amazon Redshift."""

    def __init__(self, credentials: dict[str, Any]):
        super().__init__(credentials)
        self.host = credentials.get("host")
        self.port = credentials.get("port", 5439)
        self.database = credentials.get("database")
        self.user = credentials.get("user")
        self.password = credentials.get("password")
        self._connection = None

    async def _get_connection(self):
        """Get database connection."""
        if self._connection is None:
            import redshift_connector
            self._connection = redshift_connector.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
            )
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
            "copy_from_s3": {
                "description": "Copy data from S3 into a table",
                "parameters": {
                    "table": {"type": "string", "description": "Target table", "required": True},
                    "s3_path": {"type": "string", "description": "S3 path (s3://...)", "required": True},
                    "iam_role": {"type": "string", "description": "IAM role ARN", "required": True},
                    "format": {"type": "string", "description": "CSV, JSON, PARQUET", "required": False},
                },
            },
            "unload_to_s3": {
                "description": "Unload query results to S3",
                "parameters": {
                    "sql": {"type": "string", "description": "SELECT query", "required": True},
                    "s3_path": {"type": "string", "description": "S3 destination", "required": True},
                    "iam_role": {"type": "string", "description": "IAM role ARN", "required": True},
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
            "vacuum": {
                "description": "Vacuum a table to reclaim space",
                "parameters": {
                    "table": {"type": "string", "description": "Table name", "required": True},
                },
            },
            "analyze": {
                "description": "Analyze a table for query optimization",
                "parameters": {
                    "table": {"type": "string", "description": "Table name", "required": True},
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
            elif action == "copy_from_s3":
                return await self._copy_from_s3(
                    params["table"], params["s3_path"],
                    params["iam_role"], params.get("format", "CSV")
                )
            elif action == "unload_to_s3":
                return await self._unload_to_s3(params["sql"], params["s3_path"], params["iam_role"])
            elif action == "list_tables":
                return await self._list_tables(params.get("schema", "public"))
            elif action == "describe_table":
                return await self._describe_table(params["table"])
            elif action == "vacuum":
                return await self._execute_sql(f"VACUUM {params['table']}", [])
            elif action == "analyze":
                return await self._execute_sql(f"ANALYZE {params['table']}", [])
            else:
                return ConnectorResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ConnectorResult(success=False, error=str(e))

    async def _query(self, sql: str, params: list) -> ConnectorResult:
        conn = await self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(sql, params)
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            rows = cursor.fetchall()
            results = [dict(zip(columns, row)) for row in rows]
            return ConnectorResult(success=True, data={"rows": results, "count": len(results)})
        finally:
            cursor.close()

    async def _execute_sql(self, sql: str, params: list) -> ConnectorResult:
        conn = await self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(sql, params)
            conn.commit()
            return ConnectorResult(success=True, data={"rows_affected": cursor.rowcount})
        finally:
            cursor.close()

    async def _insert(self, table: str, data: dict) -> ConnectorResult:
        columns = ", ".join(data.keys())
        placeholders = ", ".join("%s" for _ in data)
        sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        return await self._execute_sql(sql, list(data.values()))

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
            conn.commit()
            return ConnectorResult(success=True, data={"inserted": len(records)})
        finally:
            cursor.close()

    async def _copy_from_s3(self, table: str, s3_path: str, iam_role: str, format: str) -> ConnectorResult:
        sql = f"""
        COPY {table}
        FROM '{s3_path}'
        IAM_ROLE '{iam_role}'
        FORMAT AS {format}
        """
        return await self._execute_sql(sql, [])

    async def _unload_to_s3(self, sql: str, s3_path: str, iam_role: str) -> ConnectorResult:
        unload_sql = f"""
        UNLOAD ('{sql.replace("'", "''")}')
        TO '{s3_path}'
        IAM_ROLE '{iam_role}'
        """
        return await self._execute_sql(unload_sql, [])

    async def _list_tables(self, schema: str) -> ConnectorResult:
        sql = """
        SELECT table_name, table_type
        FROM information_schema.tables
        WHERE table_schema = %s
        ORDER BY table_name
        """
        return await self._query(sql, [schema])

    async def _describe_table(self, table: str) -> ConnectorResult:
        sql = """
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_name = %s
        ORDER BY ordinal_position
        """
        return await self._query(sql, [table])

    async def close(self):
        if self._connection:
            self._connection.close()
            self._connection = None
