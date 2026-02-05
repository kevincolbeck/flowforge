"""
SQL Server Connector

Connect to Microsoft SQL Server (on-premises or Azure SQL Managed Instance).
"""

from typing import Any
from ..base import BaseConnector, ConnectorResult


class SQLServerConnector(BaseConnector):
    """Connector for Microsoft SQL Server."""

    def __init__(self, credentials: dict[str, Any]):
        super().__init__(credentials)
        self.server = credentials.get("server")
        self.database = credentials.get("database")
        self.username = credentials.get("username")
        self.password = credentials.get("password")
        self.port = credentials.get("port", 1433)
        self.driver = credentials.get("driver", "ODBC Driver 18 for SQL Server")
        self.trust_cert = credentials.get("trust_server_certificate", True)
        self._connection = None

    async def _get_connection(self):
        """Get database connection."""
        if self._connection is None:
            import pyodbc
            conn_str = (
                f"DRIVER={{{self.driver}}};"
                f"SERVER={self.server},{self.port};"
                f"DATABASE={self.database};"
                f"UID={self.username};"
                f"PWD={self.password};"
            )
            if self.trust_cert:
                conn_str += "TrustServerCertificate=yes;"

            self._connection = pyodbc.connect(conn_str)
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
                },
            },
            "delete": {
                "description": "Delete rows",
                "parameters": {
                    "table": {"type": "string", "description": "Table name", "required": True},
                    "where": {"type": "string", "description": "WHERE clause", "required": True},
                },
            },
            "merge": {
                "description": "Merge (upsert) data",
                "parameters": {
                    "table": {"type": "string", "description": "Table name", "required": True},
                    "data": {"type": "object", "description": "Column-value pairs", "required": True},
                    "key_columns": {"type": "array", "description": "Match columns", "required": True},
                },
            },
            "bulk_insert": {
                "description": "Bulk insert data",
                "parameters": {
                    "table": {"type": "string", "description": "Table name", "required": True},
                    "records": {"type": "array", "description": "Records to insert", "required": True},
                    "batch_size": {"type": "integer", "description": "Batch size", "required": False},
                },
            },
            "exec_procedure": {
                "description": "Execute a stored procedure",
                "parameters": {
                    "procedure": {"type": "string", "description": "Procedure name", "required": True},
                    "params": {"type": "object", "description": "Parameters", "required": False},
                },
            },
            "list_tables": {
                "description": "List tables in database",
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
                return await self._update(params["table"], params["data"], params["where"])
            elif action == "delete":
                return await self._delete(params["table"], params["where"])
            elif action == "merge":
                return await self._merge(params["table"], params["data"], params["key_columns"])
            elif action == "bulk_insert":
                return await self._bulk_insert(
                    params["table"], params["records"], params.get("batch_size", 1000)
                )
            elif action == "exec_procedure":
                return await self._exec_procedure(params["procedure"], params.get("params", {}))
            elif action == "list_tables":
                return await self._list_tables(params.get("schema", "dbo"))
            elif action == "describe_table":
                return await self._describe_table(params["table"])
            else:
                return ConnectorResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ConnectorResult(success=False, error=str(e))

    async def _query(self, sql: str, params: list) -> ConnectorResult:
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
        columns = ", ".join(f"[{k}]" for k in data.keys())
        placeholders = ", ".join("?" for _ in data)
        sql = f"INSERT INTO [{table}] ({columns}) VALUES ({placeholders})"
        return await self._execute_sql(sql, list(data.values()))

    async def _insert_many(self, table: str, records: list[dict]) -> ConnectorResult:
        if not records:
            return ConnectorResult(success=True, data={"inserted": 0})

        conn = await self._get_connection()
        cursor = conn.cursor()

        columns = ", ".join(f"[{k}]" for k in records[0].keys())
        placeholders = ", ".join("?" for _ in records[0])
        sql = f"INSERT INTO [{table}] ({columns}) VALUES ({placeholders})"

        try:
            cursor.fast_executemany = True
            cursor.executemany(sql, [list(r.values()) for r in records])
            conn.commit()
            return ConnectorResult(success=True, data={"inserted": len(records)})
        finally:
            cursor.close()

    async def _update(self, table: str, data: dict, where: str) -> ConnectorResult:
        set_clause = ", ".join(f"[{k}] = ?" for k in data.keys())
        sql = f"UPDATE [{table}] SET {set_clause} WHERE {where}"
        return await self._execute_sql(sql, list(data.values()))

    async def _delete(self, table: str, where: str) -> ConnectorResult:
        sql = f"DELETE FROM [{table}] WHERE {where}"
        return await self._execute_sql(sql, [])

    async def _merge(self, table: str, data: dict, key_columns: list) -> ConnectorResult:
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
        return await self._execute_sql(sql, list(data.values()))

    async def _bulk_insert(self, table: str, records: list[dict], batch_size: int) -> ConnectorResult:
        if not records:
            return ConnectorResult(success=True, data={"inserted": 0})

        total = 0
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            result = await self._insert_many(table, batch)
            if not result.success:
                return ConnectorResult(
                    success=False,
                    error=f"Batch failed at {i}: {result.error}",
                    data={"inserted_before_failure": total}
                )
            total += len(batch)

        return ConnectorResult(success=True, data={"inserted": total})

    async def _exec_procedure(self, procedure: str, params: dict) -> ConnectorResult:
        conn = await self._get_connection()
        cursor = conn.cursor()

        try:
            if params:
                param_str = ", ".join(f"@{k}=?" for k in params.keys())
                sql = f"EXEC {procedure} {param_str}"
                cursor.execute(sql, list(params.values()))
            else:
                cursor.execute(f"EXEC {procedure}")

            # Try to fetch results if any
            results = []
            if cursor.description:
                columns = [col[0] for col in cursor.description]
                rows = cursor.fetchall()
                results = [dict(zip(columns, row)) for row in rows]

            conn.commit()
            return ConnectorResult(success=True, data={"results": results})
        finally:
            cursor.close()

    async def _list_tables(self, schema: str) -> ConnectorResult:
        sql = """
        SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = ?
        ORDER BY TABLE_NAME
        """
        return await self._query(sql, [schema])

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
        if self._connection:
            self._connection.close()
            self._connection = None
