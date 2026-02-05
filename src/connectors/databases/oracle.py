"""
Oracle Database Connector

Connect to Oracle Database for enterprise data operations.
"""

from typing import Any
from ..base import BaseConnector, ConnectorResult


class OracleConnector(BaseConnector):
    """Connector for Oracle Database."""

    def __init__(self, credentials: dict[str, Any]):
        super().__init__(credentials)
        self.host = credentials.get("host")
        self.port = credentials.get("port", 1521)
        self.service_name = credentials.get("service_name")
        self.sid = credentials.get("sid")
        self.user = credentials.get("user")
        self.password = credentials.get("password")
        self._pool = None

    async def _get_pool(self):
        """Get connection pool."""
        if self._pool is None:
            import oracledb
            dsn = oracledb.makedsn(
                self.host, self.port,
                service_name=self.service_name,
                sid=self.sid
            )
            self._pool = oracledb.create_pool(
                user=self.user,
                password=self.password,
                dsn=dsn,
                min=1,
                max=10,
            )
        return self._pool

    @classmethod
    def get_actions(cls) -> dict[str, dict[str, Any]]:
        return {
            "query": {
                "description": "Execute a SELECT query",
                "parameters": {
                    "sql": {"type": "string", "description": "SQL query", "required": True},
                    "params": {"type": "object", "description": "Bind parameters", "required": False},
                },
            },
            "execute": {
                "description": "Execute SQL statement",
                "parameters": {
                    "sql": {"type": "string", "description": "SQL to execute", "required": True},
                    "params": {"type": "object", "description": "Bind parameters", "required": False},
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
            "call_procedure": {
                "description": "Call a stored procedure",
                "parameters": {
                    "procedure": {"type": "string", "description": "Procedure name", "required": True},
                    "params": {"type": "object", "description": "Procedure parameters", "required": False},
                },
            },
            "list_tables": {
                "description": "List tables in schema",
                "parameters": {
                    "owner": {"type": "string", "description": "Schema owner", "required": False},
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
                return await self._query(params["sql"], params.get("params", {}))
            elif action == "execute":
                return await self._execute_sql(params["sql"], params.get("params", {}))
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
            elif action == "call_procedure":
                return await self._call_procedure(params["procedure"], params.get("params", {}))
            elif action == "list_tables":
                return await self._list_tables(params.get("owner"))
            elif action == "describe_table":
                return await self._describe_table(params["table"])
            else:
                return ConnectorResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ConnectorResult(success=False, error=str(e))

    async def _query(self, sql: str, params: dict) -> ConnectorResult:
        pool = await self._get_pool()
        with pool.acquire() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                columns = [col[0] for col in cursor.description] if cursor.description else []
                rows = cursor.fetchall()
                results = [dict(zip(columns, row)) for row in rows]
                return ConnectorResult(success=True, data={"rows": results, "count": len(results)})

    async def _execute_sql(self, sql: str, params: dict) -> ConnectorResult:
        pool = await self._get_pool()
        with pool.acquire() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                conn.commit()
                return ConnectorResult(success=True, data={"rows_affected": cursor.rowcount})

    async def _insert(self, table: str, data: dict) -> ConnectorResult:
        columns = ", ".join(data.keys())
        placeholders = ", ".join(f":{k}" for k in data.keys())
        sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        return await self._execute_sql(sql, data)

    async def _insert_many(self, table: str, records: list[dict]) -> ConnectorResult:
        if not records:
            return ConnectorResult(success=True, data={"inserted": 0})

        pool = await self._get_pool()
        columns = ", ".join(records[0].keys())
        placeholders = ", ".join(f":{k}" for k in records[0].keys())
        sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"

        with pool.acquire() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(sql, records)
                conn.commit()
                return ConnectorResult(success=True, data={"inserted": len(records)})

    async def _update(self, table: str, data: dict, where: str) -> ConnectorResult:
        set_clause = ", ".join(f"{k} = :{k}" for k in data.keys())
        sql = f"UPDATE {table} SET {set_clause} WHERE {where}"
        return await self._execute_sql(sql, data)

    async def _delete(self, table: str, where: str) -> ConnectorResult:
        sql = f"DELETE FROM {table} WHERE {where}"
        return await self._execute_sql(sql, {})

    async def _merge(self, table: str, data: dict, key_columns: list) -> ConnectorResult:
        columns = list(data.keys())
        source_cols = ", ".join(f":{c} AS {c}" for c in columns)
        match_cond = " AND ".join(f"target.{k} = source.{k}" for k in key_columns)
        update_cols = ", ".join(f"target.{c} = source.{c}" for c in columns if c not in key_columns)
        insert_cols = ", ".join(columns)
        insert_vals = ", ".join(f"source.{c}" for c in columns)

        sql = f"""
        MERGE INTO {table} target
        USING (SELECT {source_cols} FROM DUAL) source
        ON ({match_cond})
        WHEN MATCHED THEN UPDATE SET {update_cols}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """
        return await self._execute_sql(sql, data)

    async def _call_procedure(self, procedure: str, params: dict) -> ConnectorResult:
        pool = await self._get_pool()
        with pool.acquire() as conn:
            with conn.cursor() as cursor:
                cursor.callproc(procedure, list(params.values()))
                conn.commit()
                return ConnectorResult(success=True, data={"called": procedure})

    async def _list_tables(self, owner: str | None) -> ConnectorResult:
        if owner:
            sql = "SELECT table_name FROM all_tables WHERE owner = :owner ORDER BY table_name"
            return await self._query(sql, {"owner": owner.upper()})
        else:
            sql = "SELECT table_name FROM user_tables ORDER BY table_name"
            return await self._query(sql, {})

    async def _describe_table(self, table: str) -> ConnectorResult:
        sql = """
        SELECT column_name, data_type, data_length, nullable, data_default
        FROM user_tab_columns
        WHERE table_name = :table
        ORDER BY column_id
        """
        return await self._query(sql, {"table": table.upper()})

    async def close(self):
        if self._pool:
            self._pool.close()
            self._pool = None
