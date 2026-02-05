"""
PostgreSQL Connector

Connect to PostgreSQL databases including Neon, Supabase Postgres, etc.
"""

from typing import Any
from ..base import BaseConnector, ConnectorResult


class PostgreSQLConnector(BaseConnector):
    """Connector for PostgreSQL databases."""

    def __init__(self, credentials: dict[str, Any]):
        super().__init__(credentials)
        self.host = credentials.get("host", "localhost")
        self.port = credentials.get("port", 5432)
        self.database = credentials.get("database")
        self.user = credentials.get("user")
        self.password = credentials.get("password")
        self.ssl = credentials.get("ssl", True)
        self._pool = None

    async def _get_pool(self):
        """Get or create connection pool."""
        if self._pool is None:
            import asyncpg
            self._pool = await asyncpg.create_pool(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                ssl="require" if self.ssl else None,
                min_size=1,
                max_size=10,
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
                    "returning": {"type": "array", "description": "Columns to return", "required": False},
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
                "description": "Update records in a table",
                "parameters": {
                    "table": {"type": "string", "description": "Table name", "required": True},
                    "data": {"type": "object", "description": "Column-value pairs", "required": True},
                    "where": {"type": "object", "description": "WHERE conditions", "required": True},
                },
            },
            "delete": {
                "description": "Delete records from a table",
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
                "description": "Insert or update on conflict",
                "parameters": {
                    "table": {"type": "string", "description": "Table name", "required": True},
                    "data": {"type": "object", "description": "Column-value pairs", "required": True},
                    "conflict_columns": {"type": "array", "description": "Columns for conflict detection", "required": True},
                },
            },
            "copy_from": {
                "description": "Bulk copy data into a table",
                "parameters": {
                    "table": {"type": "string", "description": "Table name", "required": True},
                    "records": {"type": "array", "description": "Array of records", "required": True},
                    "columns": {"type": "array", "description": "Column names", "required": True},
                },
            },
            "list_tables": {
                "description": "List all tables",
                "parameters": {
                    "schema": {"type": "string", "description": "Schema name (default: public)", "required": False},
                },
            },
            "describe_table": {
                "description": "Get table schema",
                "parameters": {
                    "table": {"type": "string", "description": "Table name", "required": True},
                    "schema": {"type": "string", "description": "Schema name", "required": False},
                },
            },
        }

    async def execute(self, action: str, params: dict[str, Any]) -> ConnectorResult:
        try:
            if action == "insert":
                return await self._insert(
                    params["table"], params["data"], params.get("returning")
                )
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
                return await self._upsert(
                    params["table"], params["data"], params["conflict_columns"]
                )
            elif action == "copy_from":
                return await self._copy_from(
                    params["table"], params["records"], params["columns"]
                )
            elif action == "list_tables":
                return await self._list_tables(params.get("schema", "public"))
            elif action == "describe_table":
                return await self._describe_table(
                    params["table"], params.get("schema", "public")
                )
            else:
                return ConnectorResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ConnectorResult(success=False, error=str(e))

    async def _insert(self, table: str, data: dict, returning: list | None) -> ConnectorResult:
        pool = await self._get_pool()
        columns = ", ".join(f'"{k}"' for k in data.keys())
        placeholders = ", ".join(f"${i+1}" for i in range(len(data)))
        sql = f'INSERT INTO "{table}" ({columns}) VALUES ({placeholders})'

        if returning:
            sql += f' RETURNING {", ".join(returning)}'
            async with pool.acquire() as conn:
                row = await conn.fetchrow(sql, *data.values())
                return ConnectorResult(success=True, data={"inserted": dict(row) if row else None})
        else:
            async with pool.acquire() as conn:
                await conn.execute(sql, *data.values())
                return ConnectorResult(success=True, data={"inserted": 1})

    async def _insert_many(self, table: str, records: list[dict]) -> ConnectorResult:
        if not records:
            return ConnectorResult(success=True, data={"inserted": 0})

        pool = await self._get_pool()
        columns = list(records[0].keys())
        col_str = ", ".join(f'"{c}"' for c in columns)

        async with pool.acquire() as conn:
            await conn.copy_records_to_table(
                table,
                records=[tuple(r[c] for c in columns) for r in records],
                columns=columns,
            )

        return ConnectorResult(success=True, data={"inserted": len(records)})

    async def _update(self, table: str, data: dict, where: dict) -> ConnectorResult:
        pool = await self._get_pool()

        set_parts = []
        values = []
        idx = 1
        for k, v in data.items():
            set_parts.append(f'"{k}" = ${idx}')
            values.append(v)
            idx += 1

        where_parts = []
        for k, v in where.items():
            where_parts.append(f'"{k}" = ${idx}')
            values.append(v)
            idx += 1

        sql = f'UPDATE "{table}" SET {", ".join(set_parts)} WHERE {" AND ".join(where_parts)}'

        async with pool.acquire() as conn:
            result = await conn.execute(sql, *values)
            count = int(result.split()[-1])
            return ConnectorResult(success=True, data={"updated": count})

    async def _delete(self, table: str, where: dict) -> ConnectorResult:
        pool = await self._get_pool()

        where_parts = []
        values = []
        for idx, (k, v) in enumerate(where.items(), 1):
            where_parts.append(f'"{k}" = ${idx}')
            values.append(v)

        sql = f'DELETE FROM "{table}" WHERE {" AND ".join(where_parts)}'

        async with pool.acquire() as conn:
            result = await conn.execute(sql, *values)
            count = int(result.split()[-1])
            return ConnectorResult(success=True, data={"deleted": count})

    async def _query(self, sql: str, params: list) -> ConnectorResult:
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)
            return ConnectorResult(
                success=True,
                data={"rows": [dict(r) for r in rows], "count": len(rows)}
            )

    async def _execute_sql(self, sql: str, params: list) -> ConnectorResult:
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            result = await conn.execute(sql, *params)
            return ConnectorResult(success=True, data={"result": result})

    async def _upsert(self, table: str, data: dict, conflict_columns: list) -> ConnectorResult:
        pool = await self._get_pool()

        columns = ", ".join(f'"{k}"' for k in data.keys())
        placeholders = ", ".join(f"${i+1}" for i in range(len(data)))
        conflict = ", ".join(f'"{c}"' for c in conflict_columns)

        update_parts = [f'"{k}" = EXCLUDED."{k}"' for k in data.keys() if k not in conflict_columns]

        sql = f'''
        INSERT INTO "{table}" ({columns}) VALUES ({placeholders})
        ON CONFLICT ({conflict}) DO UPDATE SET {", ".join(update_parts)}
        '''

        async with pool.acquire() as conn:
            await conn.execute(sql, *data.values())
            return ConnectorResult(success=True, data={"upserted": 1})

    async def _copy_from(self, table: str, records: list[dict], columns: list) -> ConnectorResult:
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            await conn.copy_records_to_table(
                table,
                records=[tuple(r.get(c) for c in columns) for r in records],
                columns=columns,
            )
        return ConnectorResult(success=True, data={"copied": len(records)})

    async def _list_tables(self, schema: str) -> ConnectorResult:
        sql = """
        SELECT table_name, table_type
        FROM information_schema.tables
        WHERE table_schema = $1
        ORDER BY table_name
        """
        return await self._query(sql, [schema])

    async def _describe_table(self, table: str, schema: str) -> ConnectorResult:
        sql = """
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = $2
        ORDER BY ordinal_position
        """
        return await self._query(sql, [schema, table])

    async def close(self):
        if self._pool:
            await self._pool.close()
            self._pool = None
