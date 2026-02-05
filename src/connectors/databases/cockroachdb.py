"""
CockroachDB Connector

Connect to CockroachDB distributed SQL database.
"""

from typing import Any
from ..base import BaseConnector, ConnectorResult


class CockroachDBConnector(BaseConnector):
    """Connector for CockroachDB."""

    def __init__(self, credentials: dict[str, Any]):
        super().__init__(credentials)
        self.connection_string = credentials.get("connection_string")
        # Or individual params
        self.host = credentials.get("host")
        self.port = credentials.get("port", 26257)
        self.database = credentials.get("database")
        self.user = credentials.get("user")
        self.password = credentials.get("password")
        self.cluster = credentials.get("cluster")  # For CockroachDB Serverless
        self._pool = None

    async def _get_pool(self):
        """Get connection pool."""
        if self._pool is None:
            import asyncpg

            if self.connection_string:
                self._pool = await asyncpg.create_pool(self.connection_string)
            else:
                # Build connection string for CockroachDB Serverless
                options = f"--cluster={self.cluster}" if self.cluster else ""
                self._pool = await asyncpg.create_pool(
                    host=self.host,
                    port=self.port,
                    database=self.database,
                    user=self.user,
                    password=self.password,
                    ssl="require",
                    server_settings={"options": options} if options else {},
                    min_size=1,
                    max_size=10,
                )
        return self._pool

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
                    "returning": {"type": "array", "description": "Columns to return", "required": False},
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
                    "where": {"type": "object", "description": "WHERE conditions", "required": True},
                },
            },
            "delete": {
                "description": "Delete rows",
                "parameters": {
                    "table": {"type": "string", "description": "Table name", "required": True},
                    "where": {"type": "object", "description": "WHERE conditions", "required": True},
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
            if action == "query":
                return await self._query(params["sql"], params.get("params", []))
            elif action == "execute":
                return await self._execute_sql(params["sql"], params.get("params", []))
            elif action == "insert":
                return await self._insert(params["table"], params["data"], params.get("returning"))
            elif action == "insert_many":
                return await self._insert_many(params["table"], params["records"])
            elif action == "update":
                return await self._update(params["table"], params["data"], params["where"])
            elif action == "delete":
                return await self._delete(params["table"], params["where"])
            elif action == "upsert":
                return await self._upsert(params["table"], params["data"], params["conflict_columns"])
            elif action == "list_tables":
                return await self._list_tables()
            elif action == "describe_table":
                return await self._describe_table(params["table"])
            else:
                return ConnectorResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ConnectorResult(success=False, error=str(e))

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
            count = int(result.split()[-1]) if result else 0
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
            count = int(result.split()[-1]) if result else 0
            return ConnectorResult(success=True, data={"deleted": count})

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

    async def _list_tables(self) -> ConnectorResult:
        sql = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        ORDER BY table_name
        """
        return await self._query(sql, [])

    async def _describe_table(self, table: str) -> ConnectorResult:
        sql = """
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_name = $1
        ORDER BY ordinal_position
        """
        return await self._query(sql, [table])

    async def close(self):
        if self._pool:
            await self._pool.close()
            self._pool = None
