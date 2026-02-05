"""
Apache Cassandra Connector

Connect to Cassandra and DataStax Astra for distributed data operations.
"""

from typing import Any
from ..base import BaseConnector, ConnectorResult


class CassandraConnector(BaseConnector):
    """Connector for Apache Cassandra."""

    def __init__(self, credentials: dict[str, Any]):
        super().__init__(credentials)
        self.contact_points = credentials.get("contact_points", ["localhost"])
        self.port = credentials.get("port", 9042)
        self.keyspace = credentials.get("keyspace")
        self.username = credentials.get("username")
        self.password = credentials.get("password")
        # For DataStax Astra
        self.secure_connect_bundle = credentials.get("secure_connect_bundle")
        self.client_id = credentials.get("client_id")
        self.client_secret = credentials.get("client_secret")
        self._session = None

    async def _get_session(self):
        """Get Cassandra session."""
        if self._session is None:
            from cassandra.cluster import Cluster
            from cassandra.auth import PlainTextAuthProvider

            if self.secure_connect_bundle:
                # DataStax Astra connection
                from cassandra.cloud import connect
                auth_provider = PlainTextAuthProvider(self.client_id, self.client_secret)
                cluster = Cluster(
                    cloud={"secure_connect_bundle": self.secure_connect_bundle},
                    auth_provider=auth_provider,
                )
            else:
                # Standard Cassandra connection
                auth_provider = None
                if self.username and self.password:
                    auth_provider = PlainTextAuthProvider(self.username, self.password)

                cluster = Cluster(
                    contact_points=self.contact_points,
                    port=self.port,
                    auth_provider=auth_provider,
                )

            self._session = cluster.connect(self.keyspace)
        return self._session

    @classmethod
    def get_actions(cls) -> dict[str, dict[str, Any]]:
        return {
            "execute": {
                "description": "Execute a CQL query",
                "parameters": {
                    "cql": {"type": "string", "description": "CQL query", "required": True},
                    "params": {"type": "array", "description": "Query parameters", "required": False},
                },
            },
            "insert": {
                "description": "Insert a row",
                "parameters": {
                    "table": {"type": "string", "description": "Table name", "required": True},
                    "data": {"type": "object", "description": "Column-value pairs", "required": True},
                    "ttl": {"type": "integer", "description": "Time to live in seconds", "required": False},
                },
            },
            "insert_many": {
                "description": "Batch insert rows",
                "parameters": {
                    "table": {"type": "string", "description": "Table name", "required": True},
                    "records": {"type": "array", "description": "Array of records", "required": True},
                },
            },
            "select": {
                "description": "Select rows from a table",
                "parameters": {
                    "table": {"type": "string", "description": "Table name", "required": True},
                    "columns": {"type": "array", "description": "Columns to select", "required": False},
                    "where": {"type": "object", "description": "WHERE conditions", "required": False},
                    "limit": {"type": "integer", "description": "Max rows", "required": False},
                },
            },
            "update": {
                "description": "Update rows",
                "parameters": {
                    "table": {"type": "string", "description": "Table name", "required": True},
                    "data": {"type": "object", "description": "Column-value pairs", "required": True},
                    "where": {"type": "object", "description": "WHERE conditions (primary key)", "required": True},
                },
            },
            "delete": {
                "description": "Delete rows",
                "parameters": {
                    "table": {"type": "string", "description": "Table name", "required": True},
                    "where": {"type": "object", "description": "WHERE conditions (primary key)", "required": True},
                },
            },
            "list_tables": {
                "description": "List tables in keyspace",
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
                    "primary_key": {"type": "array", "description": "Primary key columns", "required": True},
                },
            },
        }

    async def execute(self, action: str, params: dict[str, Any]) -> ConnectorResult:
        try:
            session = await self._get_session()

            if action == "execute":
                return await self._execute(session, params["cql"], params.get("params", []))
            elif action == "insert":
                return await self._insert(session, params["table"], params["data"], params.get("ttl"))
            elif action == "insert_many":
                return await self._insert_many(session, params["table"], params["records"])
            elif action == "select":
                return await self._select(session, params)
            elif action == "update":
                return await self._update(session, params["table"], params["data"], params["where"])
            elif action == "delete":
                return await self._delete(session, params["table"], params["where"])
            elif action == "list_tables":
                return await self._list_tables(session)
            elif action == "describe_table":
                return await self._describe_table(session, params["table"])
            elif action == "create_table":
                return await self._create_table(
                    session, params["table"], params["columns"], params["primary_key"]
                )
            else:
                return ConnectorResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ConnectorResult(success=False, error=str(e))

    async def _execute(self, session, cql: str, params: list) -> ConnectorResult:
        result = session.execute(cql, params)
        rows = [dict(row._asdict()) for row in result]
        return ConnectorResult(success=True, data={"rows": rows, "count": len(rows)})

    async def _insert(self, session, table: str, data: dict, ttl: int | None) -> ConnectorResult:
        columns = ", ".join(data.keys())
        placeholders = ", ".join("%s" for _ in data)
        cql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"

        if ttl:
            cql += f" USING TTL {ttl}"

        session.execute(cql, list(data.values()))
        return ConnectorResult(success=True, data={"inserted": 1})

    async def _insert_many(self, session, table: str, records: list[dict]) -> ConnectorResult:
        if not records:
            return ConnectorResult(success=True, data={"inserted": 0})

        from cassandra.query import BatchStatement, BatchType

        columns = ", ".join(records[0].keys())
        placeholders = ", ".join("%s" for _ in records[0])
        cql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"

        prepared = session.prepare(cql)
        batch = BatchStatement(batch_type=BatchType.UNLOGGED)

        for record in records:
            batch.add(prepared, list(record.values()))

        session.execute(batch)
        return ConnectorResult(success=True, data={"inserted": len(records)})

    async def _select(self, session, params: dict) -> ConnectorResult:
        table = params["table"]
        columns = params.get("columns", ["*"])
        col_str = ", ".join(columns) if columns != ["*"] else "*"

        cql = f"SELECT {col_str} FROM {table}"
        values = []

        if params.get("where"):
            where_parts = [f"{k} = %s" for k in params["where"].keys()]
            cql += f" WHERE {' AND '.join(where_parts)}"
            values = list(params["where"].values())

        if params.get("limit"):
            cql += f" LIMIT {params['limit']}"

        result = session.execute(cql, values)
        rows = [dict(row._asdict()) for row in result]
        return ConnectorResult(success=True, data={"rows": rows, "count": len(rows)})

    async def _update(self, session, table: str, data: dict, where: dict) -> ConnectorResult:
        set_parts = [f"{k} = %s" for k in data.keys()]
        where_parts = [f"{k} = %s" for k in where.keys()]
        values = list(data.values()) + list(where.values())

        cql = f"UPDATE {table} SET {', '.join(set_parts)} WHERE {' AND '.join(where_parts)}"
        session.execute(cql, values)
        return ConnectorResult(success=True, data={"updated": True})

    async def _delete(self, session, table: str, where: dict) -> ConnectorResult:
        where_parts = [f"{k} = %s" for k in where.keys()]
        cql = f"DELETE FROM {table} WHERE {' AND '.join(where_parts)}"
        session.execute(cql, list(where.values()))
        return ConnectorResult(success=True, data={"deleted": True})

    async def _list_tables(self, session) -> ConnectorResult:
        cql = f"""
        SELECT table_name FROM system_schema.tables
        WHERE keyspace_name = '{self.keyspace}'
        """
        result = session.execute(cql)
        tables = [row.table_name for row in result]
        return ConnectorResult(success=True, data={"tables": tables})

    async def _describe_table(self, session, table: str) -> ConnectorResult:
        cql = f"""
        SELECT column_name, type, kind FROM system_schema.columns
        WHERE keyspace_name = '{self.keyspace}' AND table_name = '{table}'
        """
        result = session.execute(cql)
        columns = [{"name": row.column_name, "type": row.type, "kind": row.kind} for row in result]
        return ConnectorResult(success=True, data={"columns": columns})

    async def _create_table(self, session, table: str, columns: dict, primary_key: list) -> ConnectorResult:
        col_defs = [f"{name} {col_type}" for name, col_type in columns.items()]
        pk_str = ", ".join(primary_key)

        cql = f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(col_defs)}, PRIMARY KEY ({pk_str}))"
        session.execute(cql)
        return ConnectorResult(success=True, data={"created": table})

    async def close(self):
        if self._session:
            self._session.cluster.shutdown()
            self._session = None
