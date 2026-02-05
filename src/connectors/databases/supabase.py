"""
Supabase Connector

Connect to Supabase for database, auth, and storage operations.
"""

from typing import Any
import httpx
from ..base import BaseConnector, ConnectorResult


class SupabaseConnector(BaseConnector):
    """Connector for Supabase."""

    def __init__(self, credentials: dict[str, Any]):
        super().__init__(credentials)
        self.url = credentials.get("url")  # https://xxx.supabase.co
        self.api_key = credentials.get("api_key")  # anon or service_role key
        self.headers = {
            "apikey": self.api_key,
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "Prefer": "return=representation",
        }

    @classmethod
    def get_actions(cls) -> dict[str, dict[str, Any]]:
        return {
            "select": {
                "description": "Select rows from a table",
                "parameters": {
                    "table": {"type": "string", "description": "Table name", "required": True},
                    "columns": {"type": "string", "description": "Columns to select (default: *)", "required": False},
                    "filters": {"type": "object", "description": "Filter conditions", "required": False},
                    "order": {"type": "string", "description": "Order by column", "required": False},
                    "limit": {"type": "integer", "description": "Limit rows", "required": False},
                    "offset": {"type": "integer", "description": "Offset rows", "required": False},
                },
            },
            "insert": {
                "description": "Insert rows into a table",
                "parameters": {
                    "table": {"type": "string", "description": "Table name", "required": True},
                    "data": {"type": "object", "description": "Data to insert (object or array)", "required": True},
                },
            },
            "update": {
                "description": "Update rows in a table",
                "parameters": {
                    "table": {"type": "string", "description": "Table name", "required": True},
                    "data": {"type": "object", "description": "Data to update", "required": True},
                    "filters": {"type": "object", "description": "Filter conditions", "required": True},
                },
            },
            "delete": {
                "description": "Delete rows from a table",
                "parameters": {
                    "table": {"type": "string", "description": "Table name", "required": True},
                    "filters": {"type": "object", "description": "Filter conditions", "required": True},
                },
            },
            "upsert": {
                "description": "Insert or update rows",
                "parameters": {
                    "table": {"type": "string", "description": "Table name", "required": True},
                    "data": {"type": "object", "description": "Data to upsert", "required": True},
                },
            },
            "rpc": {
                "description": "Call a stored procedure/function",
                "parameters": {
                    "function": {"type": "string", "description": "Function name", "required": True},
                    "params": {"type": "object", "description": "Function parameters", "required": False},
                },
            },
            "storage_upload": {
                "description": "Upload a file to storage",
                "parameters": {
                    "bucket": {"type": "string", "description": "Bucket name", "required": True},
                    "path": {"type": "string", "description": "File path", "required": True},
                    "file_content": {"type": "string", "description": "Base64 encoded file content", "required": True},
                    "content_type": {"type": "string", "description": "MIME type", "required": False},
                },
            },
            "storage_download": {
                "description": "Download a file from storage",
                "parameters": {
                    "bucket": {"type": "string", "description": "Bucket name", "required": True},
                    "path": {"type": "string", "description": "File path", "required": True},
                },
            },
            "storage_list": {
                "description": "List files in a bucket",
                "parameters": {
                    "bucket": {"type": "string", "description": "Bucket name", "required": True},
                    "prefix": {"type": "string", "description": "Path prefix", "required": False},
                },
            },
            "storage_delete": {
                "description": "Delete a file from storage",
                "parameters": {
                    "bucket": {"type": "string", "description": "Bucket name", "required": True},
                    "paths": {"type": "array", "description": "File paths to delete", "required": True},
                },
            },
        }

    async def execute(self, action: str, params: dict[str, Any]) -> ConnectorResult:
        try:
            if action == "select":
                return await self._select(params)
            elif action == "insert":
                return await self._insert(params["table"], params["data"])
            elif action == "update":
                return await self._update(params["table"], params["data"], params["filters"])
            elif action == "delete":
                return await self._delete(params["table"], params["filters"])
            elif action == "upsert":
                return await self._upsert(params["table"], params["data"])
            elif action == "rpc":
                return await self._rpc(params["function"], params.get("params", {}))
            elif action == "storage_upload":
                return await self._storage_upload(params["bucket"], params["path"],
                                                  params["file_content"], params.get("content_type"))
            elif action == "storage_download":
                return await self._storage_download(params["bucket"], params["path"])
            elif action == "storage_list":
                return await self._storage_list(params["bucket"], params.get("prefix", ""))
            elif action == "storage_delete":
                return await self._storage_delete(params["bucket"], params["paths"])
            else:
                return ConnectorResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ConnectorResult(success=False, error=str(e))

    def _build_filter_query(self, filters: dict) -> str:
        """Build PostgREST filter query string."""
        parts = []
        for key, value in filters.items():
            if isinstance(value, dict):
                # Handle operators like {"gt": 5}, {"in": [1,2,3]}
                for op, val in value.items():
                    if op == "in":
                        parts.append(f"{key}=in.({','.join(map(str, val))})")
                    else:
                        parts.append(f"{key}={op}.{val}")
            else:
                parts.append(f"{key}=eq.{value}")
        return "&".join(parts)

    async def _select(self, params: dict) -> ConnectorResult:
        table = params["table"]
        columns = params.get("columns", "*")

        url = f"{self.url}/rest/v1/{table}?select={columns}"

        if params.get("filters"):
            url += "&" + self._build_filter_query(params["filters"])
        if params.get("order"):
            url += f"&order={params['order']}"
        if params.get("limit"):
            url += f"&limit={params['limit']}"
        if params.get("offset"):
            url += f"&offset={params['offset']}"

        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=self.headers)
            response.raise_for_status()
            data = response.json()
            return ConnectorResult(success=True, data={"rows": data, "count": len(data)})

    async def _insert(self, table: str, data: Any) -> ConnectorResult:
        url = f"{self.url}/rest/v1/{table}"

        async with httpx.AsyncClient() as client:
            response = await client.post(url, headers=self.headers, json=data)
            response.raise_for_status()
            result = response.json()
            return ConnectorResult(success=True, data={"inserted": result})

    async def _update(self, table: str, data: dict, filters: dict) -> ConnectorResult:
        url = f"{self.url}/rest/v1/{table}?" + self._build_filter_query(filters)

        async with httpx.AsyncClient() as client:
            response = await client.patch(url, headers=self.headers, json=data)
            response.raise_for_status()
            result = response.json()
            return ConnectorResult(success=True, data={"updated": result})

    async def _delete(self, table: str, filters: dict) -> ConnectorResult:
        url = f"{self.url}/rest/v1/{table}?" + self._build_filter_query(filters)

        async with httpx.AsyncClient() as client:
            response = await client.delete(url, headers=self.headers)
            response.raise_for_status()
            result = response.json()
            return ConnectorResult(success=True, data={"deleted": result})

    async def _upsert(self, table: str, data: Any) -> ConnectorResult:
        url = f"{self.url}/rest/v1/{table}"
        headers = {**self.headers, "Prefer": "resolution=merge-duplicates,return=representation"}

        async with httpx.AsyncClient() as client:
            response = await client.post(url, headers=headers, json=data)
            response.raise_for_status()
            result = response.json()
            return ConnectorResult(success=True, data={"upserted": result})

    async def _rpc(self, function: str, params: dict) -> ConnectorResult:
        url = f"{self.url}/rest/v1/rpc/{function}"

        async with httpx.AsyncClient() as client:
            response = await client.post(url, headers=self.headers, json=params)
            response.raise_for_status()
            result = response.json()
            return ConnectorResult(success=True, data={"result": result})

    async def _storage_upload(self, bucket: str, path: str, file_content: str, content_type: str | None) -> ConnectorResult:
        import base64
        url = f"{self.url}/storage/v1/object/{bucket}/{path}"

        file_bytes = base64.b64decode(file_content)
        headers = {**self.headers}
        if content_type:
            headers["Content-Type"] = content_type

        async with httpx.AsyncClient() as client:
            response = await client.post(url, headers=headers, content=file_bytes)
            response.raise_for_status()
            return ConnectorResult(success=True, data={"path": f"{bucket}/{path}"})

    async def _storage_download(self, bucket: str, path: str) -> ConnectorResult:
        import base64
        url = f"{self.url}/storage/v1/object/{bucket}/{path}"

        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=self.headers)
            response.raise_for_status()
            content = base64.b64encode(response.content).decode()
            return ConnectorResult(success=True, data={"content": content, "content_type": response.headers.get("content-type")})

    async def _storage_list(self, bucket: str, prefix: str) -> ConnectorResult:
        url = f"{self.url}/storage/v1/object/list/{bucket}"

        async with httpx.AsyncClient() as client:
            response = await client.post(url, headers=self.headers, json={"prefix": prefix})
            response.raise_for_status()
            files = response.json()
            return ConnectorResult(success=True, data={"files": files})

    async def _storage_delete(self, bucket: str, paths: list) -> ConnectorResult:
        url = f"{self.url}/storage/v1/object/{bucket}"

        async with httpx.AsyncClient() as client:
            response = await client.delete(url, headers=self.headers, json={"prefixes": paths})
            response.raise_for_status()
            return ConnectorResult(success=True, data={"deleted": paths})
