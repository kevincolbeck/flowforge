"""
Box Connector

Connect to Box for file storage operations.
"""

from typing import Any
import base64
import httpx
from ..base import BaseConnector, ConnectorResult


class BoxConnector(BaseConnector):
    """Connector for Box."""

    def __init__(self, credentials: dict[str, Any]):
        super().__init__(credentials)
        self.access_token = credentials.get("access_token")
        self.base_url = "https://api.box.com/2.0"
        self.upload_url = "https://upload.box.com/api/2.0"

    def _headers(self):
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

    @classmethod
    def get_actions(cls) -> dict[str, dict[str, Any]]:
        return {
            "upload": {
                "description": "Upload a file",
                "parameters": {
                    "folder_id": {"type": "string", "description": "Parent folder ID (0 for root)", "required": True},
                    "name": {"type": "string", "description": "File name", "required": True},
                    "content": {"type": "string", "description": "Base64-encoded content", "required": True},
                },
            },
            "download": {
                "description": "Download a file",
                "parameters": {
                    "file_id": {"type": "string", "description": "File ID", "required": True},
                },
            },
            "delete": {
                "description": "Delete a file",
                "parameters": {
                    "file_id": {"type": "string", "description": "File ID", "required": True},
                },
            },
            "list_folder": {
                "description": "List items in a folder",
                "parameters": {
                    "folder_id": {"type": "string", "description": "Folder ID (0 for root)", "required": True},
                },
            },
            "create_folder": {
                "description": "Create a folder",
                "parameters": {
                    "name": {"type": "string", "description": "Folder name", "required": True},
                    "parent_id": {"type": "string", "description": "Parent folder ID", "required": True},
                },
            },
            "delete_folder": {
                "description": "Delete a folder",
                "parameters": {
                    "folder_id": {"type": "string", "description": "Folder ID", "required": True},
                    "recursive": {"type": "boolean", "description": "Delete contents too", "required": False},
                },
            },
            "copy": {
                "description": "Copy a file",
                "parameters": {
                    "file_id": {"type": "string", "description": "File ID", "required": True},
                    "parent_id": {"type": "string", "description": "Destination folder ID", "required": True},
                    "name": {"type": "string", "description": "New name (optional)", "required": False},
                },
            },
            "move": {
                "description": "Move a file",
                "parameters": {
                    "file_id": {"type": "string", "description": "File ID", "required": True},
                    "parent_id": {"type": "string", "description": "Destination folder ID", "required": True},
                },
            },
            "get_file_info": {
                "description": "Get file information",
                "parameters": {
                    "file_id": {"type": "string", "description": "File ID", "required": True},
                },
            },
            "search": {
                "description": "Search for files",
                "parameters": {
                    "query": {"type": "string", "description": "Search query", "required": True},
                    "type": {"type": "string", "description": "file or folder", "required": False},
                },
            },
            "create_shared_link": {
                "description": "Create a shared link",
                "parameters": {
                    "file_id": {"type": "string", "description": "File ID", "required": True},
                    "access": {"type": "string", "description": "open, company, or collaborators", "required": False},
                },
            },
        }

    async def execute(self, action: str, params: dict[str, Any]) -> ConnectorResult:
        try:
            if action == "upload":
                return await self._upload(params)
            elif action == "download":
                return await self._download(params["file_id"])
            elif action == "delete":
                return await self._delete(params["file_id"])
            elif action == "list_folder":
                return await self._list_folder(params["folder_id"])
            elif action == "create_folder":
                return await self._create_folder(params["name"], params["parent_id"])
            elif action == "delete_folder":
                return await self._delete_folder(params["folder_id"], params.get("recursive", True))
            elif action == "copy":
                return await self._copy(params["file_id"], params["parent_id"], params.get("name"))
            elif action == "move":
                return await self._move(params["file_id"], params["parent_id"])
            elif action == "get_file_info":
                return await self._get_file_info(params["file_id"])
            elif action == "search":
                return await self._search(params["query"], params.get("type"))
            elif action == "create_shared_link":
                return await self._create_shared_link(params["file_id"], params.get("access", "open"))
            else:
                return ConnectorResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ConnectorResult(success=False, error=str(e))

    async def _upload(self, params: dict) -> ConnectorResult:
        content = base64.b64decode(params["content"])

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.upload_url}/files/content",
                headers={"Authorization": f"Bearer {self.access_token}"},
                data={
                    "attributes": f'{{"name":"{params["name"]}","parent":{{"id":"{params["folder_id"]}"}}}}'
                },
                files={"file": (params["name"], content)},
            )
            response.raise_for_status()
            data = response.json()
            entry = data["entries"][0]
            return ConnectorResult(success=True, data={"id": entry["id"], "name": entry["name"]})

    async def _download(self, file_id: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/files/{file_id}/content",
                headers=self._headers(),
                follow_redirects=True,
            )
            response.raise_for_status()
            content = response.content
            return ConnectorResult(
                success=True,
                data={"content": base64.b64encode(content).decode()}
            )

    async def _delete(self, file_id: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.delete(
                f"{self.base_url}/files/{file_id}",
                headers=self._headers(),
            )
            response.raise_for_status()
            return ConnectorResult(success=True, data={"deleted": file_id})

    async def _list_folder(self, folder_id: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/folders/{folder_id}/items",
                headers=self._headers(),
            )
            response.raise_for_status()
            data = response.json()

            items = [
                {
                    "id": item["id"],
                    "name": item["name"],
                    "type": item["type"],
                }
                for item in data.get("entries", [])
            ]
            return ConnectorResult(success=True, data={"items": items, "count": len(items)})

    async def _create_folder(self, name: str, parent_id: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/folders",
                headers=self._headers(),
                json={"name": name, "parent": {"id": parent_id}},
            )
            response.raise_for_status()
            data = response.json()
            return ConnectorResult(success=True, data={"id": data["id"], "name": data["name"]})

    async def _delete_folder(self, folder_id: str, recursive: bool) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.delete(
                f"{self.base_url}/folders/{folder_id}",
                headers=self._headers(),
                params={"recursive": str(recursive).lower()},
            )
            response.raise_for_status()
            return ConnectorResult(success=True, data={"deleted": folder_id})

    async def _copy(self, file_id: str, parent_id: str, name: str | None) -> ConnectorResult:
        body = {"parent": {"id": parent_id}}
        if name:
            body["name"] = name

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/files/{file_id}/copy",
                headers=self._headers(),
                json=body,
            )
            response.raise_for_status()
            data = response.json()
            return ConnectorResult(success=True, data={"id": data["id"], "name": data["name"]})

    async def _move(self, file_id: str, parent_id: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.put(
                f"{self.base_url}/files/{file_id}",
                headers=self._headers(),
                json={"parent": {"id": parent_id}},
            )
            response.raise_for_status()
            data = response.json()
            return ConnectorResult(success=True, data={"id": data["id"], "name": data["name"]})

    async def _get_file_info(self, file_id: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/files/{file_id}",
                headers=self._headers(),
            )
            response.raise_for_status()
            data = response.json()
            return ConnectorResult(
                success=True,
                data={
                    "id": data["id"],
                    "name": data["name"],
                    "size": data.get("size"),
                    "created_at": data.get("created_at"),
                    "modified_at": data.get("modified_at"),
                }
            )

    async def _search(self, query: str, type: str | None) -> ConnectorResult:
        params = {"query": query}
        if type:
            params["type"] = type

        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/search",
                headers=self._headers(),
                params=params,
            )
            response.raise_for_status()
            data = response.json()

            results = [
                {"id": item["id"], "name": item["name"], "type": item["type"]}
                for item in data.get("entries", [])
            ]
            return ConnectorResult(success=True, data={"results": results, "count": len(results)})

    async def _create_shared_link(self, file_id: str, access: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.put(
                f"{self.base_url}/files/{file_id}",
                headers=self._headers(),
                json={"shared_link": {"access": access}},
            )
            response.raise_for_status()
            data = response.json()
            return ConnectorResult(success=True, data={"url": data["shared_link"]["url"]})
