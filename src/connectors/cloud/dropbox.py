"""
Dropbox Connector

Connect to Dropbox for file storage operations.
"""

from typing import Any
import base64
import httpx
from ..base import BaseConnector, ConnectorResult


class DropboxConnector(BaseConnector):
    """Connector for Dropbox."""

    def __init__(self, credentials: dict[str, Any]):
        super().__init__(credentials)
        self.access_token = credentials.get("access_token")
        self.base_url = "https://api.dropboxapi.com/2"
        self.content_url = "https://content.dropboxapi.com/2"

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
                    "path": {"type": "string", "description": "File path (e.g., /folder/file.txt)", "required": True},
                    "content": {"type": "string", "description": "Base64-encoded content", "required": True},
                    "mode": {"type": "string", "description": "add, overwrite, or update", "required": False},
                },
            },
            "download": {
                "description": "Download a file",
                "parameters": {
                    "path": {"type": "string", "description": "File path", "required": True},
                },
            },
            "delete": {
                "description": "Delete a file or folder",
                "parameters": {
                    "path": {"type": "string", "description": "Path to delete", "required": True},
                },
            },
            "list_folder": {
                "description": "List files in a folder",
                "parameters": {
                    "path": {"type": "string", "description": "Folder path (empty for root)", "required": False},
                    "recursive": {"type": "boolean", "description": "Include subfolders", "required": False},
                },
            },
            "create_folder": {
                "description": "Create a folder",
                "parameters": {
                    "path": {"type": "string", "description": "Folder path", "required": True},
                },
            },
            "move": {
                "description": "Move a file or folder",
                "parameters": {
                    "from_path": {"type": "string", "description": "Source path", "required": True},
                    "to_path": {"type": "string", "description": "Destination path", "required": True},
                },
            },
            "copy": {
                "description": "Copy a file or folder",
                "parameters": {
                    "from_path": {"type": "string", "description": "Source path", "required": True},
                    "to_path": {"type": "string", "description": "Destination path", "required": True},
                },
            },
            "get_metadata": {
                "description": "Get file/folder metadata",
                "parameters": {
                    "path": {"type": "string", "description": "Path", "required": True},
                },
            },
            "search": {
                "description": "Search for files",
                "parameters": {
                    "query": {"type": "string", "description": "Search query", "required": True},
                    "path": {"type": "string", "description": "Path to search in", "required": False},
                },
            },
            "get_shared_link": {
                "description": "Create a shared link",
                "parameters": {
                    "path": {"type": "string", "description": "File path", "required": True},
                },
            },
        }

    async def execute(self, action: str, params: dict[str, Any]) -> ConnectorResult:
        try:
            if action == "upload":
                return await self._upload(params)
            elif action == "download":
                return await self._download(params["path"])
            elif action == "delete":
                return await self._delete(params["path"])
            elif action == "list_folder":
                return await self._list_folder(params.get("path", ""), params.get("recursive", False))
            elif action == "create_folder":
                return await self._create_folder(params["path"])
            elif action == "move":
                return await self._move(params["from_path"], params["to_path"])
            elif action == "copy":
                return await self._copy(params["from_path"], params["to_path"])
            elif action == "get_metadata":
                return await self._get_metadata(params["path"])
            elif action == "search":
                return await self._search(params["query"], params.get("path", ""))
            elif action == "get_shared_link":
                return await self._get_shared_link(params["path"])
            else:
                return ConnectorResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ConnectorResult(success=False, error=str(e))

    async def _upload(self, params: dict) -> ConnectorResult:
        content = base64.b64decode(params["content"])
        import json

        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/octet-stream",
            "Dropbox-API-Arg": json.dumps({
                "path": params["path"],
                "mode": params.get("mode", "overwrite"),
                "autorename": True,
            }),
        }

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.content_url}/files/upload",
                headers=headers,
                content=content,
            )
            response.raise_for_status()
            data = response.json()
            return ConnectorResult(success=True, data={"path": data["path_display"], "id": data["id"]})

    async def _download(self, path: str) -> ConnectorResult:
        import json

        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Dropbox-API-Arg": json.dumps({"path": path}),
        }

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.content_url}/files/download",
                headers=headers,
            )
            response.raise_for_status()
            content = response.content
            metadata = json.loads(response.headers.get("Dropbox-API-Result", "{}"))

            return ConnectorResult(
                success=True,
                data={
                    "content": base64.b64encode(content).decode(),
                    "name": metadata.get("name"),
                    "size": metadata.get("size"),
                }
            )

    async def _delete(self, path: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/files/delete_v2",
                headers=self._headers(),
                json={"path": path},
            )
            response.raise_for_status()
            return ConnectorResult(success=True, data={"deleted": path})

    async def _list_folder(self, path: str, recursive: bool) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/files/list_folder",
                headers=self._headers(),
                json={"path": path or "", "recursive": recursive},
            )
            response.raise_for_status()
            data = response.json()

            entries = [
                {
                    "name": e["name"],
                    "path": e["path_display"],
                    "type": e[".tag"],
                    "size": e.get("size"),
                    "modified": e.get("server_modified"),
                }
                for e in data.get("entries", [])
            ]
            return ConnectorResult(success=True, data={"entries": entries, "count": len(entries)})

    async def _create_folder(self, path: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/files/create_folder_v2",
                headers=self._headers(),
                json={"path": path},
            )
            response.raise_for_status()
            data = response.json()
            return ConnectorResult(success=True, data={"path": data["metadata"]["path_display"]})

    async def _move(self, from_path: str, to_path: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/files/move_v2",
                headers=self._headers(),
                json={"from_path": from_path, "to_path": to_path},
            )
            response.raise_for_status()
            data = response.json()
            return ConnectorResult(success=True, data={"path": data["metadata"]["path_display"]})

    async def _copy(self, from_path: str, to_path: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/files/copy_v2",
                headers=self._headers(),
                json={"from_path": from_path, "to_path": to_path},
            )
            response.raise_for_status()
            data = response.json()
            return ConnectorResult(success=True, data={"path": data["metadata"]["path_display"]})

    async def _get_metadata(self, path: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/files/get_metadata",
                headers=self._headers(),
                json={"path": path},
            )
            response.raise_for_status()
            data = response.json()
            return ConnectorResult(
                success=True,
                data={
                    "name": data["name"],
                    "path": data["path_display"],
                    "type": data[".tag"],
                    "size": data.get("size"),
                    "modified": data.get("server_modified"),
                }
            )

    async def _search(self, query: str, path: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            body = {"query": query}
            if path:
                body["options"] = {"path": path}

            response = await client.post(
                f"{self.base_url}/files/search_v2",
                headers=self._headers(),
                json=body,
            )
            response.raise_for_status()
            data = response.json()

            matches = [
                {
                    "name": m["metadata"]["metadata"]["name"],
                    "path": m["metadata"]["metadata"]["path_display"],
                }
                for m in data.get("matches", [])
            ]
            return ConnectorResult(success=True, data={"matches": matches, "count": len(matches)})

    async def _get_shared_link(self, path: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/sharing/create_shared_link_with_settings",
                headers=self._headers(),
                json={"path": path},
            )
            response.raise_for_status()
            data = response.json()
            return ConnectorResult(success=True, data={"url": data["url"]})
