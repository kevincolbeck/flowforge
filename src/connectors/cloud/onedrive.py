"""
OneDrive Connector

Connect to Microsoft OneDrive for file storage operations.
"""

from typing import Any
import base64
import httpx
from ..base import BaseConnector, ConnectorResult


class OneDriveConnector(BaseConnector):
    """Connector for Microsoft OneDrive."""

    def __init__(self, credentials: dict[str, Any]):
        super().__init__(credentials)
        self.access_token = credentials.get("access_token")
        self.base_url = "https://graph.microsoft.com/v1.0"

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
                    "path": {"type": "string", "description": "File path in OneDrive", "required": True},
                    "content": {"type": "string", "description": "Base64-encoded content", "required": True},
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
                "description": "List items in a folder",
                "parameters": {
                    "path": {"type": "string", "description": "Folder path (empty for root)", "required": False},
                },
            },
            "create_folder": {
                "description": "Create a folder",
                "parameters": {
                    "path": {"type": "string", "description": "Parent folder path", "required": True},
                    "name": {"type": "string", "description": "Folder name", "required": True},
                },
            },
            "copy": {
                "description": "Copy a file or folder",
                "parameters": {
                    "path": {"type": "string", "description": "Source path", "required": True},
                    "dest_path": {"type": "string", "description": "Destination folder path", "required": True},
                    "new_name": {"type": "string", "description": "New name (optional)", "required": False},
                },
            },
            "move": {
                "description": "Move a file or folder",
                "parameters": {
                    "path": {"type": "string", "description": "Source path", "required": True},
                    "dest_path": {"type": "string", "description": "Destination folder path", "required": True},
                },
            },
            "get_item": {
                "description": "Get item metadata",
                "parameters": {
                    "path": {"type": "string", "description": "Path", "required": True},
                },
            },
            "search": {
                "description": "Search for files",
                "parameters": {
                    "query": {"type": "string", "description": "Search query", "required": True},
                },
            },
            "create_sharing_link": {
                "description": "Create a sharing link",
                "parameters": {
                    "path": {"type": "string", "description": "File path", "required": True},
                    "type": {"type": "string", "description": "view or edit", "required": False},
                    "scope": {"type": "string", "description": "anonymous or organization", "required": False},
                },
            },
        }

    async def execute(self, action: str, params: dict[str, Any]) -> ConnectorResult:
        try:
            if action == "upload":
                return await self._upload(params["path"], params["content"])
            elif action == "download":
                return await self._download(params["path"])
            elif action == "delete":
                return await self._delete(params["path"])
            elif action == "list_folder":
                return await self._list_folder(params.get("path", ""))
            elif action == "create_folder":
                return await self._create_folder(params["path"], params["name"])
            elif action == "copy":
                return await self._copy(params["path"], params["dest_path"], params.get("new_name"))
            elif action == "move":
                return await self._move(params["path"], params["dest_path"])
            elif action == "get_item":
                return await self._get_item(params["path"])
            elif action == "search":
                return await self._search(params["query"])
            elif action == "create_sharing_link":
                return await self._create_sharing_link(
                    params["path"],
                    params.get("type", "view"),
                    params.get("scope", "anonymous")
                )
            else:
                return ConnectorResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ConnectorResult(success=False, error=str(e))

    def _path_url(self, path: str) -> str:
        """Build URL for path."""
        if not path or path == "/":
            return f"{self.base_url}/me/drive/root"
        return f"{self.base_url}/me/drive/root:/{path.lstrip('/')}"

    async def _upload(self, path: str, content: str) -> ConnectorResult:
        file_content = base64.b64decode(content)

        async with httpx.AsyncClient() as client:
            response = await client.put(
                f"{self._path_url(path)}:/content",
                headers={
                    "Authorization": f"Bearer {self.access_token}",
                    "Content-Type": "application/octet-stream",
                },
                content=file_content,
            )
            response.raise_for_status()
            data = response.json()
            return ConnectorResult(success=True, data={"id": data["id"], "name": data["name"]})

    async def _download(self, path: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            # Get download URL
            response = await client.get(
                f"{self._path_url(path)}:/content",
                headers=self._headers(),
                follow_redirects=True,
            )
            response.raise_for_status()
            content = response.content

            return ConnectorResult(
                success=True,
                data={"content": base64.b64encode(content).decode()}
            )

    async def _delete(self, path: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.delete(
                self._path_url(path),
                headers=self._headers(),
            )
            response.raise_for_status()
            return ConnectorResult(success=True, data={"deleted": path})

    async def _list_folder(self, path: str) -> ConnectorResult:
        url = f"{self._path_url(path)}/children" if path else f"{self.base_url}/me/drive/root/children"

        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=self._headers())
            response.raise_for_status()
            data = response.json()

            items = [
                {
                    "id": item["id"],
                    "name": item["name"],
                    "type": "folder" if "folder" in item else "file",
                    "size": item.get("size"),
                    "modified": item.get("lastModifiedDateTime"),
                }
                for item in data.get("value", [])
            ]
            return ConnectorResult(success=True, data={"items": items, "count": len(items)})

    async def _create_folder(self, path: str, name: str) -> ConnectorResult:
        url = f"{self._path_url(path)}/children" if path else f"{self.base_url}/me/drive/root/children"

        async with httpx.AsyncClient() as client:
            response = await client.post(
                url,
                headers=self._headers(),
                json={
                    "name": name,
                    "folder": {},
                    "@microsoft.graph.conflictBehavior": "rename",
                },
            )
            response.raise_for_status()
            data = response.json()
            return ConnectorResult(success=True, data={"id": data["id"], "name": data["name"]})

    async def _copy(self, path: str, dest_path: str, new_name: str | None) -> ConnectorResult:
        # First get the destination folder ID
        dest_info = await self._get_item(dest_path)
        if not dest_info.success:
            return dest_info

        body = {"parentReference": {"id": dest_info.data["id"]}}
        if new_name:
            body["name"] = new_name

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self._path_url(path)}:/copy",
                headers=self._headers(),
                json=body,
            )
            response.raise_for_status()
            return ConnectorResult(success=True, data={"copied": True})

    async def _move(self, path: str, dest_path: str) -> ConnectorResult:
        # Get destination folder ID
        dest_info = await self._get_item(dest_path)
        if not dest_info.success:
            return dest_info

        async with httpx.AsyncClient() as client:
            response = await client.patch(
                self._path_url(path),
                headers=self._headers(),
                json={"parentReference": {"id": dest_info.data["id"]}},
            )
            response.raise_for_status()
            data = response.json()
            return ConnectorResult(success=True, data={"id": data["id"], "name": data["name"]})

    async def _get_item(self, path: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                self._path_url(path),
                headers=self._headers(),
            )
            response.raise_for_status()
            data = response.json()
            return ConnectorResult(
                success=True,
                data={
                    "id": data["id"],
                    "name": data["name"],
                    "type": "folder" if "folder" in data else "file",
                    "size": data.get("size"),
                    "modified": data.get("lastModifiedDateTime"),
                    "web_url": data.get("webUrl"),
                }
            )

    async def _search(self, query: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/me/drive/root/search(q='{query}')",
                headers=self._headers(),
            )
            response.raise_for_status()
            data = response.json()

            results = [
                {
                    "id": item["id"],
                    "name": item["name"],
                    "type": "folder" if "folder" in item else "file",
                    "path": item.get("parentReference", {}).get("path", ""),
                }
                for item in data.get("value", [])
            ]
            return ConnectorResult(success=True, data={"results": results, "count": len(results)})

    async def _create_sharing_link(self, path: str, link_type: str, scope: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self._path_url(path)}:/createLink",
                headers=self._headers(),
                json={"type": link_type, "scope": scope},
            )
            response.raise_for_status()
            data = response.json()
            return ConnectorResult(success=True, data={"url": data["link"]["webUrl"]})
