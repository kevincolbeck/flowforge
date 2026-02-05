"""
ClickUp Connector

Connect to ClickUp for task and project management.
"""

from typing import Any
import httpx
from ..base import BaseConnector, ConnectorResult


class ClickUpConnector(BaseConnector):
    """Connector for ClickUp."""

    def __init__(self, credentials: dict[str, Any]):
        super().__init__(credentials)
        self.api_token = credentials.get("api_token")
        self.base_url = "https://api.clickup.com/api/v2"

    def _headers(self):
        return {
            "Authorization": self.api_token,
            "Content-Type": "application/json",
        }

    @classmethod
    def get_actions(cls) -> dict[str, dict[str, Any]]:
        return {
            "create_task": {
                "description": "Create a new task",
                "parameters": {
                    "list_id": {"type": "string", "description": "List ID", "required": True},
                    "name": {"type": "string", "description": "Task name", "required": True},
                    "description": {"type": "string", "description": "Task description", "required": False},
                    "priority": {"type": "integer", "description": "Priority (1-4)", "required": False},
                    "due_date": {"type": "integer", "description": "Due date (Unix timestamp ms)", "required": False},
                    "assignees": {"type": "array", "description": "Assignee user IDs", "required": False},
                },
            },
            "get_task": {
                "description": "Get task details",
                "parameters": {
                    "task_id": {"type": "string", "description": "Task ID", "required": True},
                },
            },
            "update_task": {
                "description": "Update a task",
                "parameters": {
                    "task_id": {"type": "string", "description": "Task ID", "required": True},
                    "data": {"type": "object", "description": "Fields to update", "required": True},
                },
            },
            "delete_task": {
                "description": "Delete a task",
                "parameters": {
                    "task_id": {"type": "string", "description": "Task ID", "required": True},
                },
            },
            "list_tasks": {
                "description": "List tasks in a list",
                "parameters": {
                    "list_id": {"type": "string", "description": "List ID", "required": True},
                },
            },
            "add_comment": {
                "description": "Add a comment to a task",
                "parameters": {
                    "task_id": {"type": "string", "description": "Task ID", "required": True},
                    "comment_text": {"type": "string", "description": "Comment text", "required": True},
                },
            },
            "list_workspaces": {
                "description": "List all workspaces (teams)",
                "parameters": {},
            },
            "list_spaces": {
                "description": "List spaces in a workspace",
                "parameters": {
                    "team_id": {"type": "string", "description": "Team/Workspace ID", "required": True},
                },
            },
            "list_folders": {
                "description": "List folders in a space",
                "parameters": {
                    "space_id": {"type": "string", "description": "Space ID", "required": True},
                },
            },
            "list_lists": {
                "description": "List lists in a folder",
                "parameters": {
                    "folder_id": {"type": "string", "description": "Folder ID", "required": True},
                },
            },
        }

    async def execute(self, action: str, params: dict[str, Any]) -> ConnectorResult:
        try:
            if action == "create_task":
                return await self._create_task(params)
            elif action == "get_task":
                return await self._get_task(params["task_id"])
            elif action == "update_task":
                return await self._update_task(params["task_id"], params["data"])
            elif action == "delete_task":
                return await self._delete_task(params["task_id"])
            elif action == "list_tasks":
                return await self._list_tasks(params["list_id"])
            elif action == "add_comment":
                return await self._add_comment(params["task_id"], params["comment_text"])
            elif action == "list_workspaces":
                return await self._list_workspaces()
            elif action == "list_spaces":
                return await self._list_spaces(params["team_id"])
            elif action == "list_folders":
                return await self._list_folders(params["space_id"])
            elif action == "list_lists":
                return await self._list_lists(params["folder_id"])
            else:
                return ConnectorResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ConnectorResult(success=False, error=str(e))

    async def _create_task(self, params: dict) -> ConnectorResult:
        data = {"name": params["name"]}
        if params.get("description"):
            data["description"] = params["description"]
        if params.get("priority"):
            data["priority"] = params["priority"]
        if params.get("due_date"):
            data["due_date"] = params["due_date"]
        if params.get("assignees"):
            data["assignees"] = params["assignees"]

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/list/{params['list_id']}/task",
                headers=self._headers(),
                json=data,
            )
            response.raise_for_status()
            result = response.json()
            return ConnectorResult(success=True, data={"id": result["id"], "name": result["name"]})

    async def _get_task(self, task_id: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/task/{task_id}",
                headers=self._headers(),
            )
            response.raise_for_status()
            return ConnectorResult(success=True, data=response.json())

    async def _update_task(self, task_id: str, data: dict) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.put(
                f"{self.base_url}/task/{task_id}",
                headers=self._headers(),
                json=data,
            )
            response.raise_for_status()
            return ConnectorResult(success=True, data={"id": task_id, "updated": True})

    async def _delete_task(self, task_id: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.delete(
                f"{self.base_url}/task/{task_id}",
                headers=self._headers(),
            )
            response.raise_for_status()
            return ConnectorResult(success=True, data={"id": task_id, "deleted": True})

    async def _list_tasks(self, list_id: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/list/{list_id}/task",
                headers=self._headers(),
            )
            response.raise_for_status()
            result = response.json()
            tasks = [{"id": t["id"], "name": t["name"], "status": t.get("status", {}).get("status")} for t in result.get("tasks", [])]
            return ConnectorResult(success=True, data={"tasks": tasks})

    async def _add_comment(self, task_id: str, comment_text: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/task/{task_id}/comment",
                headers=self._headers(),
                json={"comment_text": comment_text},
            )
            response.raise_for_status()
            result = response.json()
            return ConnectorResult(success=True, data={"id": result.get("id")})

    async def _list_workspaces(self) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/team",
                headers=self._headers(),
            )
            response.raise_for_status()
            result = response.json()
            teams = [{"id": t["id"], "name": t["name"]} for t in result.get("teams", [])]
            return ConnectorResult(success=True, data={"workspaces": teams})

    async def _list_spaces(self, team_id: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/team/{team_id}/space",
                headers=self._headers(),
            )
            response.raise_for_status()
            result = response.json()
            spaces = [{"id": s["id"], "name": s["name"]} for s in result.get("spaces", [])]
            return ConnectorResult(success=True, data={"spaces": spaces})

    async def _list_folders(self, space_id: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/space/{space_id}/folder",
                headers=self._headers(),
            )
            response.raise_for_status()
            result = response.json()
            folders = [{"id": f["id"], "name": f["name"]} for f in result.get("folders", [])]
            return ConnectorResult(success=True, data={"folders": folders})

    async def _list_lists(self, folder_id: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/folder/{folder_id}/list",
                headers=self._headers(),
            )
            response.raise_for_status()
            result = response.json()
            lists = [{"id": l["id"], "name": l["name"]} for l in result.get("lists", [])]
            return ConnectorResult(success=True, data={"lists": lists})
