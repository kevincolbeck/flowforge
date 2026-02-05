"""
Asana Connector

Connect to Asana for task and project management.
"""

from typing import Any
import httpx
from ..base import BaseConnector, ConnectorResult


class AsanaConnector(BaseConnector):
    """Connector for Asana."""

    def __init__(self, credentials: dict[str, Any]):
        super().__init__(credentials)
        self.access_token = credentials.get("access_token")
        self.base_url = "https://app.asana.com/api/1.0"

    def _headers(self):
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

    @classmethod
    def get_actions(cls) -> dict[str, dict[str, Any]]:
        return {
            "create_task": {
                "description": "Create a new task",
                "parameters": {
                    "name": {"type": "string", "description": "Task name", "required": True},
                    "projects": {"type": "array", "description": "Project GIDs", "required": False},
                    "workspace": {"type": "string", "description": "Workspace GID", "required": True},
                    "notes": {"type": "string", "description": "Task description", "required": False},
                    "due_on": {"type": "string", "description": "Due date (YYYY-MM-DD)", "required": False},
                    "assignee": {"type": "string", "description": "Assignee GID", "required": False},
                },
            },
            "get_task": {
                "description": "Get task details",
                "parameters": {
                    "task_gid": {"type": "string", "description": "Task GID", "required": True},
                },
            },
            "update_task": {
                "description": "Update a task",
                "parameters": {
                    "task_gid": {"type": "string", "description": "Task GID", "required": True},
                    "data": {"type": "object", "description": "Fields to update", "required": True},
                },
            },
            "complete_task": {
                "description": "Mark a task as complete",
                "parameters": {
                    "task_gid": {"type": "string", "description": "Task GID", "required": True},
                },
            },
            "add_comment": {
                "description": "Add a comment to a task",
                "parameters": {
                    "task_gid": {"type": "string", "description": "Task GID", "required": True},
                    "text": {"type": "string", "description": "Comment text", "required": True},
                },
            },
            "list_tasks": {
                "description": "List tasks in a project",
                "parameters": {
                    "project_gid": {"type": "string", "description": "Project GID", "required": True},
                },
            },
            "create_project": {
                "description": "Create a new project",
                "parameters": {
                    "name": {"type": "string", "description": "Project name", "required": True},
                    "workspace": {"type": "string", "description": "Workspace GID", "required": True},
                    "notes": {"type": "string", "description": "Project description", "required": False},
                },
            },
            "list_projects": {
                "description": "List projects in a workspace",
                "parameters": {
                    "workspace": {"type": "string", "description": "Workspace GID", "required": True},
                },
            },
            "list_workspaces": {
                "description": "List available workspaces",
                "parameters": {},
            },
            "search_tasks": {
                "description": "Search for tasks",
                "parameters": {
                    "workspace": {"type": "string", "description": "Workspace GID", "required": True},
                    "text": {"type": "string", "description": "Search text", "required": True},
                },
            },
        }

    async def execute(self, action: str, params: dict[str, Any]) -> ConnectorResult:
        try:
            if action == "create_task":
                return await self._create_task(params)
            elif action == "get_task":
                return await self._get_task(params["task_gid"])
            elif action == "update_task":
                return await self._update_task(params["task_gid"], params["data"])
            elif action == "complete_task":
                return await self._update_task(params["task_gid"], {"completed": True})
            elif action == "add_comment":
                return await self._add_comment(params["task_gid"], params["text"])
            elif action == "list_tasks":
                return await self._list_tasks(params["project_gid"])
            elif action == "create_project":
                return await self._create_project(params)
            elif action == "list_projects":
                return await self._list_projects(params["workspace"])
            elif action == "list_workspaces":
                return await self._list_workspaces()
            elif action == "search_tasks":
                return await self._search_tasks(params["workspace"], params["text"])
            else:
                return ConnectorResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ConnectorResult(success=False, error=str(e))

    async def _create_task(self, params: dict) -> ConnectorResult:
        data = {
            "name": params["name"],
            "workspace": params["workspace"],
        }
        if params.get("projects"):
            data["projects"] = params["projects"]
        if params.get("notes"):
            data["notes"] = params["notes"]
        if params.get("due_on"):
            data["due_on"] = params["due_on"]
        if params.get("assignee"):
            data["assignee"] = params["assignee"]

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/tasks",
                headers=self._headers(),
                json={"data": data},
            )
            response.raise_for_status()
            result = response.json()
            task = result.get("data", {})
            return ConnectorResult(success=True, data={"gid": task.get("gid"), "name": task.get("name")})

    async def _get_task(self, task_gid: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/tasks/{task_gid}",
                headers=self._headers(),
            )
            response.raise_for_status()
            result = response.json()
            return ConnectorResult(success=True, data=result.get("data", {}))

    async def _update_task(self, task_gid: str, data: dict) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.put(
                f"{self.base_url}/tasks/{task_gid}",
                headers=self._headers(),
                json={"data": data},
            )
            response.raise_for_status()
            return ConnectorResult(success=True, data={"gid": task_gid, "updated": True})

    async def _add_comment(self, task_gid: str, text: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/tasks/{task_gid}/stories",
                headers=self._headers(),
                json={"data": {"text": text}},
            )
            response.raise_for_status()
            result = response.json()
            return ConnectorResult(success=True, data={"gid": result.get("data", {}).get("gid")})

    async def _list_tasks(self, project_gid: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/projects/{project_gid}/tasks",
                headers=self._headers(),
            )
            response.raise_for_status()
            result = response.json()
            tasks = [{"gid": t["gid"], "name": t["name"]} for t in result.get("data", [])]
            return ConnectorResult(success=True, data={"tasks": tasks})

    async def _create_project(self, params: dict) -> ConnectorResult:
        data = {
            "name": params["name"],
            "workspace": params["workspace"],
        }
        if params.get("notes"):
            data["notes"] = params["notes"]

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/projects",
                headers=self._headers(),
                json={"data": data},
            )
            response.raise_for_status()
            result = response.json()
            project = result.get("data", {})
            return ConnectorResult(success=True, data={"gid": project.get("gid"), "name": project.get("name")})

    async def _list_projects(self, workspace: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/workspaces/{workspace}/projects",
                headers=self._headers(),
            )
            response.raise_for_status()
            result = response.json()
            projects = [{"gid": p["gid"], "name": p["name"]} for p in result.get("data", [])]
            return ConnectorResult(success=True, data={"projects": projects})

    async def _list_workspaces(self) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/workspaces",
                headers=self._headers(),
            )
            response.raise_for_status()
            result = response.json()
            workspaces = [{"gid": w["gid"], "name": w["name"]} for w in result.get("data", [])]
            return ConnectorResult(success=True, data={"workspaces": workspaces})

    async def _search_tasks(self, workspace: str, text: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/workspaces/{workspace}/tasks/search",
                headers=self._headers(),
                params={"text": text},
            )
            response.raise_for_status()
            result = response.json()
            tasks = [{"gid": t["gid"], "name": t["name"]} for t in result.get("data", [])]
            return ConnectorResult(success=True, data={"tasks": tasks})
