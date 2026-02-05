"""
Jira Connector

Connect to Jira for issue tracking and project management.
"""

from typing import Any
import httpx
import base64
from ..base import BaseConnector, ConnectorResult


class JiraConnector(BaseConnector):
    """Connector for Jira."""

    def __init__(self, credentials: dict[str, Any]):
        super().__init__(credentials)
        self.domain = credentials.get("domain")  # yourcompany.atlassian.net
        self.email = credentials.get("email")
        self.api_token = credentials.get("api_token")
        self.base_url = f"https://{self.domain}/rest/api/3"

    def _headers(self):
        auth = base64.b64encode(f"{self.email}:{self.api_token}".encode()).decode()
        return {
            "Authorization": f"Basic {auth}",
            "Content-Type": "application/json",
        }

    @classmethod
    def get_actions(cls) -> dict[str, dict[str, Any]]:
        return {
            "create_issue": {
                "description": "Create a new issue",
                "parameters": {
                    "project_key": {"type": "string", "description": "Project key", "required": True},
                    "summary": {"type": "string", "description": "Issue summary", "required": True},
                    "issue_type": {"type": "string", "description": "Issue type (Bug, Task, Story, etc.)", "required": True},
                    "description": {"type": "string", "description": "Issue description", "required": False},
                    "priority": {"type": "string", "description": "Priority name", "required": False},
                    "assignee": {"type": "string", "description": "Assignee account ID", "required": False},
                    "labels": {"type": "array", "description": "Labels", "required": False},
                },
            },
            "get_issue": {
                "description": "Get issue details",
                "parameters": {
                    "issue_key": {"type": "string", "description": "Issue key (e.g., PROJ-123)", "required": True},
                },
            },
            "update_issue": {
                "description": "Update an issue",
                "parameters": {
                    "issue_key": {"type": "string", "description": "Issue key", "required": True},
                    "fields": {"type": "object", "description": "Fields to update", "required": True},
                },
            },
            "transition_issue": {
                "description": "Transition an issue to a new status",
                "parameters": {
                    "issue_key": {"type": "string", "description": "Issue key", "required": True},
                    "transition_id": {"type": "string", "description": "Transition ID", "required": True},
                },
            },
            "add_comment": {
                "description": "Add a comment to an issue",
                "parameters": {
                    "issue_key": {"type": "string", "description": "Issue key", "required": True},
                    "body": {"type": "string", "description": "Comment body", "required": True},
                },
            },
            "search_issues": {
                "description": "Search issues with JQL",
                "parameters": {
                    "jql": {"type": "string", "description": "JQL query", "required": True},
                    "max_results": {"type": "integer", "description": "Max results", "required": False},
                },
            },
            "assign_issue": {
                "description": "Assign an issue to a user",
                "parameters": {
                    "issue_key": {"type": "string", "description": "Issue key", "required": True},
                    "account_id": {"type": "string", "description": "Assignee account ID", "required": True},
                },
            },
            "get_transitions": {
                "description": "Get available transitions for an issue",
                "parameters": {
                    "issue_key": {"type": "string", "description": "Issue key", "required": True},
                },
            },
            "list_projects": {
                "description": "List all projects",
                "parameters": {},
            },
            "get_project": {
                "description": "Get project details",
                "parameters": {
                    "project_key": {"type": "string", "description": "Project key", "required": True},
                },
            },
        }

    async def execute(self, action: str, params: dict[str, Any]) -> ConnectorResult:
        try:
            if action == "create_issue":
                return await self._create_issue(params)
            elif action == "get_issue":
                return await self._get_issue(params["issue_key"])
            elif action == "update_issue":
                return await self._update_issue(params["issue_key"], params["fields"])
            elif action == "transition_issue":
                return await self._transition_issue(params["issue_key"], params["transition_id"])
            elif action == "add_comment":
                return await self._add_comment(params["issue_key"], params["body"])
            elif action == "search_issues":
                return await self._search_issues(params["jql"], params.get("max_results", 50))
            elif action == "assign_issue":
                return await self._assign_issue(params["issue_key"], params["account_id"])
            elif action == "get_transitions":
                return await self._get_transitions(params["issue_key"])
            elif action == "list_projects":
                return await self._list_projects()
            elif action == "get_project":
                return await self._get_project(params["project_key"])
            else:
                return ConnectorResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ConnectorResult(success=False, error=str(e))

    async def _create_issue(self, params: dict) -> ConnectorResult:
        fields = {
            "project": {"key": params["project_key"]},
            "summary": params["summary"],
            "issuetype": {"name": params["issue_type"]},
        }
        if params.get("description"):
            fields["description"] = {
                "type": "doc",
                "version": 1,
                "content": [{"type": "paragraph", "content": [{"type": "text", "text": params["description"]}]}],
            }
        if params.get("priority"):
            fields["priority"] = {"name": params["priority"]}
        if params.get("assignee"):
            fields["assignee"] = {"accountId": params["assignee"]}
        if params.get("labels"):
            fields["labels"] = params["labels"]

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/issue",
                headers=self._headers(),
                json={"fields": fields},
            )
            response.raise_for_status()
            result = response.json()
            return ConnectorResult(success=True, data={"id": result["id"], "key": result["key"]})

    async def _get_issue(self, issue_key: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/issue/{issue_key}",
                headers=self._headers(),
            )
            response.raise_for_status()
            result = response.json()
            return ConnectorResult(success=True, data={
                "id": result["id"],
                "key": result["key"],
                "summary": result["fields"]["summary"],
                "status": result["fields"]["status"]["name"],
                "assignee": result["fields"].get("assignee"),
            })

    async def _update_issue(self, issue_key: str, fields: dict) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.put(
                f"{self.base_url}/issue/{issue_key}",
                headers=self._headers(),
                json={"fields": fields},
            )
            response.raise_for_status()
            return ConnectorResult(success=True, data={"key": issue_key, "updated": True})

    async def _transition_issue(self, issue_key: str, transition_id: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/issue/{issue_key}/transitions",
                headers=self._headers(),
                json={"transition": {"id": transition_id}},
            )
            response.raise_for_status()
            return ConnectorResult(success=True, data={"key": issue_key, "transitioned": True})

    async def _add_comment(self, issue_key: str, body: str) -> ConnectorResult:
        comment_body = {
            "body": {
                "type": "doc",
                "version": 1,
                "content": [{"type": "paragraph", "content": [{"type": "text", "text": body}]}],
            }
        }

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/issue/{issue_key}/comment",
                headers=self._headers(),
                json=comment_body,
            )
            response.raise_for_status()
            result = response.json()
            return ConnectorResult(success=True, data={"id": result["id"]})

    async def _search_issues(self, jql: str, max_results: int) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/search",
                headers=self._headers(),
                params={"jql": jql, "maxResults": max_results},
            )
            response.raise_for_status()
            result = response.json()
            issues = [
                {"id": i["id"], "key": i["key"], "summary": i["fields"]["summary"]}
                for i in result.get("issues", [])
            ]
            return ConnectorResult(success=True, data={"issues": issues, "total": result.get("total", 0)})

    async def _assign_issue(self, issue_key: str, account_id: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.put(
                f"{self.base_url}/issue/{issue_key}/assignee",
                headers=self._headers(),
                json={"accountId": account_id},
            )
            response.raise_for_status()
            return ConnectorResult(success=True, data={"key": issue_key, "assigned": True})

    async def _get_transitions(self, issue_key: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/issue/{issue_key}/transitions",
                headers=self._headers(),
            )
            response.raise_for_status()
            result = response.json()
            transitions = [{"id": t["id"], "name": t["name"]} for t in result.get("transitions", [])]
            return ConnectorResult(success=True, data={"transitions": transitions})

    async def _list_projects(self) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/project",
                headers=self._headers(),
            )
            response.raise_for_status()
            result = response.json()
            projects = [{"id": p["id"], "key": p["key"], "name": p["name"]} for p in result]
            return ConnectorResult(success=True, data={"projects": projects})

    async def _get_project(self, project_key: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/project/{project_key}",
                headers=self._headers(),
            )
            response.raise_for_status()
            result = response.json()
            return ConnectorResult(success=True, data={
                "id": result["id"],
                "key": result["key"],
                "name": result["name"],
            })
