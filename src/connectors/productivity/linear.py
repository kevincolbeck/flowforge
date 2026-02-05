"""
Linear Connector

Connect to Linear for issue tracking.
"""

from typing import Any
import httpx
from ..base import BaseConnector, ConnectorResult


class LinearConnector(BaseConnector):
    """Connector for Linear."""

    def __init__(self, credentials: dict[str, Any]):
        super().__init__(credentials)
        self.api_key = credentials.get("api_key")
        self.base_url = "https://api.linear.app/graphql"

    def _headers(self):
        return {
            "Authorization": self.api_key,
            "Content-Type": "application/json",
        }

    async def _query(self, query: str, variables: dict = None) -> dict:
        """Execute a GraphQL query."""
        async with httpx.AsyncClient() as client:
            response = await client.post(
                self.base_url,
                headers=self._headers(),
                json={"query": query, "variables": variables or {}},
            )
            response.raise_for_status()
            return response.json()

    @classmethod
    def get_actions(cls) -> dict[str, dict[str, Any]]:
        return {
            "create_issue": {
                "description": "Create a new issue",
                "parameters": {
                    "team_id": {"type": "string", "description": "Team ID", "required": True},
                    "title": {"type": "string", "description": "Issue title", "required": True},
                    "description": {"type": "string", "description": "Issue description", "required": False},
                    "priority": {"type": "integer", "description": "Priority (0-4)", "required": False},
                    "assignee_id": {"type": "string", "description": "Assignee ID", "required": False},
                },
            },
            "get_issue": {
                "description": "Get issue details",
                "parameters": {
                    "issue_id": {"type": "string", "description": "Issue ID", "required": True},
                },
            },
            "update_issue": {
                "description": "Update an issue",
                "parameters": {
                    "issue_id": {"type": "string", "description": "Issue ID", "required": True},
                    "data": {"type": "object", "description": "Fields to update", "required": True},
                },
            },
            "list_issues": {
                "description": "List issues",
                "parameters": {
                    "team_id": {"type": "string", "description": "Filter by team", "required": False},
                    "first": {"type": "integer", "description": "Number of issues", "required": False},
                },
            },
            "list_teams": {
                "description": "List all teams",
                "parameters": {},
            },
            "create_comment": {
                "description": "Add a comment to an issue",
                "parameters": {
                    "issue_id": {"type": "string", "description": "Issue ID", "required": True},
                    "body": {"type": "string", "description": "Comment body", "required": True},
                },
            },
            "list_projects": {
                "description": "List all projects",
                "parameters": {},
            },
        }

    async def execute(self, action: str, params: dict[str, Any]) -> ConnectorResult:
        try:
            if action == "create_issue":
                return await self._create_issue(params)
            elif action == "get_issue":
                return await self._get_issue(params["issue_id"])
            elif action == "update_issue":
                return await self._update_issue(params["issue_id"], params["data"])
            elif action == "list_issues":
                return await self._list_issues(params.get("team_id"), params.get("first", 50))
            elif action == "list_teams":
                return await self._list_teams()
            elif action == "create_comment":
                return await self._create_comment(params["issue_id"], params["body"])
            elif action == "list_projects":
                return await self._list_projects()
            else:
                return ConnectorResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ConnectorResult(success=False, error=str(e))

    async def _create_issue(self, params: dict) -> ConnectorResult:
        query = """
        mutation IssueCreate($input: IssueCreateInput!) {
            issueCreate(input: $input) {
                success
                issue {
                    id
                    identifier
                    title
                }
            }
        }
        """
        input_data = {
            "teamId": params["team_id"],
            "title": params["title"],
        }
        if params.get("description"):
            input_data["description"] = params["description"]
        if params.get("priority") is not None:
            input_data["priority"] = params["priority"]
        if params.get("assignee_id"):
            input_data["assigneeId"] = params["assignee_id"]

        result = await self._query(query, {"input": input_data})
        issue = result.get("data", {}).get("issueCreate", {}).get("issue", {})
        return ConnectorResult(success=True, data={
            "id": issue.get("id"),
            "identifier": issue.get("identifier"),
            "title": issue.get("title"),
        })

    async def _get_issue(self, issue_id: str) -> ConnectorResult:
        query = """
        query Issue($id: String!) {
            issue(id: $id) {
                id
                identifier
                title
                description
                priority
                state {
                    name
                }
                assignee {
                    name
                }
            }
        }
        """
        result = await self._query(query, {"id": issue_id})
        issue = result.get("data", {}).get("issue", {})
        return ConnectorResult(success=True, data=issue)

    async def _update_issue(self, issue_id: str, data: dict) -> ConnectorResult:
        query = """
        mutation IssueUpdate($id: String!, $input: IssueUpdateInput!) {
            issueUpdate(id: $id, input: $input) {
                success
                issue {
                    id
                    identifier
                }
            }
        }
        """
        result = await self._query(query, {"id": issue_id, "input": data})
        return ConnectorResult(success=True, data={"id": issue_id, "updated": True})

    async def _list_issues(self, team_id: str | None, first: int) -> ConnectorResult:
        if team_id:
            query = """
            query TeamIssues($teamId: String!, $first: Int) {
                team(id: $teamId) {
                    issues(first: $first) {
                        nodes {
                            id
                            identifier
                            title
                            state {
                                name
                            }
                        }
                    }
                }
            }
            """
            result = await self._query(query, {"teamId": team_id, "first": first})
            issues = result.get("data", {}).get("team", {}).get("issues", {}).get("nodes", [])
        else:
            query = """
            query Issues($first: Int) {
                issues(first: $first) {
                    nodes {
                        id
                        identifier
                        title
                        state {
                            name
                        }
                    }
                }
            }
            """
            result = await self._query(query, {"first": first})
            issues = result.get("data", {}).get("issues", {}).get("nodes", [])

        return ConnectorResult(success=True, data={"issues": issues})

    async def _list_teams(self) -> ConnectorResult:
        query = """
        query Teams {
            teams {
                nodes {
                    id
                    name
                    key
                }
            }
        }
        """
        result = await self._query(query)
        teams = result.get("data", {}).get("teams", {}).get("nodes", [])
        return ConnectorResult(success=True, data={"teams": teams})

    async def _create_comment(self, issue_id: str, body: str) -> ConnectorResult:
        query = """
        mutation CommentCreate($input: CommentCreateInput!) {
            commentCreate(input: $input) {
                success
                comment {
                    id
                }
            }
        }
        """
        result = await self._query(query, {"input": {"issueId": issue_id, "body": body}})
        comment = result.get("data", {}).get("commentCreate", {}).get("comment", {})
        return ConnectorResult(success=True, data={"id": comment.get("id")})

    async def _list_projects(self) -> ConnectorResult:
        query = """
        query Projects {
            projects {
                nodes {
                    id
                    name
                    state
                }
            }
        }
        """
        result = await self._query(query)
        projects = result.get("data", {}).get("projects", {}).get("nodes", [])
        return ConnectorResult(success=True, data={"projects": projects})
