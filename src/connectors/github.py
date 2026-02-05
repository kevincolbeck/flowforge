"""
GitHub Connector

Manage repositories, issues, pull requests, and more.
"""

from typing import Any
from .base import BaseConnector, ConnectorResult


class GitHubConnector(BaseConnector):
    """Connector for GitHub."""

    service_name = "github"
    display_name = "GitHub"
    base_url = "https://api.github.com"

    def get_actions(self) -> list[dict[str, str]]:
        return [
            {"action": "create_issue", "description": "Create a new issue"},
            {"action": "create_comment", "description": "Comment on an issue or PR"},
            {"action": "create_pr", "description": "Create a pull request"},
            {"action": "get_repo", "description": "Get repository information"},
            {"action": "list_issues", "description": "List repository issues"},
            {"action": "list_prs", "description": "List pull requests"},
            {"action": "add_label", "description": "Add label to issue/PR"},
            {"action": "get_file", "description": "Get file contents from repo"},
            {"action": "create_release", "description": "Create a new release"},
        ]

    def validate_credentials(self) -> bool:
        return "access_token" in self.credentials or "token" in self.credentials

    def _get_auth_header(self) -> dict[str, str]:
        token = self.credentials.get("access_token") or self.credentials.get("token")
        return {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github.v3+json",
        }

    async def execute(self, action: str, inputs: dict[str, Any]) -> ConnectorResult:
        """Execute a GitHub action."""
        actions = {
            "create_issue": self._create_issue,
            "create_comment": self._create_comment,
            "create_pr": self._create_pr,
            "get_repo": self._get_repo,
            "list_issues": self._list_issues,
            "list_prs": self._list_prs,
            "add_label": self._add_label,
            "get_file": self._get_file,
            "create_release": self._create_release,
        }

        if action not in actions:
            return ConnectorResult(success=False, error=f"Unknown action: {action}")

        return await actions[action](inputs)

    def _parse_repo(self, inputs: dict[str, Any]) -> tuple[str, str]:
        """Parse owner and repo from inputs."""
        if "repo" in inputs and "/" in inputs["repo"]:
            parts = inputs["repo"].split("/")
            return parts[0], parts[1]
        return inputs.get("owner", ""), inputs.get("repo", "")

    async def _create_issue(self, inputs: dict[str, Any]) -> ConnectorResult:
        """Create a new issue."""
        owner, repo = self._parse_repo(inputs)
        title = inputs.get("title", "")
        body = inputs.get("body", "")
        labels = inputs.get("labels", [])
        assignees = inputs.get("assignees", [])

        if not owner or not repo or not title:
            return ConnectorResult(success=False, error="Owner, repo, and title are required")

        payload = {"title": title, "body": body}
        if labels:
            payload["labels"] = labels
        if assignees:
            payload["assignees"] = assignees

        return await self._request(
            "POST",
            f"{self.base_url}/repos/{owner}/{repo}/issues",
            json=payload,
        )

    async def _create_comment(self, inputs: dict[str, Any]) -> ConnectorResult:
        """Comment on an issue or PR."""
        owner, repo = self._parse_repo(inputs)
        issue_number = inputs.get("issue_number") or inputs.get("pr_number")
        body = inputs.get("body", inputs.get("comment", ""))

        if not owner or not repo or not issue_number or not body:
            return ConnectorResult(
                success=False, error="Owner, repo, issue_number, and body are required"
            )

        return await self._request(
            "POST",
            f"{self.base_url}/repos/{owner}/{repo}/issues/{issue_number}/comments",
            json={"body": body},
        )

    async def _create_pr(self, inputs: dict[str, Any]) -> ConnectorResult:
        """Create a pull request."""
        owner, repo = self._parse_repo(inputs)
        title = inputs.get("title", "")
        body = inputs.get("body", "")
        head = inputs.get("head", "")  # Branch with changes
        base = inputs.get("base", "main")  # Target branch

        if not owner or not repo or not title or not head:
            return ConnectorResult(
                success=False, error="Owner, repo, title, and head branch are required"
            )

        return await self._request(
            "POST",
            f"{self.base_url}/repos/{owner}/{repo}/pulls",
            json={"title": title, "body": body, "head": head, "base": base},
        )

    async def _get_repo(self, inputs: dict[str, Any]) -> ConnectorResult:
        """Get repository information."""
        owner, repo = self._parse_repo(inputs)

        if not owner or not repo:
            return ConnectorResult(success=False, error="Owner and repo are required")

        return await self._request("GET", f"{self.base_url}/repos/{owner}/{repo}")

    async def _list_issues(self, inputs: dict[str, Any]) -> ConnectorResult:
        """List repository issues."""
        owner, repo = self._parse_repo(inputs)
        state = inputs.get("state", "open")
        labels = inputs.get("labels", "")
        per_page = inputs.get("per_page", 30)

        if not owner or not repo:
            return ConnectorResult(success=False, error="Owner and repo are required")

        params = {"state": state, "per_page": per_page}
        if labels:
            params["labels"] = labels

        return await self._request(
            "GET",
            f"{self.base_url}/repos/{owner}/{repo}/issues",
            params=params,
        )

    async def _list_prs(self, inputs: dict[str, Any]) -> ConnectorResult:
        """List pull requests."""
        owner, repo = self._parse_repo(inputs)
        state = inputs.get("state", "open")
        per_page = inputs.get("per_page", 30)

        if not owner or not repo:
            return ConnectorResult(success=False, error="Owner and repo are required")

        return await self._request(
            "GET",
            f"{self.base_url}/repos/{owner}/{repo}/pulls",
            params={"state": state, "per_page": per_page},
        )

    async def _add_label(self, inputs: dict[str, Any]) -> ConnectorResult:
        """Add label to issue/PR."""
        owner, repo = self._parse_repo(inputs)
        issue_number = inputs.get("issue_number") or inputs.get("pr_number")
        labels = inputs.get("labels", [])

        if isinstance(labels, str):
            labels = [labels]

        if not owner or not repo or not issue_number or not labels:
            return ConnectorResult(
                success=False, error="Owner, repo, issue_number, and labels are required"
            )

        return await self._request(
            "POST",
            f"{self.base_url}/repos/{owner}/{repo}/issues/{issue_number}/labels",
            json={"labels": labels},
        )

    async def _get_file(self, inputs: dict[str, Any]) -> ConnectorResult:
        """Get file contents from repo."""
        owner, repo = self._parse_repo(inputs)
        path = inputs.get("path", "")
        ref = inputs.get("ref", inputs.get("branch", "main"))

        if not owner or not repo or not path:
            return ConnectorResult(success=False, error="Owner, repo, and path are required")

        return await self._request(
            "GET",
            f"{self.base_url}/repos/{owner}/{repo}/contents/{path}",
            params={"ref": ref},
        )

    async def _create_release(self, inputs: dict[str, Any]) -> ConnectorResult:
        """Create a new release."""
        owner, repo = self._parse_repo(inputs)
        tag_name = inputs.get("tag_name", inputs.get("tag", ""))
        name = inputs.get("name", tag_name)
        body = inputs.get("body", "")
        draft = inputs.get("draft", False)
        prerelease = inputs.get("prerelease", False)

        if not owner or not repo or not tag_name:
            return ConnectorResult(success=False, error="Owner, repo, and tag_name are required")

        return await self._request(
            "POST",
            f"{self.base_url}/repos/{owner}/{repo}/releases",
            json={
                "tag_name": tag_name,
                "name": name,
                "body": body,
                "draft": draft,
                "prerelease": prerelease,
            },
        )

    async def test_connection(self) -> ConnectorResult:
        """Test the GitHub connection."""
        return await self._request("GET", f"{self.base_url}/user")
