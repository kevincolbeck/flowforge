"""
Notion Connector

Create pages, manage databases, and interact with Notion.
"""

from typing import Any
from .base import BaseConnector, ConnectorResult


class NotionConnector(BaseConnector):
    """Connector for Notion."""

    service_name = "notion"
    display_name = "Notion"
    base_url = "https://api.notion.com/v1"

    def get_actions(self) -> list[dict[str, str]]:
        return [
            {"action": "create_page", "description": "Create a new page"},
            {"action": "create_database_item", "description": "Add item to a database"},
            {"action": "update_page", "description": "Update a page's properties"},
            {"action": "get_page", "description": "Get page details"},
            {"action": "query_database", "description": "Query a database"},
            {"action": "search", "description": "Search across workspace"},
            {"action": "append_block", "description": "Append content to a page"},
        ]

    def validate_credentials(self) -> bool:
        return "api_key" in self.credentials or "integration_token" in self.credentials

    def _get_auth_header(self) -> dict[str, str]:
        token = self.credentials.get("api_key") or self.credentials.get("integration_token")
        return {
            "Authorization": f"Bearer {token}",
            "Notion-Version": "2022-06-28",
        }

    async def execute(self, action: str, inputs: dict[str, Any]) -> ConnectorResult:
        """Execute a Notion action."""
        actions = {
            "create_page": self._create_page,
            "create_database_item": self._create_database_item,
            "update_page": self._update_page,
            "get_page": self._get_page,
            "query_database": self._query_database,
            "search": self._search,
            "append_block": self._append_block,
        }

        if action not in actions:
            return ConnectorResult(success=False, error=f"Unknown action: {action}")

        return await actions[action](inputs)

    async def _create_page(self, inputs: dict[str, Any]) -> ConnectorResult:
        """Create a new page."""
        parent_id = inputs.get("parent_id", "")
        parent_type = inputs.get("parent_type", "page_id")
        title = inputs.get("title", "")
        content = inputs.get("content", "")

        if not parent_id or not title:
            return ConnectorResult(success=False, error="Parent ID and title are required")

        # Build parent object
        parent = {parent_type: parent_id}

        # Build properties with title
        properties = {
            "title": {
                "title": [{"text": {"content": title}}]
            }
        }

        payload = {"parent": parent, "properties": properties}

        # Add content blocks if provided
        if content:
            payload["children"] = [
                {
                    "object": "block",
                    "type": "paragraph",
                    "paragraph": {
                        "rich_text": [{"type": "text", "text": {"content": content}}]
                    },
                }
            ]

        return await self._request("POST", f"{self.base_url}/pages", json=payload)

    async def _create_database_item(self, inputs: dict[str, Any]) -> ConnectorResult:
        """Add item to a database."""
        database_id = inputs.get("database_id", "")
        properties = inputs.get("properties", {})

        if not database_id:
            return ConnectorResult(success=False, error="Database ID is required")

        # Convert simple properties to Notion format
        notion_props = {}
        for key, value in properties.items():
            if isinstance(value, str):
                # Assume title or rich_text based on common patterns
                if key.lower() in ["name", "title"]:
                    notion_props[key] = {"title": [{"text": {"content": value}}]}
                else:
                    notion_props[key] = {"rich_text": [{"text": {"content": value}}]}
            elif isinstance(value, bool):
                notion_props[key] = {"checkbox": value}
            elif isinstance(value, (int, float)):
                notion_props[key] = {"number": value}
            elif isinstance(value, dict):
                # Assume already in Notion format
                notion_props[key] = value
            elif isinstance(value, list) and all(isinstance(v, str) for v in value):
                # Multi-select
                notion_props[key] = {"multi_select": [{"name": v} for v in value]}

        return await self._request(
            "POST",
            f"{self.base_url}/pages",
            json={
                "parent": {"database_id": database_id},
                "properties": notion_props,
            },
        )

    async def _update_page(self, inputs: dict[str, Any]) -> ConnectorResult:
        """Update a page's properties."""
        page_id = inputs.get("page_id", "")
        properties = inputs.get("properties", {})
        archived = inputs.get("archived")

        if not page_id:
            return ConnectorResult(success=False, error="Page ID is required")

        payload = {}
        if properties:
            payload["properties"] = properties
        if archived is not None:
            payload["archived"] = archived

        return await self._request(
            "PATCH", f"{self.base_url}/pages/{page_id}", json=payload
        )

    async def _get_page(self, inputs: dict[str, Any]) -> ConnectorResult:
        """Get page details."""
        page_id = inputs.get("page_id", "")

        if not page_id:
            return ConnectorResult(success=False, error="Page ID is required")

        return await self._request("GET", f"{self.base_url}/pages/{page_id}")

    async def _query_database(self, inputs: dict[str, Any]) -> ConnectorResult:
        """Query a database."""
        database_id = inputs.get("database_id", "")
        filter_obj = inputs.get("filter")
        sorts = inputs.get("sorts", [])
        page_size = inputs.get("page_size", 100)

        if not database_id:
            return ConnectorResult(success=False, error="Database ID is required")

        payload = {"page_size": page_size}
        if filter_obj:
            payload["filter"] = filter_obj
        if sorts:
            payload["sorts"] = sorts

        return await self._request(
            "POST", f"{self.base_url}/databases/{database_id}/query", json=payload
        )

    async def _search(self, inputs: dict[str, Any]) -> ConnectorResult:
        """Search across workspace."""
        query = inputs.get("query", "")
        filter_type = inputs.get("filter_type")  # "page" or "database"
        page_size = inputs.get("page_size", 100)

        payload = {"page_size": page_size}
        if query:
            payload["query"] = query
        if filter_type:
            payload["filter"] = {"property": "object", "value": filter_type}

        return await self._request("POST", f"{self.base_url}/search", json=payload)

    async def _append_block(self, inputs: dict[str, Any]) -> ConnectorResult:
        """Append content to a page."""
        page_id = inputs.get("page_id", inputs.get("block_id", ""))
        content = inputs.get("content", "")
        block_type = inputs.get("block_type", "paragraph")

        if not page_id or not content:
            return ConnectorResult(success=False, error="Page ID and content are required")

        # Build block based on type
        block = {"object": "block", "type": block_type}

        if block_type == "paragraph":
            block["paragraph"] = {
                "rich_text": [{"type": "text", "text": {"content": content}}]
            }
        elif block_type == "heading_1":
            block["heading_1"] = {
                "rich_text": [{"type": "text", "text": {"content": content}}]
            }
        elif block_type == "heading_2":
            block["heading_2"] = {
                "rich_text": [{"type": "text", "text": {"content": content}}]
            }
        elif block_type == "bulleted_list_item":
            block["bulleted_list_item"] = {
                "rich_text": [{"type": "text", "text": {"content": content}}]
            }
        elif block_type == "to_do":
            block["to_do"] = {
                "rich_text": [{"type": "text", "text": {"content": content}}],
                "checked": inputs.get("checked", False),
            }
        elif block_type == "code":
            block["code"] = {
                "rich_text": [{"type": "text", "text": {"content": content}}],
                "language": inputs.get("language", "plain text"),
            }

        return await self._request(
            "PATCH",
            f"{self.base_url}/blocks/{page_id}/children",
            json={"children": [block]},
        )

    async def test_connection(self) -> ConnectorResult:
        """Test the Notion connection."""
        return await self._request("GET", f"{self.base_url}/users/me")
