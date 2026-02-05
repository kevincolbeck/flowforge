"""
Monday.com Connector

Connect to Monday.com for work management.
"""

from typing import Any
import httpx
from ..base import BaseConnector, ConnectorResult


class MondayConnector(BaseConnector):
    """Connector for Monday.com."""

    def __init__(self, credentials: dict[str, Any]):
        super().__init__(credentials)
        self.api_token = credentials.get("api_token")
        self.base_url = "https://api.monday.com/v2"

    def _headers(self):
        return {
            "Authorization": self.api_token,
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
            "create_item": {
                "description": "Create a new item",
                "parameters": {
                    "board_id": {"type": "string", "description": "Board ID", "required": True},
                    "item_name": {"type": "string", "description": "Item name", "required": True},
                    "group_id": {"type": "string", "description": "Group ID", "required": False},
                    "column_values": {"type": "object", "description": "Column values as JSON", "required": False},
                },
            },
            "get_item": {
                "description": "Get item details",
                "parameters": {
                    "item_id": {"type": "string", "description": "Item ID", "required": True},
                },
            },
            "update_item": {
                "description": "Update item column values",
                "parameters": {
                    "board_id": {"type": "string", "description": "Board ID", "required": True},
                    "item_id": {"type": "string", "description": "Item ID", "required": True},
                    "column_values": {"type": "object", "description": "Column values", "required": True},
                },
            },
            "delete_item": {
                "description": "Delete an item",
                "parameters": {
                    "item_id": {"type": "string", "description": "Item ID", "required": True},
                },
            },
            "list_boards": {
                "description": "List all boards",
                "parameters": {},
            },
            "get_board": {
                "description": "Get board details",
                "parameters": {
                    "board_id": {"type": "string", "description": "Board ID", "required": True},
                },
            },
            "list_items": {
                "description": "List items in a board",
                "parameters": {
                    "board_id": {"type": "string", "description": "Board ID", "required": True},
                    "limit": {"type": "integer", "description": "Max items", "required": False},
                },
            },
            "create_update": {
                "description": "Add an update (comment) to an item",
                "parameters": {
                    "item_id": {"type": "string", "description": "Item ID", "required": True},
                    "body": {"type": "string", "description": "Update body", "required": True},
                },
            },
            "move_item_to_group": {
                "description": "Move an item to a different group",
                "parameters": {
                    "item_id": {"type": "string", "description": "Item ID", "required": True},
                    "group_id": {"type": "string", "description": "Target group ID", "required": True},
                },
            },
            "list_groups": {
                "description": "List groups in a board",
                "parameters": {
                    "board_id": {"type": "string", "description": "Board ID", "required": True},
                },
            },
        }

    async def execute(self, action: str, params: dict[str, Any]) -> ConnectorResult:
        try:
            if action == "create_item":
                return await self._create_item(params)
            elif action == "get_item":
                return await self._get_item(params["item_id"])
            elif action == "update_item":
                return await self._update_item(params["board_id"], params["item_id"], params["column_values"])
            elif action == "delete_item":
                return await self._delete_item(params["item_id"])
            elif action == "list_boards":
                return await self._list_boards()
            elif action == "get_board":
                return await self._get_board(params["board_id"])
            elif action == "list_items":
                return await self._list_items(params["board_id"], params.get("limit", 100))
            elif action == "create_update":
                return await self._create_update(params["item_id"], params["body"])
            elif action == "move_item_to_group":
                return await self._move_item_to_group(params["item_id"], params["group_id"])
            elif action == "list_groups":
                return await self._list_groups(params["board_id"])
            else:
                return ConnectorResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ConnectorResult(success=False, error=str(e))

    async def _create_item(self, params: dict) -> ConnectorResult:
        import json
        query = """
        mutation ($board_id: ID!, $item_name: String!, $group_id: String, $column_values: JSON) {
            create_item(board_id: $board_id, item_name: $item_name, group_id: $group_id, column_values: $column_values) {
                id
                name
            }
        }
        """
        variables = {
            "board_id": params["board_id"],
            "item_name": params["item_name"],
        }
        if params.get("group_id"):
            variables["group_id"] = params["group_id"]
        if params.get("column_values"):
            variables["column_values"] = json.dumps(params["column_values"])

        result = await self._query(query, variables)
        item = result.get("data", {}).get("create_item", {})
        return ConnectorResult(success=True, data={"id": item.get("id"), "name": item.get("name")})

    async def _get_item(self, item_id: str) -> ConnectorResult:
        query = """
        query ($ids: [ID!]) {
            items(ids: $ids) {
                id
                name
                state
                column_values {
                    id
                    text
                    value
                }
            }
        }
        """
        result = await self._query(query, {"ids": [item_id]})
        items = result.get("data", {}).get("items", [])
        if items:
            return ConnectorResult(success=True, data=items[0])
        return ConnectorResult(success=False, error="Item not found")

    async def _update_item(self, board_id: str, item_id: str, column_values: dict) -> ConnectorResult:
        import json
        query = """
        mutation ($board_id: ID!, $item_id: ID!, $column_values: JSON!) {
            change_multiple_column_values(board_id: $board_id, item_id: $item_id, column_values: $column_values) {
                id
            }
        }
        """
        variables = {
            "board_id": board_id,
            "item_id": item_id,
            "column_values": json.dumps(column_values),
        }
        result = await self._query(query, variables)
        return ConnectorResult(success=True, data={"id": item_id, "updated": True})

    async def _delete_item(self, item_id: str) -> ConnectorResult:
        query = """
        mutation ($item_id: ID!) {
            delete_item(item_id: $item_id) {
                id
            }
        }
        """
        await self._query(query, {"item_id": item_id})
        return ConnectorResult(success=True, data={"id": item_id, "deleted": True})

    async def _list_boards(self) -> ConnectorResult:
        query = """
        query {
            boards(limit: 100) {
                id
                name
                state
            }
        }
        """
        result = await self._query(query)
        boards = result.get("data", {}).get("boards", [])
        return ConnectorResult(success=True, data={"boards": boards})

    async def _get_board(self, board_id: str) -> ConnectorResult:
        query = """
        query ($ids: [ID!]) {
            boards(ids: $ids) {
                id
                name
                columns {
                    id
                    title
                    type
                }
                groups {
                    id
                    title
                }
            }
        }
        """
        result = await self._query(query, {"ids": [board_id]})
        boards = result.get("data", {}).get("boards", [])
        if boards:
            return ConnectorResult(success=True, data=boards[0])
        return ConnectorResult(success=False, error="Board not found")

    async def _list_items(self, board_id: str, limit: int) -> ConnectorResult:
        query = """
        query ($ids: [ID!], $limit: Int) {
            boards(ids: $ids) {
                items_page(limit: $limit) {
                    items {
                        id
                        name
                        state
                    }
                }
            }
        }
        """
        result = await self._query(query, {"ids": [board_id], "limit": limit})
        boards = result.get("data", {}).get("boards", [])
        items = boards[0].get("items_page", {}).get("items", []) if boards else []
        return ConnectorResult(success=True, data={"items": items})

    async def _create_update(self, item_id: str, body: str) -> ConnectorResult:
        query = """
        mutation ($item_id: ID!, $body: String!) {
            create_update(item_id: $item_id, body: $body) {
                id
            }
        }
        """
        result = await self._query(query, {"item_id": item_id, "body": body})
        update = result.get("data", {}).get("create_update", {})
        return ConnectorResult(success=True, data={"id": update.get("id")})

    async def _move_item_to_group(self, item_id: str, group_id: str) -> ConnectorResult:
        query = """
        mutation ($item_id: ID!, $group_id: String!) {
            move_item_to_group(item_id: $item_id, group_id: $group_id) {
                id
            }
        }
        """
        result = await self._query(query, {"item_id": item_id, "group_id": group_id})
        return ConnectorResult(success=True, data={"id": item_id, "moved": True})

    async def _list_groups(self, board_id: str) -> ConnectorResult:
        query = """
        query ($ids: [ID!]) {
            boards(ids: $ids) {
                groups {
                    id
                    title
                }
            }
        }
        """
        result = await self._query(query, {"ids": [board_id]})
        boards = result.get("data", {}).get("boards", [])
        groups = boards[0].get("groups", []) if boards else []
        return ConnectorResult(success=True, data={"groups": groups})
