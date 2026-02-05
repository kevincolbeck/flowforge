"""
Trello Connector

Connect to Trello for board and card management.
"""

from typing import Any
import httpx
from ..base import BaseConnector, ConnectorResult


class TrelloConnector(BaseConnector):
    """Connector for Trello."""

    def __init__(self, credentials: dict[str, Any]):
        super().__init__(credentials)
        self.api_key = credentials.get("api_key")
        self.token = credentials.get("token")
        self.base_url = "https://api.trello.com/1"

    def _params(self, extra: dict = None) -> dict:
        params = {"key": self.api_key, "token": self.token}
        if extra:
            params.update(extra)
        return params

    @classmethod
    def get_actions(cls) -> dict[str, dict[str, Any]]:
        return {
            "create_card": {
                "description": "Create a new card",
                "parameters": {
                    "list_id": {"type": "string", "description": "List ID", "required": True},
                    "name": {"type": "string", "description": "Card name", "required": True},
                    "desc": {"type": "string", "description": "Card description", "required": False},
                    "due": {"type": "string", "description": "Due date (ISO format)", "required": False},
                    "labels": {"type": "array", "description": "Label IDs", "required": False},
                },
            },
            "get_card": {
                "description": "Get card details",
                "parameters": {
                    "card_id": {"type": "string", "description": "Card ID", "required": True},
                },
            },
            "update_card": {
                "description": "Update a card",
                "parameters": {
                    "card_id": {"type": "string", "description": "Card ID", "required": True},
                    "data": {"type": "object", "description": "Fields to update", "required": True},
                },
            },
            "move_card": {
                "description": "Move a card to another list",
                "parameters": {
                    "card_id": {"type": "string", "description": "Card ID", "required": True},
                    "list_id": {"type": "string", "description": "Target list ID", "required": True},
                },
            },
            "add_comment": {
                "description": "Add a comment to a card",
                "parameters": {
                    "card_id": {"type": "string", "description": "Card ID", "required": True},
                    "text": {"type": "string", "description": "Comment text", "required": True},
                },
            },
            "list_boards": {
                "description": "List all boards for the user",
                "parameters": {},
            },
            "get_board": {
                "description": "Get board details",
                "parameters": {
                    "board_id": {"type": "string", "description": "Board ID", "required": True},
                },
            },
            "list_lists": {
                "description": "List all lists on a board",
                "parameters": {
                    "board_id": {"type": "string", "description": "Board ID", "required": True},
                },
            },
            "list_cards": {
                "description": "List all cards on a list",
                "parameters": {
                    "list_id": {"type": "string", "description": "List ID", "required": True},
                },
            },
            "create_list": {
                "description": "Create a new list on a board",
                "parameters": {
                    "board_id": {"type": "string", "description": "Board ID", "required": True},
                    "name": {"type": "string", "description": "List name", "required": True},
                },
            },
            "add_label": {
                "description": "Add a label to a card",
                "parameters": {
                    "card_id": {"type": "string", "description": "Card ID", "required": True},
                    "label_id": {"type": "string", "description": "Label ID", "required": True},
                },
            },
        }

    async def execute(self, action: str, params: dict[str, Any]) -> ConnectorResult:
        try:
            if action == "create_card":
                return await self._create_card(params)
            elif action == "get_card":
                return await self._get_card(params["card_id"])
            elif action == "update_card":
                return await self._update_card(params["card_id"], params["data"])
            elif action == "move_card":
                return await self._update_card(params["card_id"], {"idList": params["list_id"]})
            elif action == "add_comment":
                return await self._add_comment(params["card_id"], params["text"])
            elif action == "list_boards":
                return await self._list_boards()
            elif action == "get_board":
                return await self._get_board(params["board_id"])
            elif action == "list_lists":
                return await self._list_lists(params["board_id"])
            elif action == "list_cards":
                return await self._list_cards(params["list_id"])
            elif action == "create_list":
                return await self._create_list(params["board_id"], params["name"])
            elif action == "add_label":
                return await self._add_label(params["card_id"], params["label_id"])
            else:
                return ConnectorResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ConnectorResult(success=False, error=str(e))

    async def _create_card(self, params: dict) -> ConnectorResult:
        data = {
            "idList": params["list_id"],
            "name": params["name"],
        }
        if params.get("desc"):
            data["desc"] = params["desc"]
        if params.get("due"):
            data["due"] = params["due"]
        if params.get("labels"):
            data["idLabels"] = ",".join(params["labels"])

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/cards",
                params=self._params(data),
            )
            response.raise_for_status()
            result = response.json()
            return ConnectorResult(success=True, data={"id": result["id"], "name": result["name"]})

    async def _get_card(self, card_id: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/cards/{card_id}",
                params=self._params(),
            )
            response.raise_for_status()
            return ConnectorResult(success=True, data=response.json())

    async def _update_card(self, card_id: str, data: dict) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.put(
                f"{self.base_url}/cards/{card_id}",
                params=self._params(data),
            )
            response.raise_for_status()
            return ConnectorResult(success=True, data={"id": card_id, "updated": True})

    async def _add_comment(self, card_id: str, text: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/cards/{card_id}/actions/comments",
                params=self._params({"text": text}),
            )
            response.raise_for_status()
            result = response.json()
            return ConnectorResult(success=True, data={"id": result["id"]})

    async def _list_boards(self) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/members/me/boards",
                params=self._params(),
            )
            response.raise_for_status()
            result = response.json()
            boards = [{"id": b["id"], "name": b["name"]} for b in result]
            return ConnectorResult(success=True, data={"boards": boards})

    async def _get_board(self, board_id: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/boards/{board_id}",
                params=self._params(),
            )
            response.raise_for_status()
            return ConnectorResult(success=True, data=response.json())

    async def _list_lists(self, board_id: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/boards/{board_id}/lists",
                params=self._params(),
            )
            response.raise_for_status()
            result = response.json()
            lists = [{"id": l["id"], "name": l["name"]} for l in result]
            return ConnectorResult(success=True, data={"lists": lists})

    async def _list_cards(self, list_id: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/lists/{list_id}/cards",
                params=self._params(),
            )
            response.raise_for_status()
            result = response.json()
            cards = [{"id": c["id"], "name": c["name"]} for c in result]
            return ConnectorResult(success=True, data={"cards": cards})

    async def _create_list(self, board_id: str, name: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/lists",
                params=self._params({"idBoard": board_id, "name": name}),
            )
            response.raise_for_status()
            result = response.json()
            return ConnectorResult(success=True, data={"id": result["id"], "name": result["name"]})

    async def _add_label(self, card_id: str, label_id: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/cards/{card_id}/idLabels",
                params=self._params({"value": label_id}),
            )
            response.raise_for_status()
            return ConnectorResult(success=True, data={"card_id": card_id, "label_added": True})
