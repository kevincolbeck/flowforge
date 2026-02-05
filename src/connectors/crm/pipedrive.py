"""
Pipedrive Connector

Connect to Pipedrive CRM for deals, contacts, and pipeline management.
"""

from typing import Any
import httpx
from ..base import BaseConnector, ConnectorResult


class PipedriveConnector(BaseConnector):
    """Connector for Pipedrive CRM."""

    def __init__(self, credentials: dict[str, Any]):
        super().__init__(credentials)
        self.api_token = credentials.get("api_token")
        self.base_url = "https://api.pipedrive.com/v1"

    def _params(self, extra: dict = None) -> dict:
        params = {"api_token": self.api_token}
        if extra:
            params.update(extra)
        return params

    @classmethod
    def get_actions(cls) -> dict[str, dict[str, Any]]:
        return {
            "create_person": {
                "description": "Create a new person (contact)",
                "parameters": {
                    "name": {"type": "string", "description": "Person name", "required": True},
                    "email": {"type": "string", "description": "Email address", "required": False},
                    "phone": {"type": "string", "description": "Phone number", "required": False},
                    "org_id": {"type": "integer", "description": "Organization ID", "required": False},
                },
            },
            "get_person": {
                "description": "Get a person by ID",
                "parameters": {
                    "person_id": {"type": "integer", "description": "Person ID", "required": True},
                },
            },
            "update_person": {
                "description": "Update a person",
                "parameters": {
                    "person_id": {"type": "integer", "description": "Person ID", "required": True},
                    "data": {"type": "object", "description": "Fields to update", "required": True},
                },
            },
            "search_persons": {
                "description": "Search for persons",
                "parameters": {
                    "term": {"type": "string", "description": "Search term", "required": True},
                },
            },
            "create_organization": {
                "description": "Create a new organization",
                "parameters": {
                    "name": {"type": "string", "description": "Organization name", "required": True},
                    "address": {"type": "string", "description": "Address", "required": False},
                },
            },
            "create_deal": {
                "description": "Create a new deal",
                "parameters": {
                    "title": {"type": "string", "description": "Deal title", "required": True},
                    "value": {"type": "number", "description": "Deal value", "required": False},
                    "currency": {"type": "string", "description": "Currency code", "required": False},
                    "person_id": {"type": "integer", "description": "Person ID", "required": False},
                    "org_id": {"type": "integer", "description": "Organization ID", "required": False},
                    "stage_id": {"type": "integer", "description": "Stage ID", "required": False},
                },
            },
            "get_deal": {
                "description": "Get a deal by ID",
                "parameters": {
                    "deal_id": {"type": "integer", "description": "Deal ID", "required": True},
                },
            },
            "update_deal": {
                "description": "Update a deal",
                "parameters": {
                    "deal_id": {"type": "integer", "description": "Deal ID", "required": True},
                    "data": {"type": "object", "description": "Fields to update", "required": True},
                },
            },
            "list_deals": {
                "description": "List all deals",
                "parameters": {
                    "status": {"type": "string", "description": "open, won, lost, deleted, all_not_deleted", "required": False},
                    "stage_id": {"type": "integer", "description": "Filter by stage", "required": False},
                },
            },
            "create_activity": {
                "description": "Create an activity",
                "parameters": {
                    "subject": {"type": "string", "description": "Activity subject", "required": True},
                    "type": {"type": "string", "description": "Activity type (call, meeting, task, etc.)", "required": True},
                    "due_date": {"type": "string", "description": "Due date (YYYY-MM-DD)", "required": False},
                    "deal_id": {"type": "integer", "description": "Associated deal ID", "required": False},
                    "person_id": {"type": "integer", "description": "Associated person ID", "required": False},
                },
            },
            "list_pipelines": {
                "description": "List all pipelines",
                "parameters": {},
            },
            "list_stages": {
                "description": "List stages in a pipeline",
                "parameters": {
                    "pipeline_id": {"type": "integer", "description": "Pipeline ID", "required": True},
                },
            },
        }

    async def execute(self, action: str, params: dict[str, Any]) -> ConnectorResult:
        try:
            if action == "create_person":
                return await self._create_person(params)
            elif action == "get_person":
                return await self._get_person(params["person_id"])
            elif action == "update_person":
                return await self._update_person(params["person_id"], params["data"])
            elif action == "search_persons":
                return await self._search_persons(params["term"])
            elif action == "create_organization":
                return await self._create_organization(params)
            elif action == "create_deal":
                return await self._create_deal(params)
            elif action == "get_deal":
                return await self._get_deal(params["deal_id"])
            elif action == "update_deal":
                return await self._update_deal(params["deal_id"], params["data"])
            elif action == "list_deals":
                return await self._list_deals(params.get("status"), params.get("stage_id"))
            elif action == "create_activity":
                return await self._create_activity(params)
            elif action == "list_pipelines":
                return await self._list_pipelines()
            elif action == "list_stages":
                return await self._list_stages(params["pipeline_id"])
            else:
                return ConnectorResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ConnectorResult(success=False, error=str(e))

    async def _create_person(self, params: dict) -> ConnectorResult:
        data = {"name": params["name"]}
        if params.get("email"):
            data["email"] = [{"value": params["email"], "primary": True}]
        if params.get("phone"):
            data["phone"] = [{"value": params["phone"], "primary": True}]
        if params.get("org_id"):
            data["org_id"] = params["org_id"]

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/persons",
                params=self._params(),
                json=data,
            )
            response.raise_for_status()
            result = response.json()
            return ConnectorResult(success=True, data={"id": result["data"]["id"], "name": result["data"]["name"]})

    async def _get_person(self, person_id: int) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/persons/{person_id}",
                params=self._params(),
            )
            response.raise_for_status()
            result = response.json()
            return ConnectorResult(success=True, data=result["data"])

    async def _update_person(self, person_id: int, data: dict) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.put(
                f"{self.base_url}/persons/{person_id}",
                params=self._params(),
                json=data,
            )
            response.raise_for_status()
            result = response.json()
            return ConnectorResult(success=True, data={"id": result["data"]["id"], "updated": True})

    async def _search_persons(self, term: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/persons/search",
                params=self._params({"term": term}),
            )
            response.raise_for_status()
            result = response.json()
            persons = [{"id": p["item"]["id"], "name": p["item"]["name"]} for p in result.get("data", {}).get("items", [])]
            return ConnectorResult(success=True, data={"persons": persons})

    async def _create_organization(self, params: dict) -> ConnectorResult:
        data = {"name": params["name"]}
        if params.get("address"):
            data["address"] = params["address"]

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/organizations",
                params=self._params(),
                json=data,
            )
            response.raise_for_status()
            result = response.json()
            return ConnectorResult(success=True, data={"id": result["data"]["id"], "name": result["data"]["name"]})

    async def _create_deal(self, params: dict) -> ConnectorResult:
        data = {"title": params["title"]}
        if params.get("value"):
            data["value"] = params["value"]
        if params.get("currency"):
            data["currency"] = params["currency"]
        if params.get("person_id"):
            data["person_id"] = params["person_id"]
        if params.get("org_id"):
            data["org_id"] = params["org_id"]
        if params.get("stage_id"):
            data["stage_id"] = params["stage_id"]

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/deals",
                params=self._params(),
                json=data,
            )
            response.raise_for_status()
            result = response.json()
            return ConnectorResult(success=True, data={"id": result["data"]["id"], "title": result["data"]["title"]})

    async def _get_deal(self, deal_id: int) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/deals/{deal_id}",
                params=self._params(),
            )
            response.raise_for_status()
            result = response.json()
            return ConnectorResult(success=True, data=result["data"])

    async def _update_deal(self, deal_id: int, data: dict) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.put(
                f"{self.base_url}/deals/{deal_id}",
                params=self._params(),
                json=data,
            )
            response.raise_for_status()
            result = response.json()
            return ConnectorResult(success=True, data={"id": result["data"]["id"], "updated": True})

    async def _list_deals(self, status: str | None, stage_id: int | None) -> ConnectorResult:
        extra = {}
        if status:
            extra["status"] = status
        if stage_id:
            extra["stage_id"] = stage_id

        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/deals",
                params=self._params(extra),
            )
            response.raise_for_status()
            result = response.json()
            deals = [{"id": d["id"], "title": d["title"], "value": d.get("value")} for d in result.get("data", []) or []]
            return ConnectorResult(success=True, data={"deals": deals})

    async def _create_activity(self, params: dict) -> ConnectorResult:
        data = {
            "subject": params["subject"],
            "type": params["type"],
        }
        if params.get("due_date"):
            data["due_date"] = params["due_date"]
        if params.get("deal_id"):
            data["deal_id"] = params["deal_id"]
        if params.get("person_id"):
            data["person_id"] = params["person_id"]

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/activities",
                params=self._params(),
                json=data,
            )
            response.raise_for_status()
            result = response.json()
            return ConnectorResult(success=True, data={"id": result["data"]["id"]})

    async def _list_pipelines(self) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/pipelines",
                params=self._params(),
            )
            response.raise_for_status()
            result = response.json()
            pipelines = [{"id": p["id"], "name": p["name"]} for p in result.get("data", [])]
            return ConnectorResult(success=True, data={"pipelines": pipelines})

    async def _list_stages(self, pipeline_id: int) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/stages",
                params=self._params({"pipeline_id": pipeline_id}),
            )
            response.raise_for_status()
            result = response.json()
            stages = [{"id": s["id"], "name": s["name"], "order_nr": s["order_nr"]} for s in result.get("data", [])]
            return ConnectorResult(success=True, data={"stages": stages})
