"""
HubSpot Connector

Connect to HubSpot CRM for contacts, deals, and marketing operations.
"""

from typing import Any
import httpx
from ..base import BaseConnector, ConnectorResult


class HubSpotConnector(BaseConnector):
    """Connector for HubSpot CRM."""

    def __init__(self, credentials: dict[str, Any]):
        super().__init__(credentials)
        self.access_token = credentials.get("access_token")
        self.base_url = "https://api.hubapi.com"

    def _headers(self):
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

    @classmethod
    def get_actions(cls) -> dict[str, dict[str, Any]]:
        return {
            "create_contact": {
                "description": "Create a new contact",
                "parameters": {
                    "email": {"type": "string", "description": "Email address", "required": True},
                    "firstname": {"type": "string", "description": "First name", "required": False},
                    "lastname": {"type": "string", "description": "Last name", "required": False},
                    "phone": {"type": "string", "description": "Phone number", "required": False},
                    "company": {"type": "string", "description": "Company name", "required": False},
                    "properties": {"type": "object", "description": "Additional properties", "required": False},
                },
            },
            "get_contact": {
                "description": "Get a contact by ID or email",
                "parameters": {
                    "contact_id": {"type": "string", "description": "Contact ID", "required": False},
                    "email": {"type": "string", "description": "Email address", "required": False},
                },
            },
            "update_contact": {
                "description": "Update a contact",
                "parameters": {
                    "contact_id": {"type": "string", "description": "Contact ID", "required": True},
                    "properties": {"type": "object", "description": "Properties to update", "required": True},
                },
            },
            "delete_contact": {
                "description": "Delete a contact",
                "parameters": {
                    "contact_id": {"type": "string", "description": "Contact ID", "required": True},
                },
            },
            "search_contacts": {
                "description": "Search for contacts",
                "parameters": {
                    "query": {"type": "string", "description": "Search query", "required": False},
                    "filters": {"type": "array", "description": "Filter groups", "required": False},
                    "limit": {"type": "integer", "description": "Max results", "required": False},
                },
            },
            "create_company": {
                "description": "Create a new company",
                "parameters": {
                    "name": {"type": "string", "description": "Company name", "required": True},
                    "domain": {"type": "string", "description": "Company domain", "required": False},
                    "properties": {"type": "object", "description": "Additional properties", "required": False},
                },
            },
            "create_deal": {
                "description": "Create a new deal",
                "parameters": {
                    "dealname": {"type": "string", "description": "Deal name", "required": True},
                    "pipeline": {"type": "string", "description": "Pipeline ID", "required": False},
                    "dealstage": {"type": "string", "description": "Deal stage", "required": True},
                    "amount": {"type": "number", "description": "Deal amount", "required": False},
                    "properties": {"type": "object", "description": "Additional properties", "required": False},
                },
            },
            "update_deal": {
                "description": "Update a deal",
                "parameters": {
                    "deal_id": {"type": "string", "description": "Deal ID", "required": True},
                    "properties": {"type": "object", "description": "Properties to update", "required": True},
                },
            },
            "create_note": {
                "description": "Create a note on a record",
                "parameters": {
                    "body": {"type": "string", "description": "Note content", "required": True},
                    "contact_id": {"type": "string", "description": "Associated contact ID", "required": False},
                    "company_id": {"type": "string", "description": "Associated company ID", "required": False},
                    "deal_id": {"type": "string", "description": "Associated deal ID", "required": False},
                },
            },
            "create_task": {
                "description": "Create a task",
                "parameters": {
                    "subject": {"type": "string", "description": "Task subject", "required": True},
                    "body": {"type": "string", "description": "Task body", "required": False},
                    "due_date": {"type": "string", "description": "Due date (ISO format)", "required": False},
                    "contact_id": {"type": "string", "description": "Associated contact ID", "required": False},
                },
            },
            "list_pipelines": {
                "description": "List deal pipelines",
                "parameters": {},
            },
        }

    async def execute(self, action: str, params: dict[str, Any]) -> ConnectorResult:
        try:
            if action == "create_contact":
                return await self._create_contact(params)
            elif action == "get_contact":
                return await self._get_contact(params.get("contact_id"), params.get("email"))
            elif action == "update_contact":
                return await self._update_contact(params["contact_id"], params["properties"])
            elif action == "delete_contact":
                return await self._delete_contact(params["contact_id"])
            elif action == "search_contacts":
                return await self._search_contacts(params)
            elif action == "create_company":
                return await self._create_company(params)
            elif action == "create_deal":
                return await self._create_deal(params)
            elif action == "update_deal":
                return await self._update_deal(params["deal_id"], params["properties"])
            elif action == "create_note":
                return await self._create_note(params)
            elif action == "create_task":
                return await self._create_task(params)
            elif action == "list_pipelines":
                return await self._list_pipelines()
            else:
                return ConnectorResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ConnectorResult(success=False, error=str(e))

    async def _create_contact(self, params: dict) -> ConnectorResult:
        properties = params.get("properties", {})
        properties["email"] = params["email"]
        if params.get("firstname"):
            properties["firstname"] = params["firstname"]
        if params.get("lastname"):
            properties["lastname"] = params["lastname"]
        if params.get("phone"):
            properties["phone"] = params["phone"]
        if params.get("company"):
            properties["company"] = params["company"]

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/crm/v3/objects/contacts",
                headers=self._headers(),
                json={"properties": properties},
            )
            response.raise_for_status()
            data = response.json()
            return ConnectorResult(success=True, data={"id": data["id"], "properties": data["properties"]})

    async def _get_contact(self, contact_id: str | None, email: str | None) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            if contact_id:
                response = await client.get(
                    f"{self.base_url}/crm/v3/objects/contacts/{contact_id}",
                    headers=self._headers(),
                )
            elif email:
                response = await client.get(
                    f"{self.base_url}/crm/v3/objects/contacts/{email}",
                    headers=self._headers(),
                    params={"idProperty": "email"},
                )
            else:
                return ConnectorResult(success=False, error="Must provide contact_id or email")

            response.raise_for_status()
            data = response.json()
            return ConnectorResult(success=True, data={"id": data["id"], "properties": data["properties"]})

    async def _update_contact(self, contact_id: str, properties: dict) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.patch(
                f"{self.base_url}/crm/v3/objects/contacts/{contact_id}",
                headers=self._headers(),
                json={"properties": properties},
            )
            response.raise_for_status()
            data = response.json()
            return ConnectorResult(success=True, data={"id": data["id"], "updated": True})

    async def _delete_contact(self, contact_id: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.delete(
                f"{self.base_url}/crm/v3/objects/contacts/{contact_id}",
                headers=self._headers(),
            )
            response.raise_for_status()
            return ConnectorResult(success=True, data={"id": contact_id, "deleted": True})

    async def _search_contacts(self, params: dict) -> ConnectorResult:
        body = {"limit": params.get("limit", 10)}
        if params.get("query"):
            body["query"] = params["query"]
        if params.get("filters"):
            body["filterGroups"] = [{"filters": params["filters"]}]

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/crm/v3/objects/contacts/search",
                headers=self._headers(),
                json=body,
            )
            response.raise_for_status()
            data = response.json()
            contacts = [{"id": c["id"], "properties": c["properties"]} for c in data.get("results", [])]
            return ConnectorResult(success=True, data={"contacts": contacts, "total": data.get("total", 0)})

    async def _create_company(self, params: dict) -> ConnectorResult:
        properties = params.get("properties", {})
        properties["name"] = params["name"]
        if params.get("domain"):
            properties["domain"] = params["domain"]

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/crm/v3/objects/companies",
                headers=self._headers(),
                json={"properties": properties},
            )
            response.raise_for_status()
            data = response.json()
            return ConnectorResult(success=True, data={"id": data["id"], "properties": data["properties"]})

    async def _create_deal(self, params: dict) -> ConnectorResult:
        properties = params.get("properties", {})
        properties["dealname"] = params["dealname"]
        properties["dealstage"] = params["dealstage"]
        if params.get("pipeline"):
            properties["pipeline"] = params["pipeline"]
        if params.get("amount"):
            properties["amount"] = str(params["amount"])

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/crm/v3/objects/deals",
                headers=self._headers(),
                json={"properties": properties},
            )
            response.raise_for_status()
            data = response.json()
            return ConnectorResult(success=True, data={"id": data["id"], "properties": data["properties"]})

    async def _update_deal(self, deal_id: str, properties: dict) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.patch(
                f"{self.base_url}/crm/v3/objects/deals/{deal_id}",
                headers=self._headers(),
                json={"properties": properties},
            )
            response.raise_for_status()
            data = response.json()
            return ConnectorResult(success=True, data={"id": data["id"], "updated": True})

    async def _create_note(self, params: dict) -> ConnectorResult:
        properties = {"hs_note_body": params["body"]}

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/crm/v3/objects/notes",
                headers=self._headers(),
                json={"properties": properties},
            )
            response.raise_for_status()
            data = response.json()
            note_id = data["id"]

            # Associate with records
            associations = []
            if params.get("contact_id"):
                associations.append(("contacts", params["contact_id"]))
            if params.get("company_id"):
                associations.append(("companies", params["company_id"]))
            if params.get("deal_id"):
                associations.append(("deals", params["deal_id"]))

            for obj_type, obj_id in associations:
                await client.put(
                    f"{self.base_url}/crm/v3/objects/notes/{note_id}/associations/{obj_type}/{obj_id}/note_to_{obj_type[:-1]}",
                    headers=self._headers(),
                )

            return ConnectorResult(success=True, data={"id": note_id})

    async def _create_task(self, params: dict) -> ConnectorResult:
        properties = {
            "hs_task_subject": params["subject"],
            "hs_task_status": "NOT_STARTED",
        }
        if params.get("body"):
            properties["hs_task_body"] = params["body"]
        if params.get("due_date"):
            properties["hs_timestamp"] = params["due_date"]

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/crm/v3/objects/tasks",
                headers=self._headers(),
                json={"properties": properties},
            )
            response.raise_for_status()
            data = response.json()
            return ConnectorResult(success=True, data={"id": data["id"]})

    async def _list_pipelines(self) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/crm/v3/pipelines/deals",
                headers=self._headers(),
            )
            response.raise_for_status()
            data = response.json()
            pipelines = [
                {
                    "id": p["id"],
                    "label": p["label"],
                    "stages": [{"id": s["id"], "label": s["label"]} for s in p.get("stages", [])],
                }
                for p in data.get("results", [])
            ]
            return ConnectorResult(success=True, data={"pipelines": pipelines})
