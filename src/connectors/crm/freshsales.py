"""
Freshsales Connector

Connect to Freshsales CRM for leads, contacts, and deals.
"""

from typing import Any
import httpx
from ..base import BaseConnector, ConnectorResult


class FreshsalesConnector(BaseConnector):
    """Connector for Freshsales CRM."""

    def __init__(self, credentials: dict[str, Any]):
        super().__init__(credentials)
        self.domain = credentials.get("domain")  # yourcompany.freshsales.io
        self.api_key = credentials.get("api_key")
        self.base_url = f"https://{self.domain}/api"

    def _headers(self):
        return {
            "Authorization": f"Token token={self.api_key}",
            "Content-Type": "application/json",
        }

    @classmethod
    def get_actions(cls) -> dict[str, dict[str, Any]]:
        return {
            "create_lead": {
                "description": "Create a new lead",
                "parameters": {
                    "first_name": {"type": "string", "description": "First name", "required": False},
                    "last_name": {"type": "string", "description": "Last name", "required": True},
                    "email": {"type": "string", "description": "Email", "required": False},
                    "mobile_number": {"type": "string", "description": "Mobile number", "required": False},
                    "company_name": {"type": "string", "description": "Company name", "required": False},
                },
            },
            "get_lead": {
                "description": "Get a lead by ID",
                "parameters": {
                    "lead_id": {"type": "integer", "description": "Lead ID", "required": True},
                },
            },
            "update_lead": {
                "description": "Update a lead",
                "parameters": {
                    "lead_id": {"type": "integer", "description": "Lead ID", "required": True},
                    "data": {"type": "object", "description": "Fields to update", "required": True},
                },
            },
            "list_leads": {
                "description": "List all leads",
                "parameters": {
                    "page": {"type": "integer", "description": "Page number", "required": False},
                },
            },
            "create_contact": {
                "description": "Create a new contact",
                "parameters": {
                    "first_name": {"type": "string", "description": "First name", "required": False},
                    "last_name": {"type": "string", "description": "Last name", "required": True},
                    "email": {"type": "string", "description": "Email", "required": False},
                    "mobile_number": {"type": "string", "description": "Mobile number", "required": False},
                },
            },
            "get_contact": {
                "description": "Get a contact by ID",
                "parameters": {
                    "contact_id": {"type": "integer", "description": "Contact ID", "required": True},
                },
            },
            "create_deal": {
                "description": "Create a new deal",
                "parameters": {
                    "name": {"type": "string", "description": "Deal name", "required": True},
                    "amount": {"type": "number", "description": "Deal amount", "required": False},
                    "contacts_id": {"type": "integer", "description": "Contact ID", "required": False},
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
            "create_account": {
                "description": "Create a new account (company)",
                "parameters": {
                    "name": {"type": "string", "description": "Company name", "required": True},
                    "website": {"type": "string", "description": "Website", "required": False},
                    "phone": {"type": "string", "description": "Phone", "required": False},
                },
            },
            "create_task": {
                "description": "Create a task",
                "parameters": {
                    "title": {"type": "string", "description": "Task title", "required": True},
                    "due_date": {"type": "string", "description": "Due date", "required": True},
                    "targetable_type": {"type": "string", "description": "Lead, Contact, or Deal", "required": False},
                    "targetable_id": {"type": "integer", "description": "Associated record ID", "required": False},
                },
            },
        }

    async def execute(self, action: str, params: dict[str, Any]) -> ConnectorResult:
        try:
            if action == "create_lead":
                return await self._create_lead(params)
            elif action == "get_lead":
                return await self._get_lead(params["lead_id"])
            elif action == "update_lead":
                return await self._update_lead(params["lead_id"], params["data"])
            elif action == "list_leads":
                return await self._list_leads(params.get("page", 1))
            elif action == "create_contact":
                return await self._create_contact(params)
            elif action == "get_contact":
                return await self._get_contact(params["contact_id"])
            elif action == "create_deal":
                return await self._create_deal(params)
            elif action == "get_deal":
                return await self._get_deal(params["deal_id"])
            elif action == "update_deal":
                return await self._update_deal(params["deal_id"], params["data"])
            elif action == "create_account":
                return await self._create_account(params)
            elif action == "create_task":
                return await self._create_task(params)
            else:
                return ConnectorResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ConnectorResult(success=False, error=str(e))

    async def _create_lead(self, params: dict) -> ConnectorResult:
        data = {"last_name": params["last_name"]}
        for field in ["first_name", "email", "mobile_number", "company_name"]:
            if params.get(field):
                data[field] = params[field]

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/leads",
                headers=self._headers(),
                json={"lead": data},
            )
            response.raise_for_status()
            result = response.json()
            return ConnectorResult(success=True, data={"id": result["lead"]["id"]})

    async def _get_lead(self, lead_id: int) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/leads/{lead_id}",
                headers=self._headers(),
            )
            response.raise_for_status()
            result = response.json()
            return ConnectorResult(success=True, data=result["lead"])

    async def _update_lead(self, lead_id: int, data: dict) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.put(
                f"{self.base_url}/leads/{lead_id}",
                headers=self._headers(),
                json={"lead": data},
            )
            response.raise_for_status()
            return ConnectorResult(success=True, data={"id": lead_id, "updated": True})

    async def _list_leads(self, page: int) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/leads",
                headers=self._headers(),
                params={"page": page},
            )
            response.raise_for_status()
            result = response.json()
            leads = [{"id": l["id"], "name": f"{l.get('first_name', '')} {l['last_name']}"} for l in result.get("leads", [])]
            return ConnectorResult(success=True, data={"leads": leads})

    async def _create_contact(self, params: dict) -> ConnectorResult:
        data = {"last_name": params["last_name"]}
        for field in ["first_name", "email", "mobile_number"]:
            if params.get(field):
                data[field] = params[field]

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/contacts",
                headers=self._headers(),
                json={"contact": data},
            )
            response.raise_for_status()
            result = response.json()
            return ConnectorResult(success=True, data={"id": result["contact"]["id"]})

    async def _get_contact(self, contact_id: int) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/contacts/{contact_id}",
                headers=self._headers(),
            )
            response.raise_for_status()
            result = response.json()
            return ConnectorResult(success=True, data=result["contact"])

    async def _create_deal(self, params: dict) -> ConnectorResult:
        data = {"name": params["name"]}
        if params.get("amount"):
            data["amount"] = params["amount"]
        if params.get("contacts_id"):
            data["contacts_id"] = params["contacts_id"]

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/deals",
                headers=self._headers(),
                json={"deal": data},
            )
            response.raise_for_status()
            result = response.json()
            return ConnectorResult(success=True, data={"id": result["deal"]["id"]})

    async def _get_deal(self, deal_id: int) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/deals/{deal_id}",
                headers=self._headers(),
            )
            response.raise_for_status()
            result = response.json()
            return ConnectorResult(success=True, data=result["deal"])

    async def _update_deal(self, deal_id: int, data: dict) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.put(
                f"{self.base_url}/deals/{deal_id}",
                headers=self._headers(),
                json={"deal": data},
            )
            response.raise_for_status()
            return ConnectorResult(success=True, data={"id": deal_id, "updated": True})

    async def _create_account(self, params: dict) -> ConnectorResult:
        data = {"name": params["name"]}
        if params.get("website"):
            data["website"] = params["website"]
        if params.get("phone"):
            data["phone"] = params["phone"]

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/sales_accounts",
                headers=self._headers(),
                json={"sales_account": data},
            )
            response.raise_for_status()
            result = response.json()
            return ConnectorResult(success=True, data={"id": result["sales_account"]["id"]})

    async def _create_task(self, params: dict) -> ConnectorResult:
        data = {
            "title": params["title"],
            "due_date": params["due_date"],
        }
        if params.get("targetable_type"):
            data["targetable_type"] = params["targetable_type"]
        if params.get("targetable_id"):
            data["targetable_id"] = params["targetable_id"]

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/tasks",
                headers=self._headers(),
                json={"task": data},
            )
            response.raise_for_status()
            result = response.json()
            return ConnectorResult(success=True, data={"id": result["task"]["id"]})
