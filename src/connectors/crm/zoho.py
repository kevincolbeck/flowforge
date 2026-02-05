"""
Zoho CRM Connector

Connect to Zoho CRM for customer and sales data operations.
"""

from typing import Any
import httpx
from ..base import BaseConnector, ConnectorResult


class ZohoCRMConnector(BaseConnector):
    """Connector for Zoho CRM."""

    def __init__(self, credentials: dict[str, Any]):
        super().__init__(credentials)
        self.access_token = credentials.get("access_token")
        self.base_url = credentials.get("base_url", "https://www.zohoapis.com/crm/v3")

    def _headers(self):
        return {
            "Authorization": f"Zoho-oauthtoken {self.access_token}",
            "Content-Type": "application/json",
        }

    @classmethod
    def get_actions(cls) -> dict[str, dict[str, Any]]:
        return {
            "get_records": {
                "description": "Get records from a module",
                "parameters": {
                    "module": {"type": "string", "description": "Module name (Leads, Contacts, etc.)", "required": True},
                    "fields": {"type": "array", "description": "Fields to retrieve", "required": False},
                    "page": {"type": "integer", "description": "Page number", "required": False},
                    "per_page": {"type": "integer", "description": "Records per page", "required": False},
                },
            },
            "get_record": {
                "description": "Get a specific record",
                "parameters": {
                    "module": {"type": "string", "description": "Module name", "required": True},
                    "record_id": {"type": "string", "description": "Record ID", "required": True},
                },
            },
            "create_record": {
                "description": "Create a new record",
                "parameters": {
                    "module": {"type": "string", "description": "Module name", "required": True},
                    "data": {"type": "object", "description": "Record data", "required": True},
                },
            },
            "update_record": {
                "description": "Update an existing record",
                "parameters": {
                    "module": {"type": "string", "description": "Module name", "required": True},
                    "record_id": {"type": "string", "description": "Record ID", "required": True},
                    "data": {"type": "object", "description": "Fields to update", "required": True},
                },
            },
            "delete_record": {
                "description": "Delete a record",
                "parameters": {
                    "module": {"type": "string", "description": "Module name", "required": True},
                    "record_id": {"type": "string", "description": "Record ID", "required": True},
                },
            },
            "search_records": {
                "description": "Search for records",
                "parameters": {
                    "module": {"type": "string", "description": "Module name", "required": True},
                    "criteria": {"type": "string", "description": "Search criteria", "required": True},
                },
            },
            "upsert_records": {
                "description": "Insert or update records",
                "parameters": {
                    "module": {"type": "string", "description": "Module name", "required": True},
                    "records": {"type": "array", "description": "Records to upsert", "required": True},
                    "duplicate_check_fields": {"type": "array", "description": "Fields for duplicate check", "required": False},
                },
            },
            "create_lead": {
                "description": "Create a new lead",
                "parameters": {
                    "first_name": {"type": "string", "description": "First name", "required": False},
                    "last_name": {"type": "string", "description": "Last name", "required": True},
                    "company": {"type": "string", "description": "Company", "required": True},
                    "email": {"type": "string", "description": "Email", "required": False},
                    "phone": {"type": "string", "description": "Phone", "required": False},
                },
            },
            "convert_lead": {
                "description": "Convert a lead to contact/account/deal",
                "parameters": {
                    "lead_id": {"type": "string", "description": "Lead ID", "required": True},
                    "deal_name": {"type": "string", "description": "Deal name (optional)", "required": False},
                },
            },
            "list_modules": {
                "description": "List available modules",
                "parameters": {},
            },
        }

    async def execute(self, action: str, params: dict[str, Any]) -> ConnectorResult:
        try:
            if action == "get_records":
                return await self._get_records(params)
            elif action == "get_record":
                return await self._get_record(params["module"], params["record_id"])
            elif action == "create_record":
                return await self._create_record(params["module"], params["data"])
            elif action == "update_record":
                return await self._update_record(params["module"], params["record_id"], params["data"])
            elif action == "delete_record":
                return await self._delete_record(params["module"], params["record_id"])
            elif action == "search_records":
                return await self._search_records(params["module"], params["criteria"])
            elif action == "upsert_records":
                return await self._upsert_records(params["module"], params["records"], params.get("duplicate_check_fields"))
            elif action == "create_lead":
                return await self._create_lead(params)
            elif action == "convert_lead":
                return await self._convert_lead(params["lead_id"], params.get("deal_name"))
            elif action == "list_modules":
                return await self._list_modules()
            else:
                return ConnectorResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ConnectorResult(success=False, error=str(e))

    async def _get_records(self, params: dict) -> ConnectorResult:
        query_params = {}
        if params.get("fields"):
            query_params["fields"] = ",".join(params["fields"])
        if params.get("page"):
            query_params["page"] = params["page"]
        if params.get("per_page"):
            query_params["per_page"] = params["per_page"]

        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/{params['module']}",
                headers=self._headers(),
                params=query_params,
            )
            response.raise_for_status()
            data = response.json()
            return ConnectorResult(success=True, data={"records": data.get("data", [])})

    async def _get_record(self, module: str, record_id: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/{module}/{record_id}",
                headers=self._headers(),
            )
            response.raise_for_status()
            data = response.json()
            return ConnectorResult(success=True, data=data.get("data", [{}])[0])

    async def _create_record(self, module: str, data: dict) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/{module}",
                headers=self._headers(),
                json={"data": [data]},
            )
            response.raise_for_status()
            result = response.json()
            record = result.get("data", [{}])[0]
            return ConnectorResult(
                success=True,
                data={"id": record.get("details", {}).get("id"), "status": record.get("status")}
            )

    async def _update_record(self, module: str, record_id: str, data: dict) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.put(
                f"{self.base_url}/{module}/{record_id}",
                headers=self._headers(),
                json={"data": [data]},
            )
            response.raise_for_status()
            return ConnectorResult(success=True, data={"id": record_id, "updated": True})

    async def _delete_record(self, module: str, record_id: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.delete(
                f"{self.base_url}/{module}",
                headers=self._headers(),
                params={"ids": record_id},
            )
            response.raise_for_status()
            return ConnectorResult(success=True, data={"id": record_id, "deleted": True})

    async def _search_records(self, module: str, criteria: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/{module}/search",
                headers=self._headers(),
                params={"criteria": criteria},
            )
            response.raise_for_status()
            data = response.json()
            return ConnectorResult(success=True, data={"records": data.get("data", [])})

    async def _upsert_records(self, module: str, records: list, duplicate_check_fields: list | None) -> ConnectorResult:
        body = {"data": records}
        if duplicate_check_fields:
            body["duplicate_check_fields"] = duplicate_check_fields

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/{module}/upsert",
                headers=self._headers(),
                json=body,
            )
            response.raise_for_status()
            data = response.json()
            return ConnectorResult(success=True, data={"results": data.get("data", [])})

    async def _create_lead(self, params: dict) -> ConnectorResult:
        data = {
            "Last_Name": params["last_name"],
            "Company": params["company"],
        }
        if params.get("first_name"):
            data["First_Name"] = params["first_name"]
        if params.get("email"):
            data["Email"] = params["email"]
        if params.get("phone"):
            data["Phone"] = params["phone"]

        return await self._create_record("Leads", data)

    async def _convert_lead(self, lead_id: str, deal_name: str | None) -> ConnectorResult:
        body = {"data": [{}]}
        if deal_name:
            body["data"][0]["Deals"] = {"Deal_Name": deal_name}

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/Leads/{lead_id}/actions/convert",
                headers=self._headers(),
                json=body,
            )
            response.raise_for_status()
            data = response.json()
            return ConnectorResult(success=True, data=data.get("data", [{}])[0])

    async def _list_modules(self) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/settings/modules",
                headers=self._headers(),
            )
            response.raise_for_status()
            data = response.json()
            modules = [{"api_name": m["api_name"], "singular_label": m["singular_label"]} for m in data.get("modules", [])]
            return ConnectorResult(success=True, data={"modules": modules})
