"""
Salesforce Connector

Connect to Salesforce CRM for customer and sales data operations.
"""

from typing import Any
import httpx
from ..base import BaseConnector, ConnectorResult


class SalesforceConnector(BaseConnector):
    """Connector for Salesforce CRM."""

    def __init__(self, credentials: dict[str, Any]):
        super().__init__(credentials)
        self.instance_url = credentials.get("instance_url")  # https://yourorg.salesforce.com
        self.access_token = credentials.get("access_token")
        self.api_version = credentials.get("api_version", "v58.0")

    def _headers(self):
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

    @property
    def _base_url(self):
        return f"{self.instance_url}/services/data/{self.api_version}"

    @classmethod
    def get_actions(cls) -> dict[str, dict[str, Any]]:
        return {
            "query": {
                "description": "Execute a SOQL query",
                "parameters": {
                    "soql": {"type": "string", "description": "SOQL query", "required": True},
                },
            },
            "get_record": {
                "description": "Get a record by ID",
                "parameters": {
                    "object_type": {"type": "string", "description": "Object type (Account, Contact, etc.)", "required": True},
                    "record_id": {"type": "string", "description": "Record ID", "required": True},
                    "fields": {"type": "array", "description": "Fields to retrieve", "required": False},
                },
            },
            "create_record": {
                "description": "Create a new record",
                "parameters": {
                    "object_type": {"type": "string", "description": "Object type", "required": True},
                    "data": {"type": "object", "description": "Record data", "required": True},
                },
            },
            "update_record": {
                "description": "Update an existing record",
                "parameters": {
                    "object_type": {"type": "string", "description": "Object type", "required": True},
                    "record_id": {"type": "string", "description": "Record ID", "required": True},
                    "data": {"type": "object", "description": "Fields to update", "required": True},
                },
            },
            "delete_record": {
                "description": "Delete a record",
                "parameters": {
                    "object_type": {"type": "string", "description": "Object type", "required": True},
                    "record_id": {"type": "string", "description": "Record ID", "required": True},
                },
            },
            "upsert_record": {
                "description": "Insert or update a record by external ID",
                "parameters": {
                    "object_type": {"type": "string", "description": "Object type", "required": True},
                    "external_id_field": {"type": "string", "description": "External ID field name", "required": True},
                    "external_id": {"type": "string", "description": "External ID value", "required": True},
                    "data": {"type": "object", "description": "Record data", "required": True},
                },
            },
            "describe_object": {
                "description": "Get object metadata",
                "parameters": {
                    "object_type": {"type": "string", "description": "Object type", "required": True},
                },
            },
            "list_objects": {
                "description": "List all available objects",
                "parameters": {},
            },
            "search": {
                "description": "Execute a SOSL search",
                "parameters": {
                    "sosl": {"type": "string", "description": "SOSL search string", "required": True},
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
                    "status": {"type": "string", "description": "Lead status", "required": False},
                },
            },
            "create_opportunity": {
                "description": "Create a new opportunity",
                "parameters": {
                    "name": {"type": "string", "description": "Opportunity name", "required": True},
                    "stage": {"type": "string", "description": "Stage name", "required": True},
                    "close_date": {"type": "string", "description": "Close date (YYYY-MM-DD)", "required": True},
                    "amount": {"type": "number", "description": "Amount", "required": False},
                    "account_id": {"type": "string", "description": "Account ID", "required": False},
                },
            },
        }

    async def execute(self, action: str, params: dict[str, Any]) -> ConnectorResult:
        try:
            if action == "query":
                return await self._query(params["soql"])
            elif action == "get_record":
                return await self._get_record(params["object_type"], params["record_id"], params.get("fields"))
            elif action == "create_record":
                return await self._create_record(params["object_type"], params["data"])
            elif action == "update_record":
                return await self._update_record(params["object_type"], params["record_id"], params["data"])
            elif action == "delete_record":
                return await self._delete_record(params["object_type"], params["record_id"])
            elif action == "upsert_record":
                return await self._upsert_record(
                    params["object_type"], params["external_id_field"],
                    params["external_id"], params["data"]
                )
            elif action == "describe_object":
                return await self._describe_object(params["object_type"])
            elif action == "list_objects":
                return await self._list_objects()
            elif action == "search":
                return await self._search(params["sosl"])
            elif action == "create_lead":
                return await self._create_lead(params)
            elif action == "create_opportunity":
                return await self._create_opportunity(params)
            else:
                return ConnectorResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ConnectorResult(success=False, error=str(e))

    async def _query(self, soql: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self._base_url}/query",
                headers=self._headers(),
                params={"q": soql},
            )
            response.raise_for_status()
            data = response.json()
            return ConnectorResult(
                success=True,
                data={
                    "records": data.get("records", []),
                    "total_size": data.get("totalSize", 0),
                    "done": data.get("done", True),
                }
            )

    async def _get_record(self, object_type: str, record_id: str, fields: list | None) -> ConnectorResult:
        url = f"{self._base_url}/sobjects/{object_type}/{record_id}"
        params = {}
        if fields:
            params["fields"] = ",".join(fields)

        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=self._headers(), params=params)
            response.raise_for_status()
            return ConnectorResult(success=True, data=response.json())

    async def _create_record(self, object_type: str, data: dict) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self._base_url}/sobjects/{object_type}",
                headers=self._headers(),
                json=data,
            )
            response.raise_for_status()
            result = response.json()
            return ConnectorResult(success=True, data={"id": result["id"], "success": result["success"]})

    async def _update_record(self, object_type: str, record_id: str, data: dict) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.patch(
                f"{self._base_url}/sobjects/{object_type}/{record_id}",
                headers=self._headers(),
                json=data,
            )
            response.raise_for_status()
            return ConnectorResult(success=True, data={"id": record_id, "updated": True})

    async def _delete_record(self, object_type: str, record_id: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.delete(
                f"{self._base_url}/sobjects/{object_type}/{record_id}",
                headers=self._headers(),
            )
            response.raise_for_status()
            return ConnectorResult(success=True, data={"id": record_id, "deleted": True})

    async def _upsert_record(self, object_type: str, ext_id_field: str, ext_id: str, data: dict) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.patch(
                f"{self._base_url}/sobjects/{object_type}/{ext_id_field}/{ext_id}",
                headers=self._headers(),
                json=data,
            )
            response.raise_for_status()
            result = response.json() if response.content else {}
            return ConnectorResult(success=True, data={"id": result.get("id"), "created": result.get("created", False)})

    async def _describe_object(self, object_type: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self._base_url}/sobjects/{object_type}/describe",
                headers=self._headers(),
            )
            response.raise_for_status()
            data = response.json()
            return ConnectorResult(
                success=True,
                data={
                    "name": data["name"],
                    "label": data["label"],
                    "fields": [{"name": f["name"], "type": f["type"], "label": f["label"]} for f in data["fields"]],
                }
            )

    async def _list_objects(self) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{self._base_url}/sobjects", headers=self._headers())
            response.raise_for_status()
            data = response.json()
            objects = [{"name": o["name"], "label": o["label"]} for o in data.get("sobjects", [])]
            return ConnectorResult(success=True, data={"objects": objects})

    async def _search(self, sosl: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self._base_url}/search",
                headers=self._headers(),
                params={"q": sosl},
            )
            response.raise_for_status()
            data = response.json()
            return ConnectorResult(success=True, data={"results": data.get("searchRecords", [])})

    async def _create_lead(self, params: dict) -> ConnectorResult:
        data = {
            "LastName": params["last_name"],
            "Company": params["company"],
        }
        if params.get("first_name"):
            data["FirstName"] = params["first_name"]
        if params.get("email"):
            data["Email"] = params["email"]
        if params.get("phone"):
            data["Phone"] = params["phone"]
        if params.get("status"):
            data["Status"] = params["status"]

        return await self._create_record("Lead", data)

    async def _create_opportunity(self, params: dict) -> ConnectorResult:
        data = {
            "Name": params["name"],
            "StageName": params["stage"],
            "CloseDate": params["close_date"],
        }
        if params.get("amount"):
            data["Amount"] = params["amount"]
        if params.get("account_id"):
            data["AccountId"] = params["account_id"]

        return await self._create_record("Opportunity", data)
