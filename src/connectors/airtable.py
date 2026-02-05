"""
Airtable Connector

Manage bases, tables, and records in Airtable.
"""

from typing import Any
from .base import BaseConnector, ConnectorResult


class AirtableConnector(BaseConnector):
    """Connector for Airtable."""

    service_name = "airtable"
    display_name = "Airtable"
    base_url = "https://api.airtable.com/v0"

    def get_actions(self) -> list[dict[str, str]]:
        return [
            {"action": "create_record", "description": "Create a new record"},
            {"action": "update_record", "description": "Update an existing record"},
            {"action": "delete_record", "description": "Delete a record"},
            {"action": "get_record", "description": "Get a single record"},
            {"action": "list_records", "description": "List records from a table"},
            {"action": "search_records", "description": "Search records with filter"},
        ]

    def validate_credentials(self) -> bool:
        return "api_key" in self.credentials or "access_token" in self.credentials

    def _get_auth_header(self) -> dict[str, str]:
        token = self.credentials.get("api_key") or self.credentials.get("access_token")
        return {"Authorization": f"Bearer {token}"}

    async def execute(self, action: str, inputs: dict[str, Any]) -> ConnectorResult:
        """Execute an Airtable action."""
        actions = {
            "create_record": self._create_record,
            "update_record": self._update_record,
            "delete_record": self._delete_record,
            "get_record": self._get_record,
            "list_records": self._list_records,
            "search_records": self._search_records,
        }

        if action not in actions:
            return ConnectorResult(success=False, error=f"Unknown action: {action}")

        return await actions[action](inputs)

    def _get_base_table_url(self, inputs: dict[str, Any]) -> tuple[str, bool]:
        """Build URL for base/table operations."""
        base_id = inputs.get("base_id", "")
        table_name = inputs.get("table_name", inputs.get("table", ""))

        if not base_id or not table_name:
            return "", False

        # URL encode table name
        import urllib.parse
        encoded_table = urllib.parse.quote(table_name)

        return f"{self.base_url}/{base_id}/{encoded_table}", True

    async def _create_record(self, inputs: dict[str, Any]) -> ConnectorResult:
        """Create a new record."""
        url, valid = self._get_base_table_url(inputs)
        if not valid:
            return ConnectorResult(success=False, error="Base ID and table name are required")

        fields = inputs.get("fields", {})
        if not fields:
            return ConnectorResult(success=False, error="Fields are required")

        return await self._request(
            "POST",
            url,
            json={"records": [{"fields": fields}]},
        )

    async def _update_record(self, inputs: dict[str, Any]) -> ConnectorResult:
        """Update an existing record."""
        url, valid = self._get_base_table_url(inputs)
        if not valid:
            return ConnectorResult(success=False, error="Base ID and table name are required")

        record_id = inputs.get("record_id", "")
        fields = inputs.get("fields", {})

        if not record_id:
            return ConnectorResult(success=False, error="Record ID is required")

        return await self._request(
            "PATCH",
            f"{url}/{record_id}",
            json={"fields": fields},
        )

    async def _delete_record(self, inputs: dict[str, Any]) -> ConnectorResult:
        """Delete a record."""
        url, valid = self._get_base_table_url(inputs)
        if not valid:
            return ConnectorResult(success=False, error="Base ID and table name are required")

        record_id = inputs.get("record_id", "")
        if not record_id:
            return ConnectorResult(success=False, error="Record ID is required")

        return await self._request("DELETE", f"{url}/{record_id}")

    async def _get_record(self, inputs: dict[str, Any]) -> ConnectorResult:
        """Get a single record."""
        url, valid = self._get_base_table_url(inputs)
        if not valid:
            return ConnectorResult(success=False, error="Base ID and table name are required")

        record_id = inputs.get("record_id", "")
        if not record_id:
            return ConnectorResult(success=False, error="Record ID is required")

        return await self._request("GET", f"{url}/{record_id}")

    async def _list_records(self, inputs: dict[str, Any]) -> ConnectorResult:
        """List records from a table."""
        url, valid = self._get_base_table_url(inputs)
        if not valid:
            return ConnectorResult(success=False, error="Base ID and table name are required")

        params = {}

        if inputs.get("max_records"):
            params["maxRecords"] = inputs["max_records"]

        if inputs.get("view"):
            params["view"] = inputs["view"]

        if inputs.get("fields"):
            # Airtable expects fields[] parameters
            fields = inputs["fields"]
            if isinstance(fields, list):
                for i, field in enumerate(fields):
                    params[f"fields[{i}]"] = field

        if inputs.get("sort"):
            # Format: [{"field": "Name", "direction": "asc"}]
            sort = inputs["sort"]
            if isinstance(sort, list):
                for i, s in enumerate(sort):
                    params[f"sort[{i}][field]"] = s.get("field", "")
                    params[f"sort[{i}][direction]"] = s.get("direction", "asc")

        return await self._request("GET", url, params=params if params else None)

    async def _search_records(self, inputs: dict[str, Any]) -> ConnectorResult:
        """Search records with filter."""
        url, valid = self._get_base_table_url(inputs)
        if not valid:
            return ConnectorResult(success=False, error="Base ID and table name are required")

        params = {}

        # Formula filter (Airtable's filter syntax)
        formula = inputs.get("formula", inputs.get("filter", ""))
        if formula:
            params["filterByFormula"] = formula

        if inputs.get("max_records"):
            params["maxRecords"] = inputs["max_records"]

        if inputs.get("view"):
            params["view"] = inputs["view"]

        return await self._request("GET", url, params=params if params else None)

    async def test_connection(self) -> ConnectorResult:
        """Test the Airtable connection."""
        # List bases to verify connection
        return await self._request("GET", "https://api.airtable.com/v0/meta/bases")
