"""
Google Sheets Connector

Read and write data to Google Sheets.
"""

from typing import Any
from .base import BaseConnector, ConnectorResult


class GoogleSheetsConnector(BaseConnector):
    """Connector for Google Sheets."""

    service_name = "google_sheets"
    display_name = "Google Sheets"
    base_url = "https://sheets.googleapis.com/v4/spreadsheets"

    def get_actions(self) -> list[dict[str, str]]:
        return [
            {"action": "read_range", "description": "Read data from a range"},
            {"action": "write_range", "description": "Write data to a range"},
            {"action": "append_row", "description": "Append a row to a sheet"},
            {"action": "get_spreadsheet", "description": "Get spreadsheet metadata"},
            {"action": "create_spreadsheet", "description": "Create a new spreadsheet"},
            {"action": "add_sheet", "description": "Add a new sheet/tab"},
            {"action": "clear_range", "description": "Clear data from a range"},
        ]

    def validate_credentials(self) -> bool:
        # Supports OAuth access_token or API key
        return (
            "access_token" in self.credentials
            or "api_key" in self.credentials
            or "service_account" in self.credentials
        )

    def _get_auth_header(self) -> dict[str, str]:
        if self.credentials.get("access_token"):
            return {"Authorization": f"Bearer {self.credentials['access_token']}"}
        return {}

    def _get_api_key_param(self) -> dict[str, str]:
        if self.credentials.get("api_key"):
            return {"key": self.credentials["api_key"]}
        return {}

    async def execute(self, action: str, inputs: dict[str, Any]) -> ConnectorResult:
        """Execute a Google Sheets action."""
        actions = {
            "read_range": self._read_range,
            "write_range": self._write_range,
            "append_row": self._append_row,
            "get_spreadsheet": self._get_spreadsheet,
            "create_spreadsheet": self._create_spreadsheet,
            "add_sheet": self._add_sheet,
            "clear_range": self._clear_range,
        }

        if action not in actions:
            return ConnectorResult(success=False, error=f"Unknown action: {action}")

        return await actions[action](inputs)

    async def _read_range(self, inputs: dict[str, Any]) -> ConnectorResult:
        """Read data from a range."""
        spreadsheet_id = inputs.get("spreadsheet_id", "")
        range_name = inputs.get("range", "")

        if not spreadsheet_id or not range_name:
            return ConnectorResult(
                success=False, error="Spreadsheet ID and range are required"
            )

        params = self._get_api_key_param()
        params["valueRenderOption"] = inputs.get("value_render_option", "FORMATTED_VALUE")

        return await self._request(
            "GET",
            f"{self.base_url}/{spreadsheet_id}/values/{range_name}",
            params=params,
        )

    async def _write_range(self, inputs: dict[str, Any]) -> ConnectorResult:
        """Write data to a range."""
        spreadsheet_id = inputs.get("spreadsheet_id", "")
        range_name = inputs.get("range", "")
        values = inputs.get("values", [])

        if not spreadsheet_id or not range_name:
            return ConnectorResult(
                success=False, error="Spreadsheet ID and range are required"
            )

        if not values:
            return ConnectorResult(success=False, error="Values are required")

        # Ensure values is a 2D array
        if values and not isinstance(values[0], list):
            values = [values]

        params = self._get_api_key_param()
        params["valueInputOption"] = inputs.get("value_input_option", "USER_ENTERED")

        return await self._request(
            "PUT",
            f"{self.base_url}/{spreadsheet_id}/values/{range_name}",
            params=params,
            json={"values": values},
        )

    async def _append_row(self, inputs: dict[str, Any]) -> ConnectorResult:
        """Append a row to a sheet."""
        spreadsheet_id = inputs.get("spreadsheet_id", "")
        sheet_name = inputs.get("sheet_name", "Sheet1")
        values = inputs.get("values", inputs.get("row", []))

        if not spreadsheet_id:
            return ConnectorResult(success=False, error="Spreadsheet ID is required")

        if not values:
            return ConnectorResult(success=False, error="Values are required")

        # Ensure values is a 2D array
        if not isinstance(values[0], list):
            values = [values]

        range_name = f"{sheet_name}!A:Z"

        params = self._get_api_key_param()
        params["valueInputOption"] = inputs.get("value_input_option", "USER_ENTERED")
        params["insertDataOption"] = "INSERT_ROWS"

        return await self._request(
            "POST",
            f"{self.base_url}/{spreadsheet_id}/values/{range_name}:append",
            params=params,
            json={"values": values},
        )

    async def _get_spreadsheet(self, inputs: dict[str, Any]) -> ConnectorResult:
        """Get spreadsheet metadata."""
        spreadsheet_id = inputs.get("spreadsheet_id", "")

        if not spreadsheet_id:
            return ConnectorResult(success=False, error="Spreadsheet ID is required")

        params = self._get_api_key_param()

        return await self._request(
            "GET",
            f"{self.base_url}/{spreadsheet_id}",
            params=params,
        )

    async def _create_spreadsheet(self, inputs: dict[str, Any]) -> ConnectorResult:
        """Create a new spreadsheet."""
        title = inputs.get("title", "New Spreadsheet")
        sheets = inputs.get("sheets", ["Sheet1"])

        sheet_properties = [
            {"properties": {"title": name}} for name in sheets
        ]

        return await self._request(
            "POST",
            self.base_url,
            params=self._get_api_key_param(),
            json={
                "properties": {"title": title},
                "sheets": sheet_properties,
            },
        )

    async def _add_sheet(self, inputs: dict[str, Any]) -> ConnectorResult:
        """Add a new sheet/tab."""
        spreadsheet_id = inputs.get("spreadsheet_id", "")
        title = inputs.get("title", "New Sheet")

        if not spreadsheet_id:
            return ConnectorResult(success=False, error="Spreadsheet ID is required")

        return await self._request(
            "POST",
            f"{self.base_url}/{spreadsheet_id}:batchUpdate",
            params=self._get_api_key_param(),
            json={
                "requests": [
                    {
                        "addSheet": {
                            "properties": {"title": title}
                        }
                    }
                ]
            },
        )

    async def _clear_range(self, inputs: dict[str, Any]) -> ConnectorResult:
        """Clear data from a range."""
        spreadsheet_id = inputs.get("spreadsheet_id", "")
        range_name = inputs.get("range", "")

        if not spreadsheet_id or not range_name:
            return ConnectorResult(
                success=False, error="Spreadsheet ID and range are required"
            )

        return await self._request(
            "POST",
            f"{self.base_url}/{spreadsheet_id}/values/{range_name}:clear",
            params=self._get_api_key_param(),
        )

    async def test_connection(self) -> ConnectorResult:
        """Test the Google Sheets connection."""
        # Try to access the API - will return error info if auth fails
        test_id = self.credentials.get("test_spreadsheet_id", "")
        if test_id:
            return await self._get_spreadsheet({"spreadsheet_id": test_id})

        # Just verify the token works by hitting the API
        return await self._request(
            "GET",
            "https://www.googleapis.com/oauth2/v1/tokeninfo",
            params={"access_token": self.credentials.get("access_token", "")},
        )
