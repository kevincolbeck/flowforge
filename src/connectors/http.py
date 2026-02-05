"""
HTTP Connector

Make HTTP requests to any API.
"""

import json
from typing import Any
from .base import BaseConnector, ConnectorResult


class HTTPConnector(BaseConnector):
    """Generic HTTP connector for any API."""

    service_name = "http"
    display_name = "HTTP Request"

    def get_actions(self) -> list[dict[str, str]]:
        return [
            {"action": "get", "description": "Make a GET request"},
            {"action": "post", "description": "Make a POST request"},
            {"action": "put", "description": "Make a PUT request"},
            {"action": "patch", "description": "Make a PATCH request"},
            {"action": "delete", "description": "Make a DELETE request"},
        ]

    def _get_auth_header(self) -> dict[str, str]:
        """Build auth header based on credentials."""
        headers = {}

        auth_type = self.credentials.get("auth_type", "none")

        if auth_type == "bearer" and self.credentials.get("token"):
            headers["Authorization"] = f"Bearer {self.credentials['token']}"
        elif auth_type == "api_key":
            key_name = self.credentials.get("key_name", "X-API-Key")
            key_value = self.credentials.get("api_key", "")
            if self.credentials.get("key_location", "header") == "header":
                headers[key_name] = key_value
        elif auth_type == "basic":
            import base64
            username = self.credentials.get("username", "")
            password = self.credentials.get("password", "")
            encoded = base64.b64encode(f"{username}:{password}".encode()).decode()
            headers["Authorization"] = f"Basic {encoded}"

        return headers

    async def execute(self, action: str, inputs: dict[str, Any]) -> ConnectorResult:
        """Execute an HTTP request."""
        method = action.upper()
        if method not in ["GET", "POST", "PUT", "PATCH", "DELETE"]:
            return ConnectorResult(success=False, error=f"Invalid HTTP method: {action}")

        url = inputs.get("url", "")
        if not url:
            return ConnectorResult(success=False, error="URL is required")

        # Parse headers
        headers = inputs.get("headers", {})
        if isinstance(headers, str):
            try:
                headers = json.loads(headers)
            except:
                headers = {}

        # Parse body
        body = inputs.get("body")
        if isinstance(body, str):
            try:
                body = json.loads(body)
            except:
                pass  # Keep as string

        # Parse query params
        params = inputs.get("params", {})
        if isinstance(params, str):
            try:
                params = json.loads(params)
            except:
                params = {}

        # Add API key to params if needed
        if self.credentials.get("auth_type") == "api_key":
            if self.credentials.get("key_location") == "query":
                key_name = self.credentials.get("key_name", "api_key")
                params[key_name] = self.credentials.get("api_key", "")

        # Make request
        return await self._request(
            method=method,
            url=url,
            headers=headers,
            json=body if isinstance(body, dict) else None,
            data=body if isinstance(body, str) else None,
            params=params if params else None,
        )

    async def test_connection(self) -> ConnectorResult:
        """Test by making a simple request."""
        test_url = self.credentials.get("test_url", "https://httpbin.org/get")
        return await self.execute("get", {"url": test_url})
