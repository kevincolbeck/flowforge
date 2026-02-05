"""
Webhook Connector

Send and manage webhooks.
"""

import json
from typing import Any
from .base import BaseConnector, ConnectorResult


class WebhookConnector(BaseConnector):
    """Connector for sending webhooks."""

    service_name = "webhook"
    display_name = "Webhooks"

    def get_actions(self) -> list[dict[str, str]]:
        return [
            {"action": "send", "description": "Send a webhook with JSON payload"},
            {"action": "send_form", "description": "Send a webhook with form data"},
        ]

    async def execute(self, action: str, inputs: dict[str, Any]) -> ConnectorResult:
        """Execute a webhook action."""
        if action == "send":
            return await self._send_json(inputs)
        elif action == "send_form":
            return await self._send_form(inputs)
        else:
            return ConnectorResult(success=False, error=f"Unknown action: {action}")

    async def _send_json(self, inputs: dict[str, Any]) -> ConnectorResult:
        """Send a webhook with JSON payload."""
        url = inputs.get("url", "")
        if not url:
            return ConnectorResult(success=False, error="URL is required")

        payload = inputs.get("payload", inputs.get("body", {}))
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except:
                payload = {"data": payload}

        headers = inputs.get("headers", {})
        if isinstance(headers, str):
            try:
                headers = json.loads(headers)
            except:
                headers = {}

        return await self._request(
            method="POST",
            url=url,
            headers={"Content-Type": "application/json", **headers},
            json=payload,
        )

    async def _send_form(self, inputs: dict[str, Any]) -> ConnectorResult:
        """Send a webhook with form data."""
        url = inputs.get("url", "")
        if not url:
            return ConnectorResult(success=False, error="URL is required")

        payload = inputs.get("payload", inputs.get("body", {}))
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except:
                payload = {"data": payload}

        return await self._request(
            method="POST",
            url=url,
            data=payload,
        )

    async def test_connection(self) -> ConnectorResult:
        """Test webhook by sending to httpbin."""
        return await self._send_json({
            "url": "https://httpbin.org/post",
            "payload": {"test": True}
        })
