"""
Slack Connector

Send messages, manage channels, and interact with Slack.
"""

from typing import Any
from .base import BaseConnector, ConnectorResult


class SlackConnector(BaseConnector):
    """Connector for Slack."""

    service_name = "slack"
    display_name = "Slack"
    base_url = "https://slack.com/api"

    def get_actions(self) -> list[dict[str, str]]:
        return [
            {"action": "send_message", "description": "Send a message to a channel"},
            {"action": "send_dm", "description": "Send a direct message to a user"},
            {"action": "upload_file", "description": "Upload a file to a channel"},
            {"action": "add_reaction", "description": "Add a reaction to a message"},
            {"action": "list_channels", "description": "List all channels"},
            {"action": "get_user", "description": "Get user information"},
        ]

    def validate_credentials(self) -> bool:
        return "access_token" in self.credentials or "api_key" in self.credentials

    def _get_auth_header(self) -> dict[str, str]:
        token = self.credentials.get("access_token") or self.credentials.get("api_key")
        return {"Authorization": f"Bearer {token}"}

    async def execute(self, action: str, inputs: dict[str, Any]) -> ConnectorResult:
        """Execute a Slack action."""
        actions = {
            "send_message": self._send_message,
            "send_dm": self._send_dm,
            "upload_file": self._upload_file,
            "add_reaction": self._add_reaction,
            "list_channels": self._list_channels,
            "get_user": self._get_user,
        }

        if action not in actions:
            return ConnectorResult(success=False, error=f"Unknown action: {action}")

        return await actions[action](inputs)

    async def _send_message(self, inputs: dict[str, Any]) -> ConnectorResult:
        """Send a message to a channel."""
        channel = inputs.get("channel", "")
        message = inputs.get("message", "")
        blocks = inputs.get("blocks")

        if not channel or not message:
            return ConnectorResult(success=False, error="Channel and message are required")

        # Add # prefix if not present
        if not channel.startswith("#") and not channel.startswith("C"):
            channel = f"#{channel}"

        payload = {
            "channel": channel,
            "text": message,
        }
        if blocks:
            payload["blocks"] = blocks

        return await self._request(
            "POST",
            f"{self.base_url}/chat.postMessage",
            json=payload,
        )

    async def _send_dm(self, inputs: dict[str, Any]) -> ConnectorResult:
        """Send a direct message to a user."""
        user = inputs.get("user", "")
        message = inputs.get("message", "")

        if not user or not message:
            return ConnectorResult(success=False, error="User and message are required")

        # First, open a DM channel
        dm_result = await self._request(
            "POST",
            f"{self.base_url}/conversations.open",
            json={"users": user},
        )

        if not dm_result.success:
            return dm_result

        channel_id = dm_result.data.get("channel", {}).get("id")
        if not channel_id:
            return ConnectorResult(success=False, error="Could not open DM channel")

        # Send the message
        return await self._request(
            "POST",
            f"{self.base_url}/chat.postMessage",
            json={"channel": channel_id, "text": message},
        )

    async def _upload_file(self, inputs: dict[str, Any]) -> ConnectorResult:
        """Upload a file to a channel."""
        channel = inputs.get("channel", "")
        content = inputs.get("content", "")
        filename = inputs.get("filename", "file.txt")
        title = inputs.get("title", filename)

        if not channel or not content:
            return ConnectorResult(success=False, error="Channel and content are required")

        return await self._request(
            "POST",
            f"{self.base_url}/files.upload",
            data={
                "channels": channel,
                "content": content,
                "filename": filename,
                "title": title,
            },
        )

    async def _add_reaction(self, inputs: dict[str, Any]) -> ConnectorResult:
        """Add a reaction to a message."""
        channel = inputs.get("channel", "")
        timestamp = inputs.get("timestamp", "")
        emoji = inputs.get("emoji", "").strip(":")

        if not channel or not timestamp or not emoji:
            return ConnectorResult(success=False, error="Channel, timestamp, and emoji are required")

        return await self._request(
            "POST",
            f"{self.base_url}/reactions.add",
            json={
                "channel": channel,
                "timestamp": timestamp,
                "name": emoji,
            },
        )

    async def _list_channels(self, inputs: dict[str, Any]) -> ConnectorResult:
        """List all channels."""
        return await self._request(
            "GET",
            f"{self.base_url}/conversations.list",
            params={"types": "public_channel,private_channel"},
        )

    async def _get_user(self, inputs: dict[str, Any]) -> ConnectorResult:
        """Get user information."""
        user_id = inputs.get("user_id", "")
        if not user_id:
            return ConnectorResult(success=False, error="User ID is required")

        return await self._request(
            "GET",
            f"{self.base_url}/users.info",
            params={"user": user_id},
        )

    async def test_connection(self) -> ConnectorResult:
        """Test the Slack connection."""
        return await self._request("GET", f"{self.base_url}/auth.test")
