"""
Discord Connector

Send messages, manage channels, and interact with Discord.
"""

from typing import Any
from .base import BaseConnector, ConnectorResult


class DiscordConnector(BaseConnector):
    """Connector for Discord."""

    service_name = "discord"
    display_name = "Discord"
    base_url = "https://discord.com/api/v10"

    def get_actions(self) -> list[dict[str, str]]:
        return [
            {"action": "send_message", "description": "Send a message to a channel"},
            {"action": "send_embed", "description": "Send a rich embed message"},
            {"action": "create_thread", "description": "Create a thread in a channel"},
            {"action": "add_reaction", "description": "Add a reaction to a message"},
            {"action": "get_channel", "description": "Get channel information"},
            {"action": "list_channels", "description": "List guild channels"},
        ]

    def validate_credentials(self) -> bool:
        return "bot_token" in self.credentials

    def _get_auth_header(self) -> dict[str, str]:
        token = self.credentials.get("bot_token", "")
        return {"Authorization": f"Bot {token}"}

    async def execute(self, action: str, inputs: dict[str, Any]) -> ConnectorResult:
        """Execute a Discord action."""
        actions = {
            "send_message": self._send_message,
            "send_embed": self._send_embed,
            "create_thread": self._create_thread,
            "add_reaction": self._add_reaction,
            "get_channel": self._get_channel,
            "list_channels": self._list_channels,
        }

        if action not in actions:
            return ConnectorResult(success=False, error=f"Unknown action: {action}")

        return await actions[action](inputs)

    async def _send_message(self, inputs: dict[str, Any]) -> ConnectorResult:
        """Send a message to a channel."""
        channel_id = inputs.get("channel_id", "")
        content = inputs.get("content", inputs.get("message", ""))

        if not channel_id or not content:
            return ConnectorResult(success=False, error="Channel ID and content are required")

        return await self._request(
            "POST",
            f"{self.base_url}/channels/{channel_id}/messages",
            json={"content": content},
        )

    async def _send_embed(self, inputs: dict[str, Any]) -> ConnectorResult:
        """Send a rich embed message."""
        channel_id = inputs.get("channel_id", "")
        title = inputs.get("title", "")
        description = inputs.get("description", "")
        color = inputs.get("color", 5814783)  # Default blue
        fields = inputs.get("fields", [])

        if not channel_id:
            return ConnectorResult(success=False, error="Channel ID is required")

        embed = {
            "title": title,
            "description": description,
            "color": color,
        }

        if fields:
            embed["fields"] = fields

        if inputs.get("thumbnail"):
            embed["thumbnail"] = {"url": inputs["thumbnail"]}

        if inputs.get("image"):
            embed["image"] = {"url": inputs["image"]}

        if inputs.get("footer"):
            embed["footer"] = {"text": inputs["footer"]}

        return await self._request(
            "POST",
            f"{self.base_url}/channels/{channel_id}/messages",
            json={"embeds": [embed]},
        )

    async def _create_thread(self, inputs: dict[str, Any]) -> ConnectorResult:
        """Create a thread in a channel."""
        channel_id = inputs.get("channel_id", "")
        name = inputs.get("name", "")
        message_id = inputs.get("message_id")

        if not channel_id or not name:
            return ConnectorResult(success=False, error="Channel ID and name are required")

        if message_id:
            # Create thread from message
            return await self._request(
                "POST",
                f"{self.base_url}/channels/{channel_id}/messages/{message_id}/threads",
                json={"name": name},
            )
        else:
            # Create standalone thread
            return await self._request(
                "POST",
                f"{self.base_url}/channels/{channel_id}/threads",
                json={
                    "name": name,
                    "type": 11,  # Public thread
                },
            )

    async def _add_reaction(self, inputs: dict[str, Any]) -> ConnectorResult:
        """Add a reaction to a message."""
        channel_id = inputs.get("channel_id", "")
        message_id = inputs.get("message_id", "")
        emoji = inputs.get("emoji", "")

        if not channel_id or not message_id or not emoji:
            return ConnectorResult(
                success=False, error="Channel ID, message ID, and emoji are required"
            )

        # URL encode emoji
        import urllib.parse
        encoded_emoji = urllib.parse.quote(emoji)

        return await self._request(
            "PUT",
            f"{self.base_url}/channels/{channel_id}/messages/{message_id}/reactions/{encoded_emoji}/@me",
        )

    async def _get_channel(self, inputs: dict[str, Any]) -> ConnectorResult:
        """Get channel information."""
        channel_id = inputs.get("channel_id", "")

        if not channel_id:
            return ConnectorResult(success=False, error="Channel ID is required")

        return await self._request("GET", f"{self.base_url}/channels/{channel_id}")

    async def _list_channels(self, inputs: dict[str, Any]) -> ConnectorResult:
        """List guild channels."""
        guild_id = inputs.get("guild_id", "")

        if not guild_id:
            return ConnectorResult(success=False, error="Guild ID is required")

        return await self._request("GET", f"{self.base_url}/guilds/{guild_id}/channels")

    async def test_connection(self) -> ConnectorResult:
        """Test the Discord connection."""
        return await self._request("GET", f"{self.base_url}/users/@me")
