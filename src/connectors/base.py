"""
Base Connector

Abstract base class for all service connectors.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any
import httpx


@dataclass
class ConnectorResult:
    """Result of a connector action."""
    success: bool
    data: Any = None
    error: str | None = None
    status_code: int | None = None
    raw_response: Any = None

    def to_dict(self) -> dict:
        return {
            "success": self.success,
            "data": self.data,
            "error": self.error,
            "status_code": self.status_code,
        }


class BaseConnector(ABC):
    """Base class for all service connectors."""

    service_name: str = "base"
    display_name: str = "Base Connector"

    def __init__(self, credentials: dict[str, str] | None = None):
        self.credentials = credentials or {}
        self.client = httpx.AsyncClient(timeout=30.0)

    async def close(self):
        """Close the HTTP client."""
        await self.client.aclose()

    @abstractmethod
    async def execute(self, action: str, inputs: dict[str, Any]) -> ConnectorResult:
        """Execute an action with the given inputs."""
        pass

    @abstractmethod
    def get_actions(self) -> list[dict[str, str]]:
        """Get list of available actions."""
        pass

    def validate_credentials(self) -> bool:
        """Validate that required credentials are present."""
        return True

    async def test_connection(self) -> ConnectorResult:
        """Test the connection to the service."""
        return ConnectorResult(success=True, data={"status": "ok"})

    def _get_auth_header(self) -> dict[str, str]:
        """Get authentication headers."""
        if "api_key" in self.credentials:
            return {"Authorization": f"Bearer {self.credentials['api_key']}"}
        if "access_token" in self.credentials:
            return {"Authorization": f"Bearer {self.credentials['access_token']}"}
        return {}

    async def _request(
        self,
        method: str,
        url: str,
        headers: dict | None = None,
        json: dict | None = None,
        params: dict | None = None,
        data: dict | None = None,
    ) -> ConnectorResult:
        """Make an HTTP request with error handling."""
        try:
            all_headers = {**self._get_auth_header(), **(headers or {})}

            response = await self.client.request(
                method=method,
                url=url,
                headers=all_headers,
                json=json,
                params=params,
                data=data,
            )

            # Try to parse JSON response
            try:
                response_data = response.json()
            except:
                response_data = response.text

            if response.status_code >= 400:
                return ConnectorResult(
                    success=False,
                    error=f"HTTP {response.status_code}: {response_data}",
                    status_code=response.status_code,
                    raw_response=response_data,
                )

            return ConnectorResult(
                success=True,
                data=response_data,
                status_code=response.status_code,
                raw_response=response_data,
            )

        except httpx.TimeoutException:
            return ConnectorResult(success=False, error="Request timed out")
        except httpx.RequestError as e:
            return ConnectorResult(success=False, error=f"Request error: {str(e)}")
        except Exception as e:
            return ConnectorResult(success=False, error=f"Unexpected error: {str(e)}")
