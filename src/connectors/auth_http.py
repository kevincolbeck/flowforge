"""
Authenticated HTTP Connector

Makes HTTP requests to any API with automatic token-based authentication.
Supports OAuth2, custom auth endpoints, and any token format.

This connector handles the complete flow:
1. Authenticate (call auth endpoint, get token)
2. Cache the token
3. Inject token into subsequent requests
4. Auto-refresh when expired
"""

import json
from typing import Any, Optional
from .base import BaseConnector, ConnectorResult
from .auth import (
    OAuth2Config,
    OAuth2Provider,
    CustomAuthConfig,
    CustomAuthProvider,
    AuthRequest,
    TokenExtraction,
    TokenInjection,
    RefreshConfig,
    TokenData,
    get_default_store,
)


class AuthenticatedHTTPConnector(BaseConnector):
    """
    HTTP connector with automatic token-based authentication.

    Credentials configuration:
    {
        # === AUTH CONFIGURATION ===

        # Option 1: OAuth2 Client Credentials
        "auth_type": "oauth2_client",
        "token_url": "https://api.example.com/oauth/token",
        "client_id": "your_client_id",
        "client_secret": "your_client_secret",
        "scope": "read write",  # optional

        # Option 2: OAuth2 Password Grant
        "auth_type": "oauth2_password",
        "token_url": "https://api.example.com/oauth/token",
        "client_id": "your_client_id",
        "client_secret": "your_client_secret",  # optional
        "username": "user@example.com",
        "password": "secret",

        # Option 3: Custom Login Endpoint
        "auth_type": "login",
        "auth_url": "https://api.example.com/auth/login",
        "auth_body": {"email": "user@example.com", "password": "secret"},
        "token_path": "data.access_token",  # where to find token in response

        # Option 4: API Key Exchange
        "auth_type": "api_key_exchange",
        "auth_url": "https://api.example.com/auth/token",
        "api_key": "your_api_key",
        "api_key_field": "apiKey",  # field name in request body
        "token_path": "token",

        # Option 5: Fully Custom Auth
        "auth_type": "custom",
        "auth_url": "https://api.example.com/authenticate",
        "auth_method": "POST",
        "auth_body": {...},
        "auth_body_format": "json",  # or "form"
        "auth_headers": {...},
        "token_path": "result.token",
        "token_type_path": "result.type",
        "expires_in_path": "result.expires_in",
        "refresh_token_path": "result.refresh_token",
        "refresh_url": "https://api.example.com/refresh",  # optional

        # === TOKEN INJECTION ===
        "inject_location": "header",  # "header", "query", "cookie"
        "inject_key": "Authorization",  # header name or query param
        "inject_prefix": "Bearer ",  # prefix before token

        # === BASE URL ===
        "base_url": "https://api.example.com",  # optional, prepended to URLs
    }
    """

    service_name = "auth_http"
    display_name = "Authenticated HTTP"

    def __init__(self, credentials: dict[str, Any]):
        super().__init__(credentials)
        self._auth_provider = self._create_auth_provider()
        self._cached_token: Optional[TokenData] = None

    def _create_auth_provider(self):
        """Create the appropriate auth provider based on credentials."""
        auth_type = self.credentials.get("auth_type", "none")

        if auth_type == "oauth2_client":
            return self._create_oauth2_client_provider()
        elif auth_type == "oauth2_password":
            return self._create_oauth2_password_provider()
        elif auth_type == "login":
            return self._create_login_provider()
        elif auth_type == "api_key_exchange":
            return self._create_api_key_provider()
        elif auth_type == "custom":
            return self._create_custom_provider()
        else:
            return None

    def _create_oauth2_client_provider(self) -> OAuth2Provider:
        """Create OAuth2 client credentials provider."""
        config = OAuth2Config(
            token_url=self.credentials["token_url"],
            client_id=self.credentials["client_id"],
            client_secret=self.credentials.get("client_secret"),
            scope=self.credentials.get("scope"),
            audience=self.credentials.get("audience"),
            auth_method=self.credentials.get("auth_method", "body"),
        )
        return OAuth2Provider(config)

    def _create_oauth2_password_provider(self) -> OAuth2Provider:
        """Create OAuth2 password grant provider."""
        config = OAuth2Config(
            token_url=self.credentials["token_url"],
            client_id=self.credentials["client_id"],
            client_secret=self.credentials.get("client_secret"),
            username=self.credentials["username"],
            password=self.credentials["password"],
            scope=self.credentials.get("scope"),
            audience=self.credentials.get("audience"),
        )
        return OAuth2Provider(config)

    def _create_login_provider(self) -> CustomAuthProvider:
        """Create custom login provider."""
        config = CustomAuthConfig(
            request=AuthRequest(
                url=self.credentials["auth_url"],
                method=self.credentials.get("auth_method", "POST"),
                body=self.credentials.get("auth_body", {}),
                body_format=self.credentials.get("auth_body_format", "json"),
                headers=self.credentials.get("auth_headers"),
            ),
            extraction=TokenExtraction(
                token_path=self.credentials.get("token_path", "access_token"),
                fallback_paths=["token", "data.token", "data.access_token", "result.token"],
                expires_in_path=self.credentials.get("expires_in_path"),
                expires_at_path=self.credentials.get("expires_at_path"),
                refresh_token_path=self.credentials.get("refresh_token_path"),
                default_expires_in=self.credentials.get("default_expires_in"),
            ),
            injection=self._get_injection_config(),
            refresh=self._get_refresh_config(),
        )
        return CustomAuthProvider(config)

    def _create_api_key_provider(self) -> CustomAuthProvider:
        """Create API key exchange provider."""
        body = {
            self.credentials.get("api_key_field", "api_key"): self.credentials["api_key"]
        }
        # Add any extra body fields
        if self.credentials.get("auth_body"):
            body.update(self.credentials["auth_body"])

        config = CustomAuthConfig(
            request=AuthRequest(
                url=self.credentials["auth_url"],
                method=self.credentials.get("auth_method", "POST"),
                body=body,
                body_format=self.credentials.get("auth_body_format", "json"),
                headers=self.credentials.get("auth_headers"),
            ),
            extraction=TokenExtraction(
                token_path=self.credentials.get("token_path", "token"),
                fallback_paths=["access_token", "data.token", "result.token"],
                expires_in_path=self.credentials.get("expires_in_path"),
                default_expires_in=self.credentials.get("default_expires_in"),
            ),
            injection=self._get_injection_config(),
        )
        return CustomAuthProvider(config)

    def _create_custom_provider(self) -> CustomAuthProvider:
        """Create fully custom auth provider."""
        config = CustomAuthConfig(
            request=AuthRequest(
                url=self.credentials["auth_url"],
                method=self.credentials.get("auth_method", "POST"),
                body=self.credentials.get("auth_body"),
                body_format=self.credentials.get("auth_body_format", "json"),
                headers=self.credentials.get("auth_headers"),
                params=self.credentials.get("auth_params"),
                basic_auth=(
                    (self.credentials["basic_user"], self.credentials["basic_pass"])
                    if self.credentials.get("basic_user") else None
                ),
            ),
            extraction=TokenExtraction(
                token_path=self.credentials.get("token_path", "access_token"),
                fallback_paths=self.credentials.get("token_fallback_paths"),
                token_type_path=self.credentials.get("token_type_path"),
                expires_in_path=self.credentials.get("expires_in_path"),
                expires_at_path=self.credentials.get("expires_at_path"),
                refresh_token_path=self.credentials.get("refresh_token_path"),
                regex_pattern=self.credentials.get("token_regex"),
                header_name=self.credentials.get("token_header"),
                default_expires_in=self.credentials.get("default_expires_in"),
            ),
            injection=self._get_injection_config(),
            refresh=self._get_refresh_config(),
        )
        return CustomAuthProvider(config)

    def _get_injection_config(self) -> TokenInjection:
        """Get token injection configuration."""
        return TokenInjection(
            location=self.credentials.get("inject_location", "header"),
            key=self.credentials.get("inject_key", "Authorization"),
            prefix=self.credentials.get("inject_prefix", "Bearer "),
        )

    def _get_refresh_config(self) -> Optional[RefreshConfig]:
        """Get refresh configuration if specified."""
        refresh_url = self.credentials.get("refresh_url")
        if not refresh_url:
            return None

        return RefreshConfig(
            url=refresh_url,
            method=self.credentials.get("refresh_method", "POST"),
            body_template=self.credentials.get("refresh_body"),
        )

    @classmethod
    def get_actions(cls) -> dict[str, dict[str, Any]]:
        return {
            "request": {
                "description": "Make an authenticated HTTP request",
                "parameters": {
                    "method": {"type": "string", "description": "HTTP method (GET, POST, PUT, PATCH, DELETE)", "required": True},
                    "url": {"type": "string", "description": "URL to request (absolute or relative to base_url)", "required": True},
                    "headers": {"type": "object", "description": "Additional headers", "required": False},
                    "body": {"type": "object", "description": "Request body", "required": False},
                    "params": {"type": "object", "description": "Query parameters", "required": False},
                },
            },
            "get": {
                "description": "Make an authenticated GET request",
                "parameters": {
                    "url": {"type": "string", "description": "URL to request", "required": True},
                    "headers": {"type": "object", "description": "Additional headers", "required": False},
                    "params": {"type": "object", "description": "Query parameters", "required": False},
                },
            },
            "post": {
                "description": "Make an authenticated POST request",
                "parameters": {
                    "url": {"type": "string", "description": "URL to request", "required": True},
                    "body": {"type": "object", "description": "Request body", "required": False},
                    "headers": {"type": "object", "description": "Additional headers", "required": False},
                    "params": {"type": "object", "description": "Query parameters", "required": False},
                },
            },
            "put": {
                "description": "Make an authenticated PUT request",
                "parameters": {
                    "url": {"type": "string", "description": "URL to request", "required": True},
                    "body": {"type": "object", "description": "Request body", "required": False},
                    "headers": {"type": "object", "description": "Additional headers", "required": False},
                },
            },
            "patch": {
                "description": "Make an authenticated PATCH request",
                "parameters": {
                    "url": {"type": "string", "description": "URL to request", "required": True},
                    "body": {"type": "object", "description": "Request body", "required": False},
                    "headers": {"type": "object", "description": "Additional headers", "required": False},
                },
            },
            "delete": {
                "description": "Make an authenticated DELETE request",
                "parameters": {
                    "url": {"type": "string", "description": "URL to request", "required": True},
                    "headers": {"type": "object", "description": "Additional headers", "required": False},
                },
            },
            "authenticate": {
                "description": "Manually trigger authentication (useful for testing)",
                "parameters": {
                    "force_refresh": {"type": "boolean", "description": "Force new token even if cached", "required": False},
                },
            },
            "get_token": {
                "description": "Get the current access token (for debugging)",
                "parameters": {},
            },
            "clear_token": {
                "description": "Clear the cached token",
                "parameters": {},
            },
        }

    async def _get_token(self, force_refresh: bool = False) -> Optional[TokenData]:
        """Get a valid access token."""
        if not self._auth_provider:
            return None

        # Use cached token if valid
        if not force_refresh and self._cached_token and not self._cached_token.is_expired:
            return self._cached_token

        # Get new token
        self._cached_token = await self._auth_provider.get_token(force_refresh)
        return self._cached_token

    def _build_url(self, url: str) -> str:
        """Build full URL with optional base_url."""
        base_url = self.credentials.get("base_url", "").rstrip("/")
        if base_url and not url.startswith(("http://", "https://")):
            return f"{base_url}/{url.lstrip('/')}"
        return url

    async def _authenticated_request(
        self,
        method: str,
        url: str,
        headers: dict = None,
        body: Any = None,
        params: dict = None,
    ) -> ConnectorResult:
        """Make an authenticated HTTP request."""
        # Get token
        token = await self._get_token()

        # Build headers
        request_headers = dict(headers or {})

        # Apply authentication
        if token:
            if isinstance(self._auth_provider, CustomAuthProvider):
                request_headers, params, body = self._auth_provider.apply_auth(
                    headers=request_headers,
                    params=params,
                    body=body if isinstance(body, dict) else None,
                    token=token,
                )
            else:
                # Default Bearer token injection
                inject_key = self.credentials.get("inject_key", "Authorization")
                inject_prefix = self.credentials.get("inject_prefix", "Bearer ")
                inject_location = self.credentials.get("inject_location", "header")

                if inject_location == "header":
                    request_headers[inject_key] = f"{inject_prefix}{token.access_token}"
                elif inject_location == "query":
                    params = params or {}
                    params[inject_key] = token.access_token

        # Build full URL
        full_url = self._build_url(url)

        # Make request
        return await self._request(
            method=method,
            url=full_url,
            headers=request_headers,
            json=body if isinstance(body, dict) else None,
            data=body if isinstance(body, str) else None,
            params=params,
        )

    async def execute(self, action: str, params: dict[str, Any]) -> ConnectorResult:
        """Execute an action."""
        try:
            if action == "request":
                return await self._authenticated_request(
                    method=params["method"].upper(),
                    url=params["url"],
                    headers=self._parse_json(params.get("headers")),
                    body=self._parse_json(params.get("body")),
                    params=self._parse_json(params.get("params")),
                )

            elif action in ["get", "post", "put", "patch", "delete"]:
                return await self._authenticated_request(
                    method=action.upper(),
                    url=params["url"],
                    headers=self._parse_json(params.get("headers")),
                    body=self._parse_json(params.get("body")),
                    params=self._parse_json(params.get("params")),
                )

            elif action == "authenticate":
                token = await self._get_token(force_refresh=params.get("force_refresh", False))
                if token:
                    return ConnectorResult(
                        success=True,
                        data={
                            "authenticated": True,
                            "token_type": token.token_type,
                            "expires_in": token.expires_in,
                            "has_refresh_token": bool(token.refresh_token),
                        }
                    )
                return ConnectorResult(success=False, error="Authentication not configured")

            elif action == "get_token":
                token = await self._get_token()
                if token:
                    return ConnectorResult(
                        success=True,
                        data={
                            "access_token": token.access_token[:20] + "..." if len(token.access_token) > 20 else token.access_token,
                            "token_type": token.token_type,
                            "expires_in": token.expires_in,
                            "is_expired": token.is_expired,
                        }
                    )
                return ConnectorResult(success=False, error="No token available")

            elif action == "clear_token":
                if self._auth_provider:
                    await self._auth_provider.clear_cached_token()
                self._cached_token = None
                return ConnectorResult(success=True, data={"cleared": True})

            else:
                return ConnectorResult(success=False, error=f"Unknown action: {action}")

        except Exception as e:
            return ConnectorResult(success=False, error=str(e))

    def _parse_json(self, value: Any) -> Any:
        """Parse JSON string if needed."""
        if isinstance(value, str):
            try:
                return json.loads(value)
            except Exception:
                return value
        return value

    async def test_connection(self) -> ConnectorResult:
        """Test by authenticating."""
        try:
            token = await self._get_token()
            if token:
                return ConnectorResult(
                    success=True,
                    data={"message": "Authentication successful", "token_type": token.token_type}
                )
            return ConnectorResult(success=True, data={"message": "No auth configured"})
        except Exception as e:
            return ConnectorResult(success=False, error=str(e))
