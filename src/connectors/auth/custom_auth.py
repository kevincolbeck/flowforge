"""
Custom Authentication Provider

A fully configurable authentication provider for any API that returns tokens.
Works with non-standard auth endpoints, custom response formats, and various
authentication patterns.
"""

import re
import time
import json
import base64
from typing import Any, Optional, Callable, Union
from dataclasses import dataclass, field
import httpx

from .token_store import TokenData, TokenStore, get_default_store


@dataclass
class AuthRequest:
    """Configuration for the authentication request."""
    # Request details
    url: str
    method: str = "POST"

    # Request body
    body: Optional[dict] = None
    body_format: str = "json"  # "json", "form", "raw"
    raw_body: Optional[str] = None

    # Headers
    headers: Optional[dict] = None

    # Query parameters
    params: Optional[dict] = None

    # Basic auth for the auth request itself
    basic_auth: Optional[tuple[str, str]] = None  # (username, password)

    # Timeout
    timeout: float = 30.0


@dataclass
class TokenExtraction:
    """
    Configuration for extracting token from response.

    Supports:
    - JSON path extraction (e.g., "data.access_token", "result.token")
    - Regex extraction from response body
    - Header extraction
    - Custom extraction function
    """
    # JSON path to access token (dot notation)
    # e.g., "access_token", "data.token", "response.credentials.jwt"
    token_path: str = "access_token"

    # Alternative paths to try (in order)
    fallback_paths: Optional[list[str]] = None

    # JSON path to token type (default: Bearer)
    token_type_path: Optional[str] = None

    # JSON path to expiration (seconds from now)
    expires_in_path: Optional[str] = None

    # JSON path to absolute expiration timestamp
    expires_at_path: Optional[str] = None

    # JSON path to refresh token
    refresh_token_path: Optional[str] = None

    # Regex pattern to extract token from raw response
    # Group 1 should capture the token
    regex_pattern: Optional[str] = None

    # Extract from response header instead of body
    header_name: Optional[str] = None

    # Default expiration in seconds if not provided in response
    default_expires_in: Optional[int] = None


@dataclass
class TokenInjection:
    """
    Configuration for injecting token into subsequent requests.

    Supports:
    - Header injection (Authorization: Bearer xxx)
    - Custom header (X-Auth-Token: xxx)
    - Query parameter
    - Cookie
    - Body field
    """
    # Where to inject: "header", "query", "cookie", "body"
    location: str = "header"

    # Key/name for the token
    key: str = "Authorization"

    # Prefix to add before token (e.g., "Bearer ", "Token ", "")
    prefix: str = "Bearer "

    # For multiple injection points
    additional: Optional[list["TokenInjection"]] = None


@dataclass
class RefreshConfig:
    """Configuration for token refresh."""
    # Refresh endpoint (if different from auth endpoint)
    url: Optional[str] = None

    # How to send refresh token
    method: str = "POST"
    body_template: Optional[dict] = None  # Use {refresh_token} placeholder
    header_name: Optional[str] = None  # Send in header instead

    # Extraction config for refresh response (if different)
    extraction: Optional[TokenExtraction] = None


@dataclass
class CustomAuthConfig:
    """Complete configuration for custom authentication."""
    # Auth request configuration
    request: AuthRequest

    # Token extraction configuration
    extraction: TokenExtraction = field(default_factory=TokenExtraction)

    # Token injection configuration
    injection: TokenInjection = field(default_factory=TokenInjection)

    # Refresh configuration (optional)
    refresh: Optional[RefreshConfig] = None

    # Identifier for caching (auto-generated if not provided)
    cache_key: Optional[str] = None


class CustomAuthProvider:
    """
    Flexible authentication provider for any token-based API.

    Handles:
    - Any auth endpoint format
    - Any response format
    - Any token location/format
    - Automatic refresh
    - Token caching
    """

    def __init__(
        self,
        config: CustomAuthConfig,
        store: Optional[TokenStore] = None,
    ):
        self.config = config
        self.store = store or get_default_store()
        self._cache_key = config.cache_key or self._generate_cache_key()

    def _generate_cache_key(self) -> str:
        """Generate a unique cache key."""
        return TokenStore.generate_key(
            self.config.request.url,
            json.dumps(self.config.request.body, sort_keys=True) if self.config.request.body else "",
        )

    def _extract_value(self, data: Any, path: str) -> Any:
        """Extract a value from nested dict using dot notation path."""
        if not path or data is None:
            return None

        parts = path.split(".")
        current = data

        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            elif isinstance(current, list) and part.isdigit():
                idx = int(part)
                current = current[idx] if idx < len(current) else None
            else:
                return None

            if current is None:
                return None

        return current

    def _extract_token(self, response: httpx.Response, request_time: float) -> TokenData:
        """Extract token from response based on configuration."""
        ext = self.config.extraction

        # Try header extraction first
        if ext.header_name:
            access_token = response.headers.get(ext.header_name, "")
            return TokenData(
                access_token=access_token,
                expires_at=time.time() + ext.default_expires_in if ext.default_expires_in else None,
            )

        # Parse response body
        try:
            data = response.json()
        except Exception:
            data = response.text

        # Try regex extraction on raw text
        if ext.regex_pattern and isinstance(data, str):
            match = re.search(ext.regex_pattern, data)
            if match:
                access_token = match.group(1) if match.groups() else match.group(0)
                return TokenData(
                    access_token=access_token,
                    expires_at=time.time() + ext.default_expires_in if ext.default_expires_in else None,
                )

        # JSON path extraction
        if isinstance(data, dict):
            # Try main path
            access_token = self._extract_value(data, ext.token_path)

            # Try fallback paths
            if not access_token and ext.fallback_paths:
                for path in ext.fallback_paths:
                    access_token = self._extract_value(data, path)
                    if access_token:
                        break

            if not access_token:
                # Last resort: use TokenData.from_response which tries many common patterns
                return TokenData.from_response(data, request_time)

            # Extract other fields
            token_type = self._extract_value(data, ext.token_type_path) if ext.token_type_path else "Bearer"
            refresh_token = self._extract_value(data, ext.refresh_token_path) if ext.refresh_token_path else None

            # Calculate expiration
            expires_at = None
            if ext.expires_at_path:
                expires_at = self._extract_value(data, ext.expires_at_path)
                if expires_at:
                    expires_at = float(expires_at)
            elif ext.expires_in_path:
                expires_in = self._extract_value(data, ext.expires_in_path)
                if expires_in:
                    expires_at = request_time + float(expires_in)
            elif ext.default_expires_in:
                expires_at = request_time + ext.default_expires_in

            return TokenData(
                access_token=str(access_token),
                token_type=token_type or "Bearer",
                expires_at=expires_at,
                refresh_token=refresh_token,
            )

        raise AuthError(f"Could not extract token from response: {response.text[:200]}")

    async def _make_auth_request(self, request: AuthRequest) -> tuple[httpx.Response, float]:
        """Make the authentication request."""
        request_time = time.time()

        # Build headers
        headers = dict(request.headers or {})

        # Determine content type
        if request.body_format == "json":
            headers.setdefault("Content-Type", "application/json")
        elif request.body_format == "form":
            headers.setdefault("Content-Type", "application/x-www-form-urlencoded")

        # Build request kwargs
        kwargs: dict[str, Any] = {
            "method": request.method,
            "url": request.url,
            "headers": headers,
            "timeout": request.timeout,
        }

        # Add body
        if request.raw_body:
            kwargs["content"] = request.raw_body
        elif request.body:
            if request.body_format == "json":
                kwargs["json"] = request.body
            elif request.body_format == "form":
                kwargs["data"] = request.body

        # Add query params
        if request.params:
            kwargs["params"] = request.params

        # Add basic auth
        if request.basic_auth:
            kwargs["auth"] = request.basic_auth

        async with httpx.AsyncClient() as client:
            response = await client.request(**kwargs)

        if response.status_code >= 400:
            raise AuthError(
                f"Auth request failed with status {response.status_code}: {response.text[:500]}",
                response.status_code
            )

        return response, request_time

    async def get_token(self, force_refresh: bool = False) -> TokenData:
        """
        Get a valid access token.

        Args:
            force_refresh: If True, ignores cache and fetches new token

        Returns:
            TokenData with valid access token
        """
        if not force_refresh:
            # Try cached token
            cached = await self.store.get(self._cache_key)
            if cached and not cached.is_expired:
                return cached

            # Try refresh
            if cached and cached.refresh_token and self.config.refresh:
                try:
                    return await self._refresh_token(cached.refresh_token)
                except Exception:
                    pass

        # Get new token
        response, request_time = await self._make_auth_request(self.config.request)
        token = self._extract_token(response, request_time)

        # Cache token
        await self.store.set(self._cache_key, token)

        return token

    async def _refresh_token(self, refresh_token: str) -> TokenData:
        """Refresh the access token."""
        refresh_cfg = self.config.refresh

        if not refresh_cfg:
            raise AuthError("Refresh not configured")

        # Build refresh request
        url = refresh_cfg.url or self.config.request.url

        body = None
        if refresh_cfg.body_template:
            # Replace placeholder with actual refresh token
            body = json.loads(
                json.dumps(refresh_cfg.body_template).replace("{refresh_token}", refresh_token)
            )

        headers = None
        if refresh_cfg.header_name:
            headers = {refresh_cfg.header_name: refresh_token}

        request = AuthRequest(
            url=url,
            method=refresh_cfg.method,
            body=body,
            headers=headers,
        )

        response, request_time = await self._make_auth_request(request)

        extraction = refresh_cfg.extraction or self.config.extraction
        old_extraction = self.config.extraction
        self.config.extraction = extraction

        try:
            token = self._extract_token(response, request_time)
        finally:
            self.config.extraction = old_extraction

        # Keep refresh token if not in response
        if not token.refresh_token:
            token.refresh_token = refresh_token

        await self.store.set(self._cache_key, token)
        return token

    def apply_auth(
        self,
        headers: dict = None,
        params: dict = None,
        body: dict = None,
        token: TokenData = None,
    ) -> tuple[dict, dict, dict]:
        """
        Apply authentication to request components.

        Args:
            headers: Request headers dict
            params: Query parameters dict
            body: Request body dict
            token: Token to apply (required)

        Returns:
            Tuple of (headers, params, body) with auth applied
        """
        headers = dict(headers or {})
        params = dict(params or {})
        body = dict(body or {}) if body else None

        if not token:
            return headers, params, body

        inj = self.config.injection
        token_value = f"{inj.prefix}{token.access_token}"

        if inj.location == "header":
            headers[inj.key] = token_value
        elif inj.location == "query":
            params[inj.key] = token.access_token
        elif inj.location == "cookie":
            existing = headers.get("Cookie", "")
            cookie = f"{inj.key}={token.access_token}"
            headers["Cookie"] = f"{existing}; {cookie}" if existing else cookie
        elif inj.location == "body" and body is not None:
            body[inj.key] = token.access_token

        # Apply additional injection points
        if inj.additional:
            for add_inj in inj.additional:
                add_value = f"{add_inj.prefix}{token.access_token}"
                if add_inj.location == "header":
                    headers[add_inj.key] = add_value
                elif add_inj.location == "query":
                    params[add_inj.key] = token.access_token
                elif add_inj.location == "cookie":
                    existing = headers.get("Cookie", "")
                    cookie = f"{add_inj.key}={token.access_token}"
                    headers["Cookie"] = f"{existing}; {cookie}" if existing else cookie
                elif add_inj.location == "body" and body is not None:
                    body[add_inj.key] = token.access_token

        return headers, params, body

    async def clear_cached_token(self) -> None:
        """Clear the cached token."""
        await self.store.delete(self._cache_key)


class AuthError(Exception):
    """Authentication error."""

    def __init__(self, message: str, status_code: int = None):
        super().__init__(message)
        self.status_code = status_code


# Convenience factory functions

def create_api_key_auth(
    login_url: str,
    api_key: str,
    api_key_field: str = "api_key",
    token_path: str = "access_token",
    method: str = "POST",
    **extra_body_fields,
) -> CustomAuthProvider:
    """Create auth provider for APIs that exchange an API key for a session token."""
    body = {api_key_field: api_key}
    body.update(extra_body_fields)

    config = CustomAuthConfig(
        request=AuthRequest(url=login_url, method=method, body=body),
        extraction=TokenExtraction(token_path=token_path),
    )
    return CustomAuthProvider(config)


def create_login_auth(
    login_url: str,
    username: str,
    password: str,
    username_field: str = "username",
    password_field: str = "password",
    token_path: str = "token",
    method: str = "POST",
    body_format: str = "json",
    **extra_body_fields,
) -> CustomAuthProvider:
    """Create auth provider for username/password login endpoints."""
    body = {
        username_field: username,
        password_field: password,
    }
    body.update(extra_body_fields)

    config = CustomAuthConfig(
        request=AuthRequest(
            url=login_url,
            method=method,
            body=body,
            body_format=body_format,
        ),
        extraction=TokenExtraction(
            token_path=token_path,
            fallback_paths=["access_token", "data.token", "data.access_token", "result.token"],
        ),
    )
    return CustomAuthProvider(config)


def create_header_auth(
    login_url: str,
    auth_header_value: str,
    auth_header_name: str = "Authorization",
    response_token_path: str = "token",
    method: str = "POST",
    body: dict = None,
) -> CustomAuthProvider:
    """Create auth provider that sends auth in header and extracts token from response."""
    config = CustomAuthConfig(
        request=AuthRequest(
            url=login_url,
            method=method,
            headers={auth_header_name: auth_header_value},
            body=body,
        ),
        extraction=TokenExtraction(token_path=response_token_path),
    )
    return CustomAuthProvider(config)
