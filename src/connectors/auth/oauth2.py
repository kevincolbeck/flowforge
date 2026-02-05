"""
OAuth2 Authentication Provider

Supports all standard OAuth2 flows:
- Client Credentials
- Password Grant (Resource Owner)
- Authorization Code (with PKCE)
- Refresh Token
- Device Code
"""

import time
import base64
import secrets
import hashlib
from typing import Any, Optional
from dataclasses import dataclass
import httpx

from .token_store import TokenData, TokenStore, get_default_store


@dataclass
class OAuth2Config:
    """Configuration for OAuth2 authentication."""
    # Token endpoint (required)
    token_url: str

    # Client credentials
    client_id: str
    client_secret: Optional[str] = None

    # For password grant
    username: Optional[str] = None
    password: Optional[str] = None

    # For authorization code
    authorization_url: Optional[str] = None
    redirect_uri: Optional[str] = None
    code: Optional[str] = None
    code_verifier: Optional[str] = None  # PKCE

    # Scopes
    scope: Optional[str] = None

    # Additional params to include in token request
    extra_params: Optional[dict] = None

    # How to send credentials: "body" or "header" (Basic auth)
    auth_method: str = "body"

    # Custom headers for token request
    headers: Optional[dict] = None

    # Audience (for some OAuth providers like Auth0)
    audience: Optional[str] = None


class OAuth2Provider:
    """
    OAuth2 authentication provider with automatic token management.

    Supports:
    - Client Credentials Flow
    - Password Grant Flow
    - Authorization Code Flow (with PKCE support)
    - Automatic token refresh
    - Token caching
    """

    def __init__(
        self,
        config: OAuth2Config,
        store: Optional[TokenStore] = None,
        cache_key: Optional[str] = None,
    ):
        self.config = config
        self.store = store or get_default_store()
        self._cache_key = cache_key or self._generate_cache_key()

    def _generate_cache_key(self) -> str:
        """Generate a unique cache key for this OAuth config."""
        return TokenStore.generate_key(
            self.config.token_url,
            self.config.client_id,
            self.config.username,
            self.config.scope,
        )

    async def get_token(self, force_refresh: bool = False) -> TokenData:
        """
        Get a valid access token, fetching or refreshing as needed.

        Args:
            force_refresh: If True, ignores cached token and fetches new one

        Returns:
            TokenData with valid access token
        """
        if not force_refresh:
            # Try to get cached token
            cached = await self.store.get(self._cache_key)
            if cached and not cached.is_expired:
                return cached

            # Try to refresh if we have a refresh token
            if cached and cached.refresh_token:
                try:
                    return await self._refresh_token(cached.refresh_token)
                except Exception:
                    pass  # Fall through to get new token

        # Get new token based on available credentials
        if self.config.code:
            return await self._authorization_code_flow()
        elif self.config.username and self.config.password:
            return await self._password_flow()
        else:
            return await self._client_credentials_flow()

    async def _make_token_request(self, data: dict) -> TokenData:
        """Make a token request and parse the response."""
        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        # Add custom headers
        if self.config.headers:
            headers.update(self.config.headers)

        # Handle client authentication
        if self.config.auth_method == "header" and self.config.client_secret:
            credentials = f"{self.config.client_id}:{self.config.client_secret}"
            encoded = base64.b64encode(credentials.encode()).decode()
            headers["Authorization"] = f"Basic {encoded}"
        elif self.config.auth_method == "body":
            data["client_id"] = self.config.client_id
            if self.config.client_secret:
                data["client_secret"] = self.config.client_secret

        # Add extra params
        if self.config.extra_params:
            data.update(self.config.extra_params)

        # Add audience if specified
        if self.config.audience:
            data["audience"] = self.config.audience

        request_time = time.time()

        async with httpx.AsyncClient() as client:
            response = await client.post(
                self.config.token_url,
                data=data,
                headers=headers,
            )

            if response.status_code >= 400:
                error_data = response.json() if response.headers.get("content-type", "").startswith("application/json") else {}
                error_msg = error_data.get("error_description") or error_data.get("error") or response.text
                raise OAuth2Error(f"Token request failed: {error_msg}", response.status_code)

            token_response = response.json()

        token = TokenData.from_response(token_response, request_time)

        if not token.access_token:
            raise OAuth2Error("No access token in response")

        # Cache the token
        await self.store.set(self._cache_key, token)

        return token

    async def _client_credentials_flow(self) -> TokenData:
        """Execute OAuth2 Client Credentials flow."""
        data = {"grant_type": "client_credentials"}

        if self.config.scope:
            data["scope"] = self.config.scope

        return await self._make_token_request(data)

    async def _password_flow(self) -> TokenData:
        """Execute OAuth2 Password Grant flow."""
        data = {
            "grant_type": "password",
            "username": self.config.username,
            "password": self.config.password,
        }

        if self.config.scope:
            data["scope"] = self.config.scope

        return await self._make_token_request(data)

    async def _authorization_code_flow(self) -> TokenData:
        """Execute OAuth2 Authorization Code flow."""
        data = {
            "grant_type": "authorization_code",
            "code": self.config.code,
            "redirect_uri": self.config.redirect_uri,
        }

        # PKCE support
        if self.config.code_verifier:
            data["code_verifier"] = self.config.code_verifier

        return await self._make_token_request(data)

    async def _refresh_token(self, refresh_token: str) -> TokenData:
        """Refresh an access token using a refresh token."""
        data = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        }

        if self.config.scope:
            data["scope"] = self.config.scope

        return await self._make_token_request(data)

    async def revoke_token(self, token: str = None, token_type: str = "access_token") -> bool:
        """
        Revoke a token if the OAuth provider supports it.

        Args:
            token: Token to revoke (uses cached if not provided)
            token_type: "access_token" or "refresh_token"
        """
        if not token:
            cached = await self.store.get(self._cache_key)
            if not cached:
                return True
            token = cached.access_token if token_type == "access_token" else cached.refresh_token

        if not token:
            return True

        # Try common revocation endpoints
        revoke_url = self.config.token_url.replace("/token", "/revoke")

        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    revoke_url,
                    data={
                        "token": token,
                        "token_type_hint": token_type,
                        "client_id": self.config.client_id,
                        "client_secret": self.config.client_secret,
                    },
                )
                if response.status_code < 400:
                    await self.store.delete(self._cache_key)
                    return True
        except Exception:
            pass

        return False

    async def clear_cached_token(self) -> None:
        """Clear the cached token."""
        await self.store.delete(self._cache_key)

    @staticmethod
    def generate_pkce() -> tuple[str, str]:
        """
        Generate PKCE code verifier and challenge.

        Returns:
            Tuple of (code_verifier, code_challenge)
        """
        code_verifier = secrets.token_urlsafe(32)
        code_challenge = base64.urlsafe_b64encode(
            hashlib.sha256(code_verifier.encode()).digest()
        ).decode().rstrip("=")
        return code_verifier, code_challenge

    def get_authorization_url(self, state: str = None, code_challenge: str = None) -> str:
        """
        Generate the authorization URL for the authorization code flow.

        Args:
            state: Optional state parameter for CSRF protection
            code_challenge: Optional PKCE code challenge

        Returns:
            Authorization URL to redirect user to
        """
        if not self.config.authorization_url:
            raise OAuth2Error("Authorization URL not configured")

        params = {
            "response_type": "code",
            "client_id": self.config.client_id,
            "redirect_uri": self.config.redirect_uri,
        }

        if self.config.scope:
            params["scope"] = self.config.scope

        if state:
            params["state"] = state

        if code_challenge:
            params["code_challenge"] = code_challenge
            params["code_challenge_method"] = "S256"

        if self.config.audience:
            params["audience"] = self.config.audience

        query = "&".join(f"{k}={v}" for k, v in params.items())
        separator = "&" if "?" in self.config.authorization_url else "?"
        return f"{self.config.authorization_url}{separator}{query}"


class OAuth2Error(Exception):
    """OAuth2 authentication error."""

    def __init__(self, message: str, status_code: int = None):
        super().__init__(message)
        self.status_code = status_code


# Convenience functions for common OAuth2 providers

async def get_oauth2_token(
    token_url: str,
    client_id: str,
    client_secret: str = None,
    scope: str = None,
    **kwargs
) -> TokenData:
    """Quick helper to get an OAuth2 token using client credentials."""
    config = OAuth2Config(
        token_url=token_url,
        client_id=client_id,
        client_secret=client_secret,
        scope=scope,
        **kwargs
    )
    provider = OAuth2Provider(config)
    return await provider.get_token()


async def get_password_token(
    token_url: str,
    client_id: str,
    username: str,
    password: str,
    client_secret: str = None,
    scope: str = None,
    **kwargs
) -> TokenData:
    """Quick helper to get a token using password grant."""
    config = OAuth2Config(
        token_url=token_url,
        client_id=client_id,
        client_secret=client_secret,
        username=username,
        password=password,
        scope=scope,
        **kwargs
    )
    provider = OAuth2Provider(config)
    return await provider.get_token()
