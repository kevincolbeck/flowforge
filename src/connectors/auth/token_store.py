"""
Token Store

Caches and manages authentication tokens with expiration handling.
Supports multiple storage backends: memory, file, Redis.
"""

import json
import time
import hashlib
import asyncio
from typing import Any, Optional
from dataclasses import dataclass, asdict
from abc import ABC, abstractmethod
from pathlib import Path


@dataclass
class TokenData:
    """Represents a cached token with metadata."""
    access_token: str
    token_type: str = "Bearer"
    expires_at: Optional[float] = None  # Unix timestamp
    refresh_token: Optional[str] = None
    scope: Optional[str] = None
    extra: Optional[dict] = None  # Any additional fields from the response

    @property
    def is_expired(self) -> bool:
        """Check if token is expired (with 60s buffer)."""
        if self.expires_at is None:
            return False
        return time.time() >= (self.expires_at - 60)

    @property
    def expires_in(self) -> Optional[int]:
        """Seconds until expiration."""
        if self.expires_at is None:
            return None
        return max(0, int(self.expires_at - time.time()))

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "TokenData":
        return cls(**data)

    @classmethod
    def from_response(cls, response: dict, requested_at: float = None) -> "TokenData":
        """
        Parse token from various API response formats.
        Handles OAuth2 standard and common variations.
        """
        requested_at = requested_at or time.time()

        # Find access token (try common field names)
        access_token = (
            response.get("access_token") or
            response.get("accessToken") or
            response.get("token") or
            response.get("auth_token") or
            response.get("authToken") or
            response.get("jwt") or
            response.get("id_token") or
            response.get("session_token") or
            response.get("sessionToken") or
            response.get("api_token") or
            response.get("apiToken") or
            response.get("bearer_token") or
            response.get("bearerToken") or
            # Nested structures
            response.get("data", {}).get("access_token") or
            response.get("data", {}).get("token") or
            response.get("result", {}).get("access_token") or
            response.get("result", {}).get("token") or
            response.get("response", {}).get("access_token") or
            response.get("response", {}).get("token") or
            response.get("credentials", {}).get("access_token") or
            response.get("credentials", {}).get("token") or
            response.get("auth", {}).get("token") or
            ""
        )

        # Find token type
        token_type = (
            response.get("token_type") or
            response.get("tokenType") or
            response.get("type") or
            "Bearer"
        )

        # Calculate expiration
        expires_at = None
        if "expires_at" in response:
            expires_at = float(response["expires_at"])
        elif "expiresAt" in response:
            expires_at = float(response["expiresAt"])
        elif "expires_in" in response:
            expires_at = requested_at + float(response["expires_in"])
        elif "expiresIn" in response:
            expires_at = requested_at + float(response["expiresIn"])
        elif "expiry" in response:
            expires_at = float(response["expiry"])
        elif "exp" in response:
            expires_at = float(response["exp"])
        # Check nested
        elif "data" in response and isinstance(response["data"], dict):
            if "expires_in" in response["data"]:
                expires_at = requested_at + float(response["data"]["expires_in"])

        # Find refresh token
        refresh_token = (
            response.get("refresh_token") or
            response.get("refreshToken") or
            response.get("data", {}).get("refresh_token") or
            None
        )

        # Find scope
        scope = response.get("scope") or response.get("scopes")
        if isinstance(scope, list):
            scope = " ".join(scope)

        # Collect extra fields
        known_fields = {
            "access_token", "accessToken", "token", "auth_token", "authToken",
            "jwt", "id_token", "session_token", "sessionToken", "api_token",
            "token_type", "tokenType", "type", "expires_at", "expiresAt",
            "expires_in", "expiresIn", "expiry", "exp", "refresh_token",
            "refreshToken", "scope", "scopes", "data", "result", "response"
        }
        extra = {k: v for k, v in response.items() if k not in known_fields}

        return cls(
            access_token=access_token,
            token_type=token_type,
            expires_at=expires_at,
            refresh_token=refresh_token,
            scope=scope,
            extra=extra if extra else None,
        )


class TokenStore(ABC):
    """Abstract base class for token storage."""

    @abstractmethod
    async def get(self, key: str) -> Optional[TokenData]:
        """Retrieve a token by key."""
        pass

    @abstractmethod
    async def set(self, key: str, token: TokenData) -> None:
        """Store a token with the given key."""
        pass

    @abstractmethod
    async def delete(self, key: str) -> None:
        """Delete a token by key."""
        pass

    @abstractmethod
    async def clear(self) -> None:
        """Clear all stored tokens."""
        pass

    @staticmethod
    def generate_key(identifier: str, *args) -> str:
        """Generate a consistent cache key from identifier and optional args."""
        parts = [identifier] + [str(a) for a in args if a]
        combined = ":".join(parts)
        return hashlib.sha256(combined.encode()).hexdigest()[:32]


class MemoryTokenStore(TokenStore):
    """In-memory token storage (for single instance use)."""

    def __init__(self):
        self._tokens: dict[str, TokenData] = {}
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> Optional[TokenData]:
        async with self._lock:
            token = self._tokens.get(key)
            if token and token.is_expired:
                del self._tokens[key]
                return None
            return token

    async def set(self, key: str, token: TokenData) -> None:
        async with self._lock:
            self._tokens[key] = token

    async def delete(self, key: str) -> None:
        async with self._lock:
            self._tokens.pop(key, None)

    async def clear(self) -> None:
        async with self._lock:
            self._tokens.clear()

    async def cleanup_expired(self) -> int:
        """Remove all expired tokens. Returns count of removed tokens."""
        async with self._lock:
            expired = [k for k, v in self._tokens.items() if v.is_expired]
            for k in expired:
                del self._tokens[k]
            return len(expired)


class FileTokenStore(TokenStore):
    """File-based token storage (persists across restarts)."""

    def __init__(self, storage_path: str = None):
        self._path = Path(storage_path or "./.tokens.json")
        self._lock = asyncio.Lock()
        self._cache: dict[str, dict] = {}
        self._loaded = False

    async def _load(self) -> None:
        """Load tokens from file."""
        if self._loaded:
            return
        try:
            if self._path.exists():
                self._cache = json.loads(self._path.read_text())
        except Exception:
            self._cache = {}
        self._loaded = True

    async def _save(self) -> None:
        """Save tokens to file."""
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            self._path.write_text(json.dumps(self._cache, indent=2))
        except Exception:
            pass

    async def get(self, key: str) -> Optional[TokenData]:
        async with self._lock:
            await self._load()
            data = self._cache.get(key)
            if not data:
                return None
            token = TokenData.from_dict(data)
            if token.is_expired:
                del self._cache[key]
                await self._save()
                return None
            return token

    async def set(self, key: str, token: TokenData) -> None:
        async with self._lock:
            await self._load()
            self._cache[key] = token.to_dict()
            await self._save()

    async def delete(self, key: str) -> None:
        async with self._lock:
            await self._load()
            self._cache.pop(key, None)
            await self._save()

    async def clear(self) -> None:
        async with self._lock:
            self._cache = {}
            await self._save()


class RedisTokenStore(TokenStore):
    """Redis-based token storage (for distributed systems)."""

    def __init__(self, redis_url: str = "redis://localhost:6379", prefix: str = "token:"):
        self._url = redis_url
        self._prefix = prefix
        self._client = None

    async def _get_client(self):
        """Lazy initialize Redis client."""
        if self._client is None:
            import redis.asyncio as redis
            self._client = redis.from_url(self._url)
        return self._client

    async def get(self, key: str) -> Optional[TokenData]:
        client = await self._get_client()
        data = await client.get(f"{self._prefix}{key}")
        if not data:
            return None
        token = TokenData.from_dict(json.loads(data))
        if token.is_expired:
            await self.delete(key)
            return None
        return token

    async def set(self, key: str, token: TokenData) -> None:
        client = await self._get_client()
        ttl = token.expires_in if token.expires_at else None
        data = json.dumps(token.to_dict())
        if ttl:
            await client.setex(f"{self._prefix}{key}", ttl + 60, data)
        else:
            await client.set(f"{self._prefix}{key}", data)

    async def delete(self, key: str) -> None:
        client = await self._get_client()
        await client.delete(f"{self._prefix}{key}")

    async def clear(self) -> None:
        client = await self._get_client()
        keys = await client.keys(f"{self._prefix}*")
        if keys:
            await client.delete(*keys)


# Global default store
_default_store: Optional[TokenStore] = None


def get_default_store() -> TokenStore:
    """Get the default token store (memory-based)."""
    global _default_store
    if _default_store is None:
        _default_store = MemoryTokenStore()
    return _default_store


def set_default_store(store: TokenStore) -> None:
    """Set the default token store."""
    global _default_store
    _default_store = store
