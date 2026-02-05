"""
Authentication Module

Provides flexible authentication for any API:
- OAuth2 (client credentials, password grant, authorization code, PKCE)
- Custom token-based authentication
- Automatic token caching and refresh
"""

from .token_store import (
    TokenData,
    TokenStore,
    MemoryTokenStore,
    FileTokenStore,
    RedisTokenStore,
    get_default_store,
    set_default_store,
)

from .oauth2 import (
    OAuth2Config,
    OAuth2Provider,
    OAuth2Error,
    get_oauth2_token,
    get_password_token,
)

from .custom_auth import (
    AuthRequest,
    TokenExtraction,
    TokenInjection,
    RefreshConfig,
    CustomAuthConfig,
    CustomAuthProvider,
    AuthError,
    create_api_key_auth,
    create_login_auth,
    create_header_auth,
)

__all__ = [
    # Token store
    "TokenData",
    "TokenStore",
    "MemoryTokenStore",
    "FileTokenStore",
    "RedisTokenStore",
    "get_default_store",
    "set_default_store",

    # OAuth2
    "OAuth2Config",
    "OAuth2Provider",
    "OAuth2Error",
    "get_oauth2_token",
    "get_password_token",

    # Custom auth
    "AuthRequest",
    "TokenExtraction",
    "TokenInjection",
    "RefreshConfig",
    "CustomAuthConfig",
    "CustomAuthProvider",
    "AuthError",
    "create_api_key_auth",
    "create_login_auth",
    "create_header_auth",
]
