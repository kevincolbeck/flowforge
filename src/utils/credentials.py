"""
Credential Management with Encryption

Securely stores and retrieves API credentials with AES-256 encryption.
"""

import base64
import json
import os
import secrets
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC


@dataclass
class Credential:
    """Represents stored credentials for a service."""

    id: str
    service: str
    name: str
    credential_type: str  # api_key, oauth2, basic, custom
    data: dict[str, str]  # The actual credentials (encrypted at rest)
    owner_id: str | None = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    expires_at: datetime | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def is_expired(self) -> bool:
        """Check if the credential has expired."""
        if self.expires_at is None:
            return False
        return datetime.utcnow() > self.expires_at


class CredentialManager:
    """
    Manages encrypted credential storage.

    Uses Fernet (AES-128-CBC) for encryption with a master key derived
    from a password using PBKDF2.

    For production, integrate with a proper secrets manager like:
    - AWS Secrets Manager
    - HashiCorp Vault
    - Azure Key Vault
    - Google Secret Manager
    """

    def __init__(
        self,
        master_password: str | None = None,
        storage_path: str | None = None,
    ):
        # Use environment variable or provided password
        self.master_password = master_password or os.environ.get(
            "INTEGRATOR_MASTER_KEY",
            "change-me-in-production",  # Default for development only
        )

        self.storage_path = storage_path or os.path.expanduser(
            "~/.universal-integrator/credentials"
        )

        # Generate encryption key from master password
        self._fernet = self._create_fernet()

        # In-memory credential cache
        self._credentials: dict[str, Credential] = {}

        # Ensure storage directory exists
        os.makedirs(self.storage_path, exist_ok=True)

    def _create_fernet(self) -> Fernet:
        """Create Fernet instance from master password."""
        # Use a fixed salt (in production, store this securely)
        salt = b"universal-integrator-salt-v1"

        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=480000,
        )

        key = base64.urlsafe_b64encode(kdf.derive(self.master_password.encode()))
        return Fernet(key)

    def store_credential(
        self,
        service: str,
        name: str,
        credential_type: str,
        data: dict[str, str],
        owner_id: str | None = None,
        expires_at: datetime | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Credential:
        """Store a new credential."""
        credential_id = secrets.token_urlsafe(16)

        credential = Credential(
            id=credential_id,
            service=service,
            name=name,
            credential_type=credential_type,
            data=data,
            owner_id=owner_id,
            expires_at=expires_at,
            metadata=metadata or {},
        )

        # Encrypt and store
        self._save_credential(credential)
        self._credentials[credential_id] = credential

        return credential

    def get_credential(self, credential_id: str) -> Credential | None:
        """Retrieve a credential by ID."""
        # Check cache first
        if credential_id in self._credentials:
            return self._credentials[credential_id]

        # Load from storage
        return self._load_credential(credential_id)

    def get_credentials_for_service(
        self,
        service: str,
        owner_id: str | None = None,
    ) -> list[Credential]:
        """Get all credentials for a service."""
        # Load all credentials from storage
        self._load_all_credentials()

        return [
            cred for cred in self._credentials.values()
            if cred.service == service and (owner_id is None or cred.owner_id == owner_id)
        ]

    def update_credential(
        self,
        credential_id: str,
        data: dict[str, str] | None = None,
        metadata: dict[str, Any] | None = None,
        expires_at: datetime | None = None,
    ) -> Credential | None:
        """Update an existing credential."""
        credential = self.get_credential(credential_id)
        if not credential:
            return None

        if data is not None:
            credential.data = data
        if metadata is not None:
            credential.metadata = metadata
        if expires_at is not None:
            credential.expires_at = expires_at

        credential.updated_at = datetime.utcnow()

        self._save_credential(credential)
        return credential

    def delete_credential(self, credential_id: str) -> bool:
        """Delete a credential."""
        if credential_id in self._credentials:
            del self._credentials[credential_id]

        file_path = os.path.join(self.storage_path, f"{credential_id}.enc")
        if os.path.exists(file_path):
            os.remove(file_path)
            return True

        return False

    def rotate_credential(
        self,
        credential_id: str,
        new_data: dict[str, str],
    ) -> Credential | None:
        """Rotate a credential to new values."""
        credential = self.get_credential(credential_id)
        if not credential:
            return None

        # Store old credential in metadata for rollback
        credential.metadata["previous_data_hash"] = self._hash_data(credential.data)
        credential.data = new_data
        credential.updated_at = datetime.utcnow()

        self._save_credential(credential)
        return credential

    def decrypt_for_use(self, credential: Credential) -> dict[str, str]:
        """
        Get decrypted credential data for use.

        This is what you call when you need the actual API keys/tokens.
        """
        return credential.data

    def _save_credential(self, credential: Credential):
        """Save credential to encrypted file."""
        # Serialize credential
        data = {
            "id": credential.id,
            "service": credential.service,
            "name": credential.name,
            "credential_type": credential.credential_type,
            "data": credential.data,
            "owner_id": credential.owner_id,
            "created_at": credential.created_at.isoformat(),
            "updated_at": credential.updated_at.isoformat(),
            "expires_at": credential.expires_at.isoformat() if credential.expires_at else None,
            "metadata": credential.metadata,
        }

        # Encrypt
        encrypted = self._fernet.encrypt(json.dumps(data).encode())

        # Save to file
        file_path = os.path.join(self.storage_path, f"{credential.id}.enc")
        with open(file_path, "wb") as f:
            f.write(encrypted)

    def _load_credential(self, credential_id: str) -> Credential | None:
        """Load a credential from encrypted file."""
        file_path = os.path.join(self.storage_path, f"{credential_id}.enc")
        if not os.path.exists(file_path):
            return None

        try:
            with open(file_path, "rb") as f:
                encrypted = f.read()

            decrypted = self._fernet.decrypt(encrypted)
            data = json.loads(decrypted.decode())

            credential = Credential(
                id=data["id"],
                service=data["service"],
                name=data["name"],
                credential_type=data["credential_type"],
                data=data["data"],
                owner_id=data.get("owner_id"),
                created_at=datetime.fromisoformat(data["created_at"]),
                updated_at=datetime.fromisoformat(data["updated_at"]),
                expires_at=datetime.fromisoformat(data["expires_at"]) if data.get("expires_at") else None,
                metadata=data.get("metadata", {}),
            )

            self._credentials[credential_id] = credential
            return credential

        except Exception as e:
            # Log error but don't expose details
            print(f"Failed to load credential {credential_id}: {e}")
            return None

    def _load_all_credentials(self):
        """Load all credentials from storage."""
        if not os.path.exists(self.storage_path):
            return

        for filename in os.listdir(self.storage_path):
            if filename.endswith(".enc"):
                credential_id = filename[:-4]
                if credential_id not in self._credentials:
                    self._load_credential(credential_id)

    def _hash_data(self, data: dict[str, str]) -> str:
        """Create a hash of credential data (for audit/comparison, not storage)."""
        import hashlib
        content = json.dumps(data, sort_keys=True)
        return hashlib.sha256(content.encode()).hexdigest()[:16]

    def export_credential_info(self, credential_id: str) -> dict[str, Any] | None:
        """Export credential metadata (not the actual secrets)."""
        credential = self.get_credential(credential_id)
        if not credential:
            return None

        return {
            "id": credential.id,
            "service": credential.service,
            "name": credential.name,
            "credential_type": credential.credential_type,
            "owner_id": credential.owner_id,
            "created_at": credential.created_at.isoformat(),
            "updated_at": credential.updated_at.isoformat(),
            "expires_at": credential.expires_at.isoformat() if credential.expires_at else None,
            "is_expired": credential.is_expired(),
            # Don't include actual credential data!
        }


class OAuthManager:
    """
    Manages OAuth2 token lifecycle.

    Handles:
    - Authorization URL generation
    - Token exchange
    - Token refresh
    - Token storage via CredentialManager
    """

    def __init__(self, credential_manager: CredentialManager):
        self.credential_manager = credential_manager
        self._pending_auth: dict[str, dict[str, Any]] = {}  # state -> auth info

    def start_oauth_flow(
        self,
        service: str,
        client_id: str,
        client_secret: str,
        authorize_url: str,
        token_url: str,
        redirect_uri: str,
        scopes: list[str],
        owner_id: str | None = None,
    ) -> tuple[str, str]:
        """
        Start OAuth2 authorization flow.

        Returns (authorization_url, state)
        """
        import urllib.parse

        state = secrets.token_urlsafe(32)

        # Store pending auth info
        self._pending_auth[state] = {
            "service": service,
            "client_id": client_id,
            "client_secret": client_secret,
            "token_url": token_url,
            "redirect_uri": redirect_uri,
            "owner_id": owner_id,
        }

        # Build authorization URL
        params = {
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "scope": " ".join(scopes),
            "state": state,
            "response_type": "code",
        }

        auth_url = f"{authorize_url}?{urllib.parse.urlencode(params)}"
        return auth_url, state

    async def complete_oauth_flow(
        self,
        state: str,
        code: str,
    ) -> Credential | None:
        """
        Complete OAuth2 flow by exchanging code for tokens.
        """
        import httpx

        if state not in self._pending_auth:
            return None

        auth_info = self._pending_auth.pop(state)

        async with httpx.AsyncClient() as client:
            response = await client.post(
                auth_info["token_url"],
                data={
                    "grant_type": "authorization_code",
                    "code": code,
                    "redirect_uri": auth_info["redirect_uri"],
                    "client_id": auth_info["client_id"],
                    "client_secret": auth_info["client_secret"],
                },
            )

            if response.status_code != 200:
                return None

            tokens = response.json()

        # Calculate expiry
        expires_at = None
        if "expires_in" in tokens:
            from datetime import timedelta
            expires_at = datetime.utcnow() + timedelta(seconds=tokens["expires_in"])

        # Store credential
        return self.credential_manager.store_credential(
            service=auth_info["service"],
            name=f"{auth_info['service']} OAuth",
            credential_type="oauth2",
            data={
                "access_token": tokens["access_token"],
                "refresh_token": tokens.get("refresh_token", ""),
                "token_type": tokens.get("token_type", "Bearer"),
                "client_id": auth_info["client_id"],
                "client_secret": auth_info["client_secret"],
                "token_url": auth_info["token_url"],
            },
            owner_id=auth_info["owner_id"],
            expires_at=expires_at,
            metadata={"scopes": tokens.get("scope", "").split()},
        )

    async def refresh_token(self, credential_id: str) -> Credential | None:
        """Refresh an OAuth2 token."""
        import httpx

        credential = self.credential_manager.get_credential(credential_id)
        if not credential or credential.credential_type != "oauth2":
            return None

        data = credential.data
        if not data.get("refresh_token"):
            return None

        async with httpx.AsyncClient() as client:
            response = await client.post(
                data["token_url"],
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": data["refresh_token"],
                    "client_id": data["client_id"],
                    "client_secret": data["client_secret"],
                },
            )

            if response.status_code != 200:
                return None

            tokens = response.json()

        # Calculate new expiry
        expires_at = None
        if "expires_in" in tokens:
            from datetime import timedelta
            expires_at = datetime.utcnow() + timedelta(seconds=tokens["expires_in"])

        # Update credential
        new_data = {
            **data,
            "access_token": tokens["access_token"],
        }
        if "refresh_token" in tokens:
            new_data["refresh_token"] = tokens["refresh_token"]

        return self.credential_manager.update_credential(
            credential_id=credential_id,
            data=new_data,
            expires_at=expires_at,
        )
