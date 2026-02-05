"""
Authentication

Simple JWT-based authentication.
"""

import os
from datetime import datetime, timedelta
from typing import Optional

import bcrypt
import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from ..db import get_db, User

# Configuration
SECRET_KEY = os.environ.get("JWT_SECRET_KEY", "change-me-in-production-use-a-secure-key")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_HOURS = 24 * 7  # 1 week

security = HTTPBearer(auto_error=False)


class AuthManager:
    """Handles user authentication."""

    def __init__(self):
        self.db = get_db()

    def hash_password(self, password: str) -> str:
        """Hash a password."""
        return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

    def verify_password(self, password: str, hashed: str) -> bool:
        """Verify a password against its hash."""
        return bcrypt.checkpw(password.encode(), hashed.encode())

    def register(self, email: str, password: str, name: str) -> User:
        """Register a new user."""
        # Check if email exists
        existing = self.db.get_user_by_email(email)
        if existing:
            raise ValueError("Email already registered")

        # Validate
        if len(password) < 8:
            raise ValueError("Password must be at least 8 characters")
        if "@" not in email:
            raise ValueError("Invalid email address")

        # Create user
        password_hash = self.hash_password(password)
        return self.db.create_user(email, name, password_hash)

    def login(self, email: str, password: str) -> tuple[User, str]:
        """Login and return user with access token."""
        user = self.db.get_user_by_email(email)
        if not user:
            raise ValueError("Invalid email or password")

        if not self.verify_password(password, user.password_hash):
            raise ValueError("Invalid email or password")

        if not user.is_active:
            raise ValueError("Account is disabled")

        token = create_access_token(user.id)
        return user, token

    def get_user(self, user_id: str) -> User | None:
        """Get user by ID."""
        return self.db.get_user_by_id(user_id)


def create_access_token(user_id: str, expires_delta: timedelta | None = None) -> str:
    """Create a JWT access token."""
    if expires_delta is None:
        expires_delta = timedelta(hours=ACCESS_TOKEN_EXPIRE_HOURS)

    expire = datetime.utcnow() + expires_delta
    payload = {
        "sub": user_id,
        "exp": expire,
        "iat": datetime.utcnow(),
    }
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)


def decode_token(token: str) -> dict | None:
    """Decode and validate a JWT token."""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except jwt.ExpiredSignatureError:
        return None
    except jwt.InvalidTokenError:
        return None


async def get_current_user(
    credentials: HTTPAuthorizationCredentials | None = Depends(security)
) -> User | None:
    """
    Get the current authenticated user.

    Returns None if not authenticated (allows public endpoints).
    Use get_required_user for endpoints that require auth.
    """
    if not credentials:
        return None

    payload = decode_token(credentials.credentials)
    if not payload:
        return None

    user_id = payload.get("sub")
    if not user_id:
        return None

    db = get_db()
    return db.get_user_by_id(user_id)


async def get_required_user(
    credentials: HTTPAuthorizationCredentials | None = Depends(security)
) -> User:
    """Get current user, raising 401 if not authenticated."""
    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
            headers={"WWW-Authenticate": "Bearer"},
        )

    payload = decode_token(credentials.credentials)
    if not payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    user_id = payload.get("sub")
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    db = get_db()
    user = db.get_user_by_id(user_id)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account disabled",
        )

    return user
