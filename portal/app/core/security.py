"""
Portal authentication utilities.

This module provides:
- password hashing
- password verification
- session token generation
- UTC timestamp helper
"""

import base64
import hashlib
import hmac
import secrets
from datetime import datetime, timezone


def now_iso() -> str:
    """
    Return current UTC time in ISO 8601 format.
    """
    return datetime.now(timezone.utc).isoformat()


def hash_password(password: str) -> str:
    """
    Hash a password with PBKDF2-HMAC-SHA256 and a per-user random salt.

    Returns:
        A string in the format "salt$hash", both base64-encoded.

    Raises:
        ValueError: if password is invalid
    """
    if not isinstance(password, str) or len(password) < 6:
        raise ValueError("Password must be at least 6 characters.")

    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        200_000,
    )

    return (
        f"{base64.b64encode(salt).decode()}"
        f"${base64.b64encode(digest).decode()}"
    )


def verify_password(password: str, stored: str) -> bool:
    """
    Verify a plaintext password against a stored "salt$hash" value.

    Returns:
        True if password matches, False otherwise.
    """
    try:
        salt_b64, digest_b64 = stored.split("$", 1)
        salt = base64.b64decode(salt_b64.encode())
        digest_expected = base64.b64decode(digest_b64.encode())

        digest_actual = hashlib.pbkdf2_hmac(
            "sha256",
            password.encode("utf-8"),
            salt,
            200_000,
        )

        return hmac.compare_digest(digest_actual, digest_expected)
    except Exception:
        return False


def new_token() -> str:
    """
    Generate a random session token for Portal login sessions.
    """
    return secrets.token_urlsafe(32)