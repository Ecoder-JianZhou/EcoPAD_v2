from fastapi import APIRouter, HTTPException, Header
from pydantic import BaseModel

from app.core.db import get_db
from app.core.security import hash_password, verify_password, new_token, now_iso

router = APIRouter()

# ---------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------
# Keep permission strings centralized to avoid magic strings everywhere.
PERMISSION_SUBMIT_AUTO_FORECAST = "submit_auto_forecast"


# ---------------------------------------------------------------------
# Request schemas
# ---------------------------------------------------------------------
# These Pydantic models define the expected JSON payloads for each API.
class RegisterIn(BaseModel):
    username: str
    password: str


class LoginIn(BaseModel):
    username: str
    password: str


class PermissionGrantIn(BaseModel):
    user_id: int
    site_id: str
    permission: str  # Example: "submit_auto_forecast"


# ---------------------------------------------------------------------
# Small validation / parsing helpers
# ---------------------------------------------------------------------
def _clean_username(username: str) -> str:
    """
    Normalize and validate the username.

    Rules:
    - strip leading/trailing spaces
    - require at least 3 characters

    Raise:
        ValueError: if the username is invalid
    """
    username = (username or "").strip()
    if len(username) < 3:
        raise ValueError("Username must be at least 3 characters.")
    return username


def _parse_bearer_token(authorization: str | None) -> str | None:
    """
    Extract token from 'Authorization: Bearer <token>' header.

    Returns:
        token string if valid Bearer header is present
        None otherwise
    """
    if not authorization:
        return None

    parts = authorization.split(" ", 1)
    if len(parts) == 2 and parts[0].lower() == "bearer":
        return parts[1].strip()

    return None


# ---------------------------------------------------------------------
# Database-backed auth helpers
# ---------------------------------------------------------------------
async def get_current_user_from_token(token: str | None) -> dict | None:
    """
    Resolve a session token into a user record.

    Returns:
        {
            "id": ...,
            "username": ...,
            "role": ...
        }
        or None if token is missing / invalid
    """
    if not token:
        return None

    db = await get_db()
    try:
        cur = await db.execute(
            """
            SELECT users.id, users.username, users.role
            FROM sessions
            JOIN users ON users.id = sessions.user_id
            WHERE sessions.token = ?
            """,
            (token,),
        )
        row = await cur.fetchone()
        if not row:
            return None

        user_id, username, role = row
        return {
            "id": user_id,
            "username": username,
            "role": role,
        }
    finally:
        await db.close()


async def require_user(authorization: str | None) -> dict:
    """
    Require a valid logged-in user.

    Raise:
        HTTPException(401): if no valid session exists
    """
    token = _parse_bearer_token(authorization)
    user = await get_current_user_from_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="Not logged in or invalid session")
    return user


async def require_superuser(authorization: str | None) -> dict:
    """
    Require a logged-in user with role='superuser'.

    Raise:
        HTTPException(403): if user is not superuser
    """
    user = await require_user(authorization)
    if user["role"] != "superuser":
        raise HTTPException(status_code=403, detail="Superuser required")
    return user


async def has_site_permission(user_id: int, site_id: str, permission: str) -> bool:
    """
    Check whether a user has one specific permission for one site.
    """
    db = await get_db()
    try:
        cur = await db.execute(
            """
            SELECT 1
            FROM user_site_permissions
            WHERE user_id = ? AND site_id = ? AND permission = ?
            LIMIT 1
            """,
            (user_id, site_id, permission),
        )
        row = await cur.fetchone()
        return row is not None
    finally:
        await db.close()


async def list_site_permissions_for_user(user_id: int) -> dict[str, dict[str, bool]]:
    """
    Return permissions in a frontend-friendly structure.

    Example output:
    {
        "SPRUCE": {
            "submit_auto_forecast": True
        },
        "OTHER_SITE": {
            "submit_auto_forecast": True
        }
    }
    """
    db = await get_db()
    try:
        cur = await db.execute(
            """
            SELECT site_id, permission
            FROM user_site_permissions
            WHERE user_id = ?
            ORDER BY site_id, permission
            """,
            (user_id,),
        )
        rows = await cur.fetchall()
    finally:
        await db.close()

    permissions_by_site: dict[str, dict[str, bool]] = {}
    for site_id, permission in rows:
        if site_id not in permissions_by_site:
            permissions_by_site[site_id] = {}
        permissions_by_site[site_id][permission] = True

    return permissions_by_site


async def _get_user_basic_by_id(user_id: int) -> dict:
    """
    Load basic user info by user_id.

    Returns:
        {
            "id": ...,
            "username": ...,
            "role": ...
        }

    Raise:
        HTTPException(404): if user does not exist
    """
    db = await get_db()
    try:
        cur = await db.execute(
            "SELECT id, username, role FROM users WHERE id = ?",
            (user_id,),
        )
        row = await cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="User not found")

        uid, username, role = row
        return {
            "id": uid,
            "username": username,
            "role": role,
        }
    finally:
        await db.close()


# ---------------------------------------------------------------------
# Auth APIs
# ---------------------------------------------------------------------
@router.post("/register")
async def register(payload: RegisterIn):
    """
    Create a normal user account.

    Notes:
    - Every newly registered user gets role='user'
    - Username uniqueness is enforced by the database
    """
    try:
        username = _clean_username(payload.username)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    password_hash = hash_password(payload.password)

    db = await get_db()
    try:
        await db.execute(
            """
            INSERT INTO users(username, password_hash, role, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (username, password_hash, "user", now_iso()),
        )
        await db.commit()
        return {"ok": True}
    except Exception:
        await db.rollback()
        raise HTTPException(
            status_code=400,
            detail="Username already exists or invalid input.",
        )
    finally:
        await db.close()


@router.post("/login")
async def login(payload: LoginIn):
    """
    Login with username and password.

    On success:
    - create a session token
    - store it in the sessions table
    - return token + user info
    """
    try:
        username = _clean_username(payload.username)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    db = await get_db()
    try:
        cur = await db.execute(
            """
            SELECT id, password_hash, role
            FROM users
            WHERE username = ?
            """,
            (username,),
        )
        row = await cur.fetchone()

        if not row:
            raise HTTPException(status_code=401, detail="Invalid username or password.")

        user_id, password_hash, role = row

        if not verify_password(payload.password, password_hash):
            raise HTTPException(status_code=401, detail="Invalid username or password.")

        token = new_token()
        await db.execute(
            """
            INSERT INTO sessions(token, user_id, created_at)
            VALUES (?, ?, ?)
            """,
            (token, user_id, now_iso()),
        )
        await db.commit()

        return {
            "token": token,
            "user": {
                "id": user_id,
                "username": username,
                "role": role,
            },
        }
    finally:
        await db.close()


@router.post("/logout")
async def logout(authorization: str | None = Header(default=None)):
    """
    Delete the current session token.

    This endpoint is intentionally idempotent:
    - if no token is provided, return ok
    - if token does not exist, delete does nothing and still return ok
    """
    token = _parse_bearer_token(authorization)
    if not token:
        return {"ok": True}

    db = await get_db()
    try:
        await db.execute("DELETE FROM sessions WHERE token = ?", (token,))
        await db.commit()
        return {"ok": True}
    finally:
        await db.close()


@router.get("/me")
async def me(authorization: str | None = Header(default=None)):
    """
    Return current logged-in user info from the session token.

    This is useful for frontend refresh / page reload / auth bootstrap.
    """
    token = _parse_bearer_token(authorization)
    user = await get_current_user_from_token(token)
    return {"user": user}


# ---------------------------------------------------------------------
# Superuser-only user management APIs
# ---------------------------------------------------------------------
@router.get("/users")
async def list_users(authorization: str | None = Header(default=None)):
    """
    List all users.

    Access:
        superuser only
    """
    await require_superuser(authorization)

    db = await get_db()
    try:
        cur = await db.execute(
            """
            SELECT id, username, role, created_at
            FROM users
            ORDER BY id
            """
        )
        rows = await cur.fetchall()

        return {
            "users": [
                {
                    "id": row[0],
                    "username": row[1],
                    "role": row[2],
                    "created_at": row[3],
                }
                for row in rows
            ]
        }
    finally:
        await db.close()


@router.get("/users/{user_id}/permissions")
async def get_user_permissions(
    user_id: int,
    authorization: str | None = Header(default=None),
):
    """
    Get one user's site-level permissions.

    Access:
        superuser only
    """
    await require_superuser(authorization)

    user = await _get_user_basic_by_id(user_id)
    site_permissions = await list_site_permissions_for_user(user["id"])

    return {
        "user_id": user["id"],
        "username": user["username"],
        "role": user["role"],
        "site_permissions": site_permissions,
    }


@router.post("/permissions/grant")
async def grant_permission(
    payload: PermissionGrantIn,
    authorization: str | None = Header(default=None),
):
    """
    Grant a site-specific permission to a user.

    Access:
        superuser only
    """
    await require_superuser(authorization)
    await _get_user_basic_by_id(payload.user_id)  # validate target user exists

    db = await get_db()
    try:
        await db.execute(
            """
            INSERT OR IGNORE INTO user_site_permissions(user_id, site_id, permission, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (payload.user_id, payload.site_id, payload.permission, now_iso()),
        )
        await db.commit()

        return {
            "ok": True,
            "user_id": payload.user_id,
            "site_id": payload.site_id,
            "permission": payload.permission,
        }
    finally:
        await db.close()


@router.post("/permissions/revoke")
async def revoke_permission(
    payload: PermissionGrantIn,
    authorization: str | None = Header(default=None),
):
    """
    Revoke a site-specific permission from a user.

    Access:
        superuser only
    """
    await require_superuser(authorization)
    await _get_user_basic_by_id(payload.user_id)  # validate target user exists

    db = await get_db()
    try:
        await db.execute(
            """
            DELETE FROM user_site_permissions
            WHERE user_id = ? AND site_id = ? AND permission = ?
            """,
            (payload.user_id, payload.site_id, payload.permission),
        )
        await db.commit()

        return {
            "ok": True,
            "user_id": payload.user_id,
            "site_id": payload.site_id,
            "permission": payload.permission,
        }
    finally:
        await db.close()