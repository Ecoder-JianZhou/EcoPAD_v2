from __future__ import annotations

import httpx
from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, Field

from app.api.auth import require_superuser
from app.core.db import get_db
from app.core.security import now_iso
from app.core.settings import RUNNER_SERVICE_URL

router = APIRouter()


class SavePermissionIn(BaseModel):
    site_id: str = Field(..., description="Site ID, e.g. SPRUCE")
    can_auto_forecast: bool = Field(default=False)


async def _runner_sites() -> list[str]:
    """
    Read site list from Runner.
    Supports:
    - {"sites": ["SPRUCE", ...]}
    - {"sites": [{"id": "SPRUCE", ...}, ...]}
    """
    if not RUNNER_SERVICE_URL:
        return []

    try:
        async with httpx.AsyncClient(timeout=8) as client:
            response = await client.get(f"{RUNNER_SERVICE_URL}/api/sites")
            if response.status_code != 200:
                return []

            payload = response.json() or {}
            raw_sites = payload.get("sites", []) if isinstance(payload, dict) else []

            out: list[str] = []
            for item in raw_sites:
                if isinstance(item, str):
                    out.append(item)
                elif isinstance(item, dict):
                    site_id = item.get("site_id") or item.get("id")
                    if site_id:
                        out.append(site_id)
            return out
    except Exception:
        return []


@router.get("/users")
async def admin_users(authorization: str | None = Header(default=None)):
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
                    "id": row["id"],
                    "username": row["username"],
                    "role": row["role"],
                    "created_at": row["created_at"],
                }
                for row in rows
            ]
        }
    finally:
        await db.close()


@router.get("/sites")
async def admin_sites(authorization: str | None = Header(default=None)):
    await require_superuser(authorization)
    return {"sites": await _runner_sites()}


@router.get("/users/{user_id}/permissions")
async def admin_user_permissions(user_id: int, authorization: str | None = Header(default=None)):
    await require_superuser(authorization)

    db = await get_db()
    try:
        cur_user = await db.execute(
            """
            SELECT id, username, role
            FROM users
            WHERE id=?
            """,
            (user_id,),
        )
        user_row = await cur_user.fetchone()
        if not user_row:
            raise HTTPException(status_code=404, detail="User not found")

        cur = await db.execute(
            """
            SELECT site_id, can_auto_forecast
            FROM user_site_permissions
            WHERE user_id=?
            ORDER BY site_id
            """,
            (user_id,),
        )
        rows = await cur.fetchall()

        permissions = {
            row["site_id"]: {
                "can_auto_forecast": bool(row["can_auto_forecast"]),
            }
            for row in rows
        }

        return {
            "user_id": user_row["id"],
            "username": user_row["username"],
            "role": user_row["role"],
            "site_permissions": permissions,
        }
    finally:
        await db.close()


@router.post("/users/{user_id}/permissions")
async def admin_save_permission(
    user_id: int,
    payload: SavePermissionIn,
    authorization: str | None = Header(default=None),
):
    """
    Upsert one user's auto_forecast permission for one site.
    """
    await require_superuser(authorization)

    site_id = (payload.site_id or "").strip()
    if not site_id:
        raise HTTPException(status_code=400, detail="site_id is required")

    can_auto_forecast = bool(payload.can_auto_forecast)

    db = await get_db()
    try:
        cur_user = await db.execute(
            """
            SELECT id
            FROM users
            WHERE id=?
            """,
            (user_id,),
        )
        user_row = await cur_user.fetchone()
        if not user_row:
            raise HTTPException(status_code=404, detail="User not found")

        cur_existing = await db.execute(
            """
            SELECT id
            FROM user_site_permissions
            WHERE user_id=? AND site_id=?
            """,
            (user_id, site_id),
        )
        existing = await cur_existing.fetchone()

        if existing:
            await db.execute(
                """
                UPDATE user_site_permissions
                SET can_auto_forecast=?,
                    updated_at=?
                WHERE user_id=? AND site_id=?
                """,
                (
                    1 if can_auto_forecast else 0,
                    now_iso(),
                    user_id,
                    site_id,
                ),
            )
        else:
            await db.execute(
                """
                INSERT INTO user_site_permissions(
                    user_id, site_id, can_access, can_auto_forecast, created_at, updated_at
                )
                VALUES (?, ?, 1, ?, ?, ?)
                """,
                (
                    user_id,
                    site_id,
                    1 if can_auto_forecast else 0,
                    now_iso(),
                    now_iso(),
                ),
            )

        await db.commit()

        return {
            "ok": True,
            "user_id": user_id,
            "site_id": site_id,
            "can_auto_forecast": can_auto_forecast,
        }
    finally:
        await db.close()