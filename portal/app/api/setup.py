from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.core.db import get_db
from app.core.security import now_iso, hash_password

router = APIRouter()


class SetupInitIn(BaseModel):
    username: str = Field(..., min_length=3, max_length=64)
    password: str = Field(..., min_length=6, max_length=256)


async def _is_initialized() -> bool:
    db = await get_db()
    try:
        cur = await db.execute(
            """
            SELECT value
            FROM system_state
            WHERE key='initialized'
            """
        )
        row = await cur.fetchone()
        return bool(row and row["value"] == "true")
    finally:
        await db.close()


@router.get("/status")
async def setup_status():
    """
    Return whether the portal has already been initialized.
    """
    initialized = await _is_initialized()
    return {"initialized": initialized}


@router.post("/init")
async def setup_init(payload: SetupInitIn):
    """
    One-time initialization:
    - create first superuser
    - mark system initialized=true

    If already initialized, reject.
    """
    db = await get_db()
    try:
        cur = await db.execute(
            """
            SELECT value
            FROM system_state
            WHERE key='initialized'
            """
        )
        row = await cur.fetchone()
        if row and row["value"] == "true":
            raise HTTPException(status_code=400, detail="System already initialized")

        cur_user = await db.execute(
            """
            SELECT id
            FROM users
            WHERE username=?
            """,
            (payload.username,),
        )
        existing = await cur_user.fetchone()
        if existing:
            raise HTTPException(status_code=400, detail="Username already exists")

        await db.execute(
            """
            INSERT INTO users(username, password_hash, role, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (
                payload.username,
                hash_password(payload.password),
                "superuser",
                now_iso(),
            ),
        )

        await db.execute(
            """
            INSERT INTO system_state(key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value=excluded.value,
                updated_at=excluded.updated_at
            """,
            ("initialized", "true", now_iso()),
        )

        await db.commit()

        return {
            "ok": True,
            "initialized": True,
            "superuser_username": payload.username,
        }
    finally:
        await db.close()