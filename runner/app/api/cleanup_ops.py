"""
Runner cleanup operations API.

Responsibilities:
- inspect cleanup candidates
- run cleanup in dry-run mode
- run actual cleanup
- inspect cleanup logs

This API is for cleanup operations and debugging.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query
from pydantic import BaseModel, Field

from app.core.db import get_db
from app.services.cleanup import (
    list_cleanup_candidates,
    run_cleanup,
)


router = APIRouter(prefix="/api/cleanup", tags=["cleanup"])


# ---------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------
class CleanupRunRequest(BaseModel):
    ttl_days_ephemeral: int = Field(default=7, ge=1)
    ttl_days_normal: int = Field(default=90, ge=1)
    site_id: str | None = None
    limit: int = Field(default=500, ge=1, le=5000)


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def _normalize_site_id(value: str | None) -> str | None:
    """
    Normalize optional site_id.
    """
    if value is None:
        return None
    v = str(value).strip()
    return v or None


# ---------------------------------------------------------------------
# Candidates
# ---------------------------------------------------------------------
@router.get("/candidates")
async def cleanup_candidates(
    ttl_days_ephemeral: int = Query(7, ge=1, description="TTL for ephemeral runs in days"),
    ttl_days_normal: int = Query(90, ge=1, description="TTL for normal runs in days"),
    site_id: str = Query("", description="Optional site ID"),
    limit: int = Query(500, ge=1, le=5000, description="Max number of candidates"),
):
    """
    Return cleanup candidates without deleting anything.
    """
    rows = await list_cleanup_candidates(
        ttl_days_ephemeral=ttl_days_ephemeral,
        ttl_days_normal=ttl_days_normal,
        site_id=_normalize_site_id(site_id),
        limit=limit,
    )
    return {
        "candidates": rows,
        "count": len(rows),
        "ttl_days_ephemeral": ttl_days_ephemeral,
        "ttl_days_normal": ttl_days_normal,
        "site_id": _normalize_site_id(site_id),
        "limit": limit,
    }


# ---------------------------------------------------------------------
# Dry run
# ---------------------------------------------------------------------
@router.post("/dry-run")
async def cleanup_dry_run(req: CleanupRunRequest):
    """
    Execute cleanup in dry-run mode.
    """
    result = await run_cleanup(
        ttl_days_ephemeral=req.ttl_days_ephemeral,
        ttl_days_normal=req.ttl_days_normal,
        site_id=_normalize_site_id(req.site_id),
        limit=req.limit,
        dry_run=True,
    )
    return result


# ---------------------------------------------------------------------
# Real cleanup
# ---------------------------------------------------------------------
@router.post("/run")
async def cleanup_run(req: CleanupRunRequest):
    """
    Execute actual cleanup.
    """
    result = await run_cleanup(
        ttl_days_ephemeral=req.ttl_days_ephemeral,
        ttl_days_normal=req.ttl_days_normal,
        site_id=_normalize_site_id(req.site_id),
        limit=req.limit,
        dry_run=False,
    )
    return result


# ---------------------------------------------------------------------
# Cleanup logs
# ---------------------------------------------------------------------
@router.get("/logs")
async def cleanup_logs(
    run_id: str = Query("", description="Optional run ID"),
    limit: int = Query(200, ge=1, le=5000, description="Max rows"),
):
    """
    Return cleanup_log rows.
    """
    where: list[str] = []
    params: list[Any] = []

    run_id_norm = str(run_id or "").strip()
    if run_id_norm:
        where.append("run_id=?")
        params.append(run_id_norm)

    sql = """
    SELECT *
    FROM cleanup_log
    """
    if where:
        sql += " WHERE " + " AND ".join(where)

    sql += " ORDER BY created_at DESC LIMIT ?"
    params.append(int(limit))

    db = await get_db()
    try:
        cur = await db.execute(sql, tuple(params))
        rows = await cur.fetchall()
        return {
            "logs": [dict(r) for r in rows],
            "count": len(rows),
        }
    finally:
        await db.close()