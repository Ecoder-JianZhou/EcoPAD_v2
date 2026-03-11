from __future__ import annotations

from typing import Any

import httpx
from fastapi import APIRouter, Body, Header, HTTPException, Query

from app.core.settings import RUNNER_SERVICE_URL
from app.api.auth import require_user

router = APIRouter()


# ---------------------------------------------------------------------
# Auth helper
# ---------------------------------------------------------------------
async def _require_superuser(authorization: str | None) -> dict[str, Any]:
    """
    Only superuser can manage cleanup.
    """
    user = await require_user(authorization)
    if str(user.get("role") or "") != "superuser":
        raise HTTPException(status_code=403, detail="Superuser access required")
    return user


# ---------------------------------------------------------------------
# Runner proxy helpers
# ---------------------------------------------------------------------
async def _runner_request(
    method: str,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    json_body: dict[str, Any] | None = None,
    timeout: float = 10.0,
) -> dict[str, Any]:
    """
    Proxy one request to Runner and preserve status/error details.
    """
    if not RUNNER_SERVICE_URL:
        raise HTTPException(status_code=500, detail="RUNNER_SERVICE_URL is not configured")

    url = f"{RUNNER_SERVICE_URL}{path}"

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.request(method, url, params=params, json=json_body)

        try:
            data = resp.json()
        except Exception:
            data = {"detail": resp.text or resp.reason_phrase or "Runner request failed"}

        if resp.status_code >= 400:
            raise HTTPException(
                status_code=resp.status_code,
                detail=data.get("detail") if isinstance(data, dict) else str(data),
            )

        if isinstance(data, dict):
            return data

        return {"ok": True, "data": data}

    except HTTPException:
        raise
    except Exception as ex:
        raise HTTPException(status_code=502, detail=f"Runner request failed: {ex}")


async def _runner_get(
    path: str,
    *,
    params: dict[str, Any] | None = None,
    timeout: float = 10.0,
) -> dict[str, Any]:
    return await _runner_request("GET", path, params=params, timeout=timeout)


async def _runner_post(
    path: str,
    *,
    json_body: dict[str, Any] | None = None,
    timeout: float = 10.0,
) -> dict[str, Any]:
    return await _runner_request("POST", path, json_body=json_body, timeout=timeout)


# ---------------------------------------------------------------------
# Cleanup candidates
# ---------------------------------------------------------------------
@router.get("/candidates")
async def cleanup_candidates(
    ttl_days_ephemeral: int = Query(7, ge=1),
    ttl_days_normal: int = Query(90, ge=1),
    site_id: str = Query(""),
    limit: int = Query(500, ge=1, le=5000),
    authorization: str | None = Header(default=None),
):
    """
    Return cleanup candidates from Runner.
    """
    await _require_superuser(authorization)

    params = {
        "ttl_days_ephemeral": ttl_days_ephemeral,
        "ttl_days_normal": ttl_days_normal,
        "site_id": site_id,
        "limit": limit,
    }
    return await _runner_get(
        "/api/cleanup/candidates",
        params=params,
        timeout=15.0,
    )


# ---------------------------------------------------------------------
# Dry run
# ---------------------------------------------------------------------
@router.post("/dry-run")
async def cleanup_dry_run(
    payload: dict[str, Any] = Body(default={}),
    authorization: str | None = Header(default=None),
):
    """
    Execute cleanup dry-run through Runner.

    Example payload:
    {
      "ttl_days_ephemeral": 7,
      "ttl_days_normal": 90,
      "site_id": "SPRUCE",
      "limit": 100
    }
    """
    await _require_superuser(authorization)

    return await _runner_post(
        "/api/cleanup/dry-run",
        json_body=payload or {},
        timeout=30.0,
    )


# ---------------------------------------------------------------------
# Real cleanup
# ---------------------------------------------------------------------
@router.post("/run")
async def cleanup_run(
    payload: dict[str, Any] = Body(default={}),
    authorization: str | None = Header(default=None),
):
    """
    Execute real cleanup through Runner.
    """
    await _require_superuser(authorization)

    return await _runner_post(
        "/api/cleanup/run",
        json_body=payload or {},
        timeout=60.0,
    )


# ---------------------------------------------------------------------
# Cleanup logs
# ---------------------------------------------------------------------
@router.get("/logs")
async def cleanup_logs(
    run_id: str = Query(""),
    limit: int = Query(200, ge=1, le=5000),
    authorization: str | None = Header(default=None),
):
    """
    Return cleanup logs from Runner.
    """
    await _require_superuser(authorization)

    params = {
        "run_id": run_id,
        "limit": limit,
    }
    return await _runner_get(
        "/api/cleanup/logs",
        params=params,
        timeout=15.0,
    )