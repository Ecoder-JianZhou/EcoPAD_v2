"""
Runner sites API.

Responsibilities:
- expose site registry information
- proxy lightweight site metadata from Runner to Portal
- keep Portal talking only to Runner

Notes:
- Runner is the source of truth for which sites are registered
- Site remains the source of truth for site-specific /meta details
"""

from __future__ import annotations

from typing import Any

import httpx
from fastapi import APIRouter, HTTPException

from app.core.settings import SITE_REQUEST_TIMEOUT
from app.services.site_registry import registry


router = APIRouter(prefix="/api/sites", tags=["sites"])


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
async def _site_get_json(
    *,
    site_id: str,
    path: str,
    params: dict[str, Any] | None = None,
) -> dict[str, Any] | list[Any] | None:
    """
    Fetch JSON from one site endpoint.

    Returns:
    - parsed JSON on success
    - None on failure
    """
    site = registry.get_site(site_id)
    if not site:
        return None

    if not site.get("enabled", True):
        return None

    base_url = (site.get("base_url") or "").rstrip("/")
    if not base_url:
        return None

    try:
        async with httpx.AsyncClient(timeout=SITE_REQUEST_TIMEOUT) as client:
            resp = await client.get(f"{base_url}{path}", params=params)
            if resp.status_code != 200:
                return None
            return resp.json()
    except Exception:
        return None


# ---------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------
@router.get("")
async def list_sites():
    """
    Return enabled site records.
    """
    return {"sites": registry.list_sites(enabled_only=True)}


@router.get("/all")
async def list_all_sites():
    """
    Return all site records, including disabled ones.
    """
    return {"sites": registry.list_sites(enabled_only=False)}


@router.get("/{site_id}")
async def get_site(site_id: str):
    """
    Return one site record from Runner registry.
    """
    site = registry.get_site(site_id)
    if not site:
        raise HTTPException(status_code=404, detail=f"Site not found: {site_id}")

    return site


@router.get("/{site_id}/meta")
async def get_site_meta(site_id: str):
    """
    Return site /meta through Runner.

    Behavior:
    - fetch from site service
    - if site /meta is unavailable, fall back to registry record
    """
    site = registry.get_site(site_id)
    if not site:
        raise HTTPException(status_code=404, detail=f"Site not found: {site_id}")

    data = await _site_get_json(site_id=site_id, path="/meta")

    if isinstance(data, dict):
        merged = dict(site)
        merged.update(data)
        return merged

    # Fallback to registry info only.
    return dict(site)