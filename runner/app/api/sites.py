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

    base_url = str(site.get("base_url") or "").rstrip("/")
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


def _shape_site_public(site: dict[str, Any]) -> dict[str, Any]:
    """
    Public-facing site info for Portal.

    Notes:
    - id   : canonical short site id for display and API routing
    - name : long display name / tooltip
    - do not expose unnecessary internal-only fields unless useful
    """
    return {
        "id": str(site.get("id") or "").strip(),
        "name": str(site.get("name") or "").strip(),
        "enabled": bool(site.get("enabled", True)),
        "models": site.get("models") if isinstance(site.get("models"), list) else [],
        "treatments": site.get("treatments") if isinstance(site.get("treatments"), list) else [],
        "meta": site.get("meta") if isinstance(site.get("meta"), dict) else {},
    }


def _shape_site_internal(site: dict[str, Any]) -> dict[str, Any]:
    """
    Internal/full site registry record.

    Used when you want the full registry entry, including base_url.
    """
    return {
        "id": str(site.get("id") or "").strip(),
        "name": str(site.get("name") or "").strip(),
        "base_url": str(site.get("base_url") or "").strip(),
        "enabled": bool(site.get("enabled", True)),
        "models": site.get("models") if isinstance(site.get("models"), list) else [],
        "treatments": site.get("treatments") if isinstance(site.get("treatments"), list) else [],
        "meta": site.get("meta") if isinstance(site.get("meta"), dict) else {},
    }


# ---------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------
@router.get("")
async def list_sites():
    """
    Return enabled site records for public/Portal use.

    Frontend should display:
    - site.id   as the canonical short name
    - site.name as long name / tooltip
    """
    sites = registry.list_sites(enabled_only=True) or []
    return {"sites": [_shape_site_public(site) for site in sites if isinstance(site, dict)]}


@router.get("/list")
async def list_sites_alias():
    """
    Backward-compatible alias.
    """
    sites = registry.list_sites(enabled_only=True) or []
    return {"sites": [_shape_site_public(site) for site in sites if isinstance(site, dict)]}


@router.get("/all")
async def list_all_sites():
    """
    Return all site records, including disabled ones.

    This keeps full internal fields such as base_url, mainly for admin/debug use.
    """
    sites = registry.list_sites(enabled_only=False) or []
    return {"sites": [_shape_site_internal(site) for site in sites if isinstance(site, dict)]}


@router.get("/{site_id}")
async def get_site(site_id: str):
    """
    Return one site registry record from Runner.

    This endpoint keeps the full/internal record.
    """
    site = registry.get_site(site_id)
    if not site:
        raise HTTPException(status_code=404, detail=f"Site not found: {site_id}")

    return _shape_site_internal(site)


@router.get("/{site_id}/meta")
async def get_site_meta(site_id: str):
    """
    Return site /meta through Runner.

    Behavior:
    - fetch from site service
    - if site /meta is unavailable, fall back to registry record
    - keep id/name from Runner registry as stable public identifiers
    """
    site = registry.get_site(site_id)
    if not site:
        raise HTTPException(status_code=404, detail=f"Site not found: {site_id}")

    data = await _site_get_json(site_id=site_id, path="/meta")

    base = _shape_site_public(site)

    if isinstance(data, dict):
        merged = dict(base)
        merged.update(data)

        # keep Runner registry as the source of truth for these stable fields
        merged["id"] = base["id"]
        merged["name"] = base["name"]
        merged["enabled"] = base["enabled"]

        if "models" not in merged or not isinstance(merged.get("models"), list):
            merged["models"] = base["models"]

        if "treatments" not in merged or not isinstance(merged.get("treatments"), list):
            merged["treatments"] = base["treatments"]

        if "meta" not in merged or not isinstance(merged.get("meta"), dict):
            merged["meta"] = base["meta"]

        return merged

    return base