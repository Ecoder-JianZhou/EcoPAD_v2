"""
Runner runs API.

Responsibilities:
- expose run list/detail from Runner DB
- expose registered run outputs
- proxy run manifest and timeseries from Site
- delete terminal runs safely
- provide a stable Runner-facing run API for Portal

Notes:
- Runner owns run truth and run status
- Site owns raw output files and run-specific data reading
"""

from __future__ import annotations

from typing import Any

import httpx
from fastapi import APIRouter, HTTPException, Query

from app.core.settings import SITE_REQUEST_TIMEOUT
from app.services.run_manager import (
    delete_run,
    get_run,
    get_run_with_outputs,
    list_run_outputs,
    list_runs,
)
from app.services.site_registry import registry


router = APIRouter(prefix="/api/runs", tags=["runs"])


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


def _parse_int_or_none(value: str) -> int | None:
    """
    Parse optional integer query parameter.
    """
    if value is None or value == "":
        return None
    try:
        return int(value)
    except Exception:
        return None


# ---------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------
@router.get("")
async def api_list_runs(
    user_id: str = Query("", description="Optional user ID"),
    site_id: str = Query("", description="Optional site ID"),
    status: str = Query("", description="Optional run status"),
    task_type: str = Query("", description="Optional task type"),
    limit: int = Query(100, description="Max number of rows"),
):
    """
    List runs from Runner DB.
    """
    rows = await list_runs(
        user_id=_parse_int_or_none(user_id),
        site_id=site_id or None,
        status=status or None,
        task_type=task_type or None,
        limit=limit,
    )
    return {"runs": rows}


@router.get("/{run_id}")
async def api_get_run(run_id: str):
    """
    Return one run with its registered outputs.
    """
    run = await get_run_with_outputs(run_id)
    if not run:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")
    return run


@router.delete("/{run_id}")
async def api_delete_run(run_id: str):
    """
    Delete one terminal run.

    Rules:
    - only done / failed / cancelled runs can be deleted
    - runs referenced by forecast_registry cannot be deleted
    """
    try:
        deleted = await delete_run(run_id)
        return {
            "ok": True,
            "deleted_run_id": run_id,
            "run": deleted,
        }
    except ValueError as ex:
        msg = str(ex)
        if "not found" in msg.lower():
            raise HTTPException(status_code=404, detail=msg)
        raise HTTPException(status_code=400, detail=msg)


@router.get("/{run_id}/outputs")
async def api_get_run_outputs(run_id: str):
    """
    Return run_outputs rows for one run.
    """
    run = await get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")

    outputs = await list_run_outputs(run_id)
    return {
        "run_id": run_id,
        "outputs": outputs,
    }


@router.get("/{run_id}/manifest")
async def api_get_run_manifest(run_id: str):
    """
    Proxy run manifest from Site.
    """
    run = await get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")

    site_id = run["site_id"]

    data = await _site_get_json(
        site_id=site_id,
        path=f"/runs/{run_id}/manifest",
    )

    if data is None:
        raise HTTPException(
            status_code=502,
            detail=f"Manifest unavailable from site {site_id} for run {run_id}",
        )

    return data


@router.get("/{run_id}/timeseries")
async def api_get_run_timeseries(
    run_id: str,
    variable: str = Query(..., description="Variable name"),
    model: str = Query("", description="Optional model ID"),
    treatment: str = Query("", description="Optional treatment ID"),
):
    """
    Proxy run timeseries from Site.
    """
    run = await get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")

    site_id = run["site_id"]

    params: dict[str, Any] = {
        "variable": variable,
    }
    if model:
        params["model"] = model
    if treatment:
        params["treatment"] = treatment

    data = await _site_get_json(
        site_id=site_id,
        path=f"/runs/{run_id}/timeseries",
        params=params,
    )

    if data is None:
        raise HTTPException(
            status_code=502,
            detail=f"Timeseries unavailable from site {site_id} for run {run_id}",
        )

    return data