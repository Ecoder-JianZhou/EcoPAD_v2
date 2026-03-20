"""
Runner runs API.

Responsibilities:
- expose run list/detail from Runner DB
- expose registered run outputs / artifacts
- proxy run manifest and timeseries from Site
- proxy run parameter readers from Site
- proxy file download endpoints for one run
- delete terminal runs safely and ask Site to remove run workspace files
- provide a stable Runner-facing run API for Portal

Notes:
- Runner owns run truth and run status
- Site owns raw output files and run-specific data reading
- Runner should not depend on direct filesystem access to Site containers
"""

from __future__ import annotations

from typing import Any

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response, StreamingResponse

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


SUPPORTED_OUTPUT_TYPES = (
    "simulation_without_da",
    "simulation_with_da",
    "forecast_with_da",
    "forecast_without_da",
    "auto_forecast_with_da",
    "auto_forecast_without_da",
)

LEGACY_OUTPUT_ALIASES = {
    "simulate": "simulation_without_da",
    "simulation_with_da": "simulation_with_da",
    "forecast_with_da": "forecast_with_da",
    "forecast_without_da": "forecast_without_da",
}


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


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


def _normalize_output_type(value: Any, default: str = "forecast_with_da") -> str:
    text = str(value or "").strip()
    if not text:
        return default

    if text in SUPPORTED_OUTPUT_TYPES:
        return text

    mapped = LEGACY_OUTPUT_ALIASES.get(text)
    if mapped:
        return mapped

    return default


def _build_output_params(
    *,
    output_type: str = "",
    series_type: str = "",
) -> dict[str, Any]:
    resolved = _normalize_output_type(output_type or series_type, default="")
    if not resolved:
        return {}
    return {
        "output_type": resolved,
        "series_type": resolved,
    }


def _require_site(site_id: str) -> dict[str, Any]:
    site = registry.get_site(site_id)
    if not site:
        raise HTTPException(status_code=404, detail=f"Site not found: {site_id}")
    if not site.get("enabled", True):
        raise HTTPException(status_code=403, detail=f"Site is disabled: {site_id}")
    return site


def _site_base_url(site_id: str) -> str:
    site = _require_site(site_id)
    base_url = _normalize_text(site.get("base_url")).rstrip("/")
    if not base_url:
        raise HTTPException(status_code=500, detail=f"Site base_url missing: {site_id}")
    return base_url


async def _site_get_json(
    *,
    site_id: str,
    path: str,
    params: dict[str, Any] | None = None,
) -> dict[str, Any] | list[Any] | None:
    """
    Fetch JSON from one site endpoint.

    Returns None on non-200 or request failure.
    """
    base_url = _site_base_url(site_id)

    try:
        async with httpx.AsyncClient(timeout=SITE_REQUEST_TIMEOUT) as client:
            resp = await client.get(f"{base_url}{path}", params=params)
            if resp.status_code != 200:
                return None
            return resp.json()
    except Exception:
        return None


async def _require_site_json(
    *,
    site_id: str,
    path: str,
    params: dict[str, Any] | None = None,
) -> dict[str, Any] | list[Any]:
    """
    Fetch JSON from one site endpoint or raise HTTPException.
    """
    base_url = _site_base_url(site_id)

    try:
        async with httpx.AsyncClient(timeout=SITE_REQUEST_TIMEOUT) as client:
            resp = await client.get(f"{base_url}{path}", params=params)
            if resp.status_code != 200:
                detail = ""
                try:
                    body = resp.json()
                    detail = str(body.get("detail") or "")
                except Exception:
                    detail = resp.text or ""
                raise HTTPException(
                    status_code=resp.status_code,
                    detail=detail or f"Site request failed: GET {path} returned {resp.status_code}",
                )
            data = resp.json()
            return data if isinstance(data, (dict, list)) else {}
    except HTTPException:
        raise
    except Exception as ex:
        raise HTTPException(status_code=502, detail=f"Site request failed: {str(ex)}")


async def _proxy_site_download(
    *,
    site_id: str,
    path: str,
    params: dict[str, Any] | None = None,
) -> Response:
    """
    Proxy a download response from Site.

    This is used for:
    - one artifact file
    - one zip bundle
    """
    base_url = _site_base_url(site_id)

    try:
        async with httpx.AsyncClient(timeout=SITE_REQUEST_TIMEOUT) as client:
            resp = await client.get(f"{base_url}{path}", params=params)

            if resp.status_code != 200:
                detail = ""
                try:
                    body = resp.json()
                    detail = str(body.get("detail") or "")
                except Exception:
                    detail = resp.text or ""
                raise HTTPException(
                    status_code=resp.status_code,
                    detail=detail or f"Site download failed: GET {path} returned {resp.status_code}",
                )

            headers: dict[str, str] = {}
            content_type = resp.headers.get("content-type") or "application/octet-stream"
            content_disposition = resp.headers.get("content-disposition")
            if content_disposition:
                headers["Content-Disposition"] = content_disposition

            return StreamingResponse(
                content=iter([resp.content]),
                media_type=content_type,
                headers=headers,
            )

    except HTTPException:
        raise
    except Exception as ex:
        raise HTTPException(status_code=502, detail=f"Site download failed: {str(ex)}")


async def _best_effort_site_delete_run(site_id: str, run_id: str) -> dict[str, Any]:
    """
    Ask Site to remove one run directory.

    This is best-effort cleanup after Runner DB deletion succeeds.
    """
    base_url = _site_base_url(site_id)

    try:
        async with httpx.AsyncClient(timeout=SITE_REQUEST_TIMEOUT) as client:
            resp = await client.delete(f"{base_url}/runs/{run_id}")
            if resp.status_code == 200:
                data = resp.json()
                return data if isinstance(data, dict) else {"ok": True}
            return {
                "ok": False,
                "detail": f"Site delete returned {resp.status_code}",
            }
    except Exception as ex:
        return {
            "ok": False,
            "detail": str(ex),
        }


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


@router.get("/{run_id}/outputs")
async def api_get_run_outputs(run_id: str):
    """
    Return raw run_outputs rows for one run.
    """
    run = await get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")

    outputs = await list_run_outputs(run_id)
    return {
        "run_id": run_id,
        "outputs": outputs,
    }


@router.get("/{run_id}/artifacts")
async def api_get_run_artifacts(run_id: str):
    """
    Proxy normalized artifact list from Site.

    This is the Portal-facing endpoint for the download panel.
    """
    run = await get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")

    site_id = _normalize_text(run.get("site_id"))
    return await _require_site_json(
        site_id=site_id,
        path=f"/runs/{run_id}/artifacts",
    )


@router.get("/{run_id}/download")
async def api_download_run_artifact(
    run_id: str,
    rel_path: str = "",
    artifact_id: str = "",
    bundle: str = "0",
):
    """
    Proxy one download request to Site.

    Query parameters are kept as strings to avoid 422 when the frontend sends:
    - artifact_id=
    - bundle=0
    """
    run = await get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")

    site_id = _normalize_text(run.get("site_id"))

    params = {
        "rel_path": _normalize_text(rel_path),
        "artifact_id": _normalize_text(artifact_id),
        "bundle": _normalize_text(bundle) or "0",
    }

    return await _proxy_site_download(
        site_id=site_id,
        path=f"/runs/{run_id}/download",
        params=params,
    )


@router.delete("/{run_id}")
async def api_delete_run(run_id: str):
    """
    Delete one terminal run from Runner DB.

    Rules:
    - only done / failed / cancelled runs can be deleted
    - runs referenced by forecast_registry cannot be deleted

    Extended behavior:
    - after Runner DB deletion succeeds, Runner asks Site to remove the run directory
    """
    run = await get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")

    site_id = _normalize_text(run.get("site_id"))

    try:
        deleted = await delete_run(run_id)
    except ValueError as ex:
        msg = str(ex)
        if "not found" in msg.lower():
            raise HTTPException(status_code=404, detail=msg)
        raise HTTPException(status_code=400, detail=msg)

    site_cleanup = await _best_effort_site_delete_run(site_id, run_id)

    return {
        "ok": True,
        "deleted_run_id": run_id,
        "run": deleted,
        "site_cleanup": site_cleanup,
    }


@router.get("/{run_id}/manifest")
async def api_get_run_manifest(run_id: str):
    """
    Proxy run manifest from Site.
    """
    run = await get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")

    site_id = _normalize_text(run.get("site_id"))

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
    output_type: str = Query("", description="Optional output type"),
    series_type: str = Query("", description="Backward-compatible alias"),
):
    """
    Proxy run timeseries from Site.
    """
    run = await get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")

    site_id = _normalize_text(run.get("site_id"))

    params: dict[str, Any] = {
        "variable": variable,
    }
    if model:
        params["model"] = model
    if treatment:
        params["treatment"] = treatment

    params.update(_build_output_params(output_type=output_type, series_type=series_type))

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


@router.get("/{run_id}/parameter_summary")
async def api_get_run_parameter_summary(
    run_id: str,
    model: str = Query("", description="Optional model ID"),
    treatment: str = Query("", description="Optional treatment ID"),
    output_type: str = Query("", description="Optional output type"),
    series_type: str = Query("", description="Backward-compatible alias"),
):
    """
    Proxy run parameter summary from Site.
    """
    run = await get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")

    site_id = _normalize_text(run.get("site_id"))

    params: dict[str, Any] = {}
    if model:
        params["model"] = model
    if treatment:
        params["treatment"] = treatment

    params.update(_build_output_params(output_type=output_type, series_type=series_type))

    data = await _site_get_json(
        site_id=site_id,
        path=f"/runs/{run_id}/parameter_summary",
        params=params,
    )

    if data is None:
        raise HTTPException(
            status_code=502,
            detail=f"Parameter summary unavailable from site {site_id} for run {run_id}",
        )

    return data


@router.get("/{run_id}/parameters_accepted")
async def api_get_run_parameters_accepted(
    run_id: str,
    model: str = Query("", description="Optional model ID"),
    treatment: str = Query("", description="Optional treatment ID"),
    output_type: str = Query("", description="Optional output type"),
    series_type: str = Query("", description="Backward-compatible alias"),
):
    """
    Proxy accepted parameter samples from Site.

    If Site does not provide accepted samples, return an empty-but-valid payload
    instead of crashing the page.
    """
    run = await get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")

    site_id = _normalize_text(run.get("site_id"))

    params: dict[str, Any] = {}
    if model:
        params["model"] = model
    if treatment:
        params["treatment"] = treatment

    params.update(_build_output_params(output_type=output_type, series_type=series_type))

    data = await _site_get_json(
        site_id=site_id,
        path=f"/runs/{run_id}/parameters_accepted",
        params=params,
    )

    if data is None:
        return {
            "run_id": run_id,
            "model": model,
            "treatment": treatment,
            "rows": [],
            "available": False,
            "detail": "Accepted parameter samples are not available for this run.",
        }

    if isinstance(data, dict):
        if "rows" not in data:
            data["rows"] = []
        if "available" not in data:
            data["available"] = bool(data.get("rows"))
        return data

    return {
        "run_id": run_id,
        "model": model,
        "treatment": treatment,
        "rows": [],
        "available": False,
        "detail": "Accepted parameter samples response is invalid.",
    }


@router.get("/{run_id}/parameter_best")
async def api_get_run_parameter_best(
    run_id: str,
    model: str = Query("", description="Optional model ID"),
    treatment: str = Query("", description="Optional treatment ID"),
    output_type: str = Query("", description="Optional output type"),
    series_type: str = Query("", description="Backward-compatible alias"),
):
    """
    Proxy best-parameter payload from Site.
    """
    run = await get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")

    site_id = _normalize_text(run.get("site_id"))

    params: dict[str, Any] = {}
    if model:
        params["model"] = model
    if treatment:
        params["treatment"] = treatment

    params.update(_build_output_params(output_type=output_type, series_type=series_type))

    data = await _site_get_json(
        site_id=site_id,
        path=f"/runs/{run_id}/parameter_best",
        params=params,
    )

    if data is None:
        raise HTTPException(
            status_code=502,
            detail=f"Parameter best unavailable from site {site_id} for run {run_id}",
        )

    return data