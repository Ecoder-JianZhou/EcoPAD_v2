from __future__ import annotations

from typing import Any

import httpx
from fastapi import APIRouter, HTTPException, Query

from app.core.settings import RUNNER_SERVICE_URL

router = APIRouter()


# ---------------------------------------------------------------------
# Runner proxy helpers
# ---------------------------------------------------------------------
async def _runner_request(
    method: str,
    path: str,
    *,
    params: dict[str, Any] | None = None,
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
            resp = await client.request(method, url, params=params)

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


# ---------------------------------------------------------------------
# Forecast APIs
# ---------------------------------------------------------------------
@router.get("/sites")
async def forecast_sites():
    return await _runner_get("/api/forecast/sites", timeout=5.0)


@router.get("/{site_id}/meta")
async def forecast_meta(site_id: str):
    return await _runner_get(f"/api/forecast/{site_id}/meta", timeout=8.0)


@router.get("/{site_id}/summary")
async def forecast_summary(site_id: str):
    return await _runner_get(
        f"/api/forecast/{site_id}/summary",
        timeout=8.0,
    )


@router.get("/{site_id}/runs")
async def forecast_runs(
    site_id: str,
    models: str = Query(default="", description="Comma-separated model IDs"),
    treatments: str = Query(default="", description="Comma-separated treatment IDs"),
    variable: str = Query(default="", description="Variable name, e.g. GPP"),
    task_type: str = Query(default="", description="Optional task type"),
    scheduled_task_id: int | None = Query(default=None, description="Optional schedule task id"),
    limit: int = Query(default=200, ge=1, le=1000),
):
    params: dict[str, Any] = {
        "models": models,
        "treatments": treatments,
        "variable": variable,
        "task_type": task_type,
        "limit": limit,
    }
    if scheduled_task_id is not None:
        params["scheduled_task_id"] = scheduled_task_id

    return await _runner_get(
        f"/api/forecast/{site_id}/runs",
        params=params,
        timeout=15.0,
    )


@router.get("/{site_id}/runs/{run_id}/timeseries")
async def forecast_run_timeseries(
    site_id: str,
    run_id: str,
    variable: str = Query(..., description="Variable name, e.g. GPP"),
    model: str = Query(..., description="Model ID"),
    treatment: str = Query(..., description="Treatment ID"),
):
    return await _runner_get(
        f"/api/forecast/{site_id}/runs/{run_id}/timeseries",
        params={
            "variable": variable,
            "model": model,
            "treatment": treatment,
        },
        timeout=20.0,
    )


@router.get("/{site_id}/data")
async def forecast_data(
    site_id: str,
    variable: str = Query(..., description="Variable name, e.g. GPP"),
    models: str = Query(default="", description="Comma-separated model IDs"),
    treatments: str = Query(default="", description="Comma-separated treatment IDs"),
):
    return await _runner_get(
        f"/api/forecast/{site_id}/data",
        params={
            "variable": variable,
            "models": models,
            "treatments": treatments,
        },
        timeout=20.0,
    )


@router.get("/{site_id}/obs")
async def forecast_obs(
    site_id: str,
    variable: str = Query(..., description="Variable name, e.g. GPP"),
    treatments: str = Query(default="", description="Comma-separated treatment IDs"),
):
    return await _runner_get(
        f"/api/forecast/{site_id}/obs",
        params={
            "variable": variable,
            "treatments": treatments,
        },
        timeout=15.0,
    )


# ---------------------------------------------------------------------
# Parameter metadata
# ---------------------------------------------------------------------
@router.get("/{site_id}/params/meta")
async def forecast_params_meta(
    site_id: str,
    model: str = Query(default="", description="Optional model ID"),
):
    params = {"model": model} if model else None
    return await _runner_get(
        f"/api/forecast/{site_id}/params/meta",
        params=params,
        timeout=10.0,
    )


# ---------------------------------------------------------------------
# Latest parameter snapshot
# ---------------------------------------------------------------------
@router.get("/{site_id}/params/latest")
async def forecast_params_latest(
    site_id: str,
    model: str = Query(..., description="Model ID"),
    treatment: str = Query(..., description="Treatment ID"),
    variable: str = Query(..., description="Forecast variable, e.g. GPP"),
):
    return await _runner_get(
        f"/api/forecast/{site_id}/params/latest",
        params={
            "model": model,
            "treatment": treatment,
            "variable": variable,
        },
        timeout=15.0,
    )


# ---------------------------------------------------------------------
# Parameter history across repeated auto-forecast runs
# ---------------------------------------------------------------------
@router.get("/{site_id}/params/history")
async def forecast_params_history(
    site_id: str,
    param: str = Query(..., description="Parameter name"),
    models: str = Query(default="", description="Comma-separated model IDs"),
    treatments: str = Query(default="", description="Comma-separated treatment IDs"),
    variable: str = Query(default="GPP", description="Reference forecast variable"),
):
    return await _runner_get(
        f"/api/forecast/{site_id}/params/history",
        params={
            "param": param,
            "models": models,
            "treatments": treatments,
            "variable": variable,
        },
        timeout=20.0,
    )


# ---------------------------------------------------------------------
# Parameter histogram / accepted samples
# ---------------------------------------------------------------------
@router.get("/{site_id}/params/hist")
async def forecast_params_hist(
    site_id: str,
    run_id: str = Query(..., description="Run ID"),
    models: str = Query(default="", description="Comma-separated model IDs"),
    treatments: str = Query(default="", description="Comma-separated treatment IDs"),
    params: str = Query(default="", description="Comma-separated parameter names"),
):
    return await _runner_get(
        f"/api/forecast/{site_id}/params/hist",
        params={
            "run_id": run_id,
            "models": models,
            "treatments": treatments,
            "params": params,
        },
        timeout=30.0,
    )


# ---------------------------------------------------------------------
# Run-level parameter readers for Forecast page
# ---------------------------------------------------------------------
@router.get("/{site_id}/runs/{run_id}/parameter_summary")
async def forecast_run_parameter_summary(
    site_id: str,
    run_id: str,
    model: str = Query(..., description="Model ID"),
    treatment: str = Query(..., description="Treatment ID"),
):
    return await _runner_get(
        f"/api/workflow/runs/{run_id}/parameter_summary",
        params={
            "model": model,
            "treatment": treatment,
        },
        timeout=15.0,
    )


@router.get("/{site_id}/runs/{run_id}/parameters_accepted")
async def forecast_run_parameters_accepted(
    site_id: str,
    run_id: str,
    model: str = Query(..., description="Model ID"),
    treatment: str = Query(..., description="Treatment ID"),
):
    return await _runner_get(
        f"/api/workflow/runs/{run_id}/parameters_accepted",
        params={
            "model": model,
            "treatment": treatment,
        },
        timeout=20.0,
    )