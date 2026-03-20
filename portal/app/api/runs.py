from __future__ import annotations

from typing import Any

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response, StreamingResponse

from app.core.settings import RUNNER_SERVICE_URL

router = APIRouter()


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
async def _runner_request(
    method: str,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    timeout: float = 20.0,
) -> dict[str, Any]:
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


async def _runner_stream(
    method: str,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    timeout: float = 60.0,
):
    if not RUNNER_SERVICE_URL:
        raise HTTPException(status_code=500, detail="RUNNER_SERVICE_URL is not configured")

    url = f"{RUNNER_SERVICE_URL}{path}"

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.request(method, url, params=params)

            if resp.status_code >= 400:
                try:
                    data = resp.json()
                    detail = data.get("detail", "Runner download request failed")
                except Exception:
                    detail = resp.text or resp.reason_phrase or "Runner download request failed"
                raise HTTPException(status_code=resp.status_code, detail=detail)

            content = resp.content
            media_type = resp.headers.get("content-type", "application/octet-stream")
            content_disposition = resp.headers.get("content-disposition")

            headers = {}
            if content_disposition:
                headers["Content-Disposition"] = content_disposition

            return Response(
                content=content,
                media_type=media_type,
                headers=headers,
            )

    except HTTPException:
        raise
    except Exception as ex:
        raise HTTPException(status_code=502, detail=f"Runner request failed: {ex}")


# ---------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------
@router.get("")
async def list_runs(
    user_id: str = Query(default=""),
    site_id: str = Query(default=""),
    status: str = Query(default=""),
    task_type: str = Query(default=""),
    limit: int = Query(default=100),
):
    params = {
        "user_id": user_id,
        "site_id": site_id,
        "status": status,
        "task_type": task_type,
        "limit": limit,
    }
    return await _runner_request("GET", "/api/runs", params=params, timeout=20.0)


@router.get("/{run_id}")
async def get_run(run_id: str):
    return await _runner_request("GET", f"/api/runs/{run_id}", timeout=20.0)


@router.delete("/{run_id}")
async def delete_run(run_id: str):
    return await _runner_request("DELETE", f"/api/runs/{run_id}", timeout=60.0)


@router.get("/{run_id}/outputs")
async def get_run_outputs(run_id: str):
    return await _runner_request("GET", f"/api/runs/{run_id}/outputs", timeout=20.0)


@router.get("/{run_id}/artifacts")
async def get_run_artifacts(run_id: str):
    return await _runner_request("GET", f"/api/runs/{run_id}/artifacts", timeout=20.0)


@router.get("/{run_id}/manifest")
async def get_run_manifest(run_id: str):
    return await _runner_request("GET", f"/api/runs/{run_id}/manifest", timeout=20.0)


@router.get("/{run_id}/timeseries")
async def get_run_timeseries(
    run_id: str,
    variable: str = Query(...),
    model: str = Query(default=""),
    treatment: str = Query(default=""),
    output_type: str = Query(default=""),
    series_type: str = Query(default=""),
):
    params = {
        "variable": variable,
        "model": model,
        "treatment": treatment,
        "output_type": output_type,
        "series_type": series_type,
    }
    return await _runner_request("GET", f"/api/runs/{run_id}/timeseries", params=params, timeout=30.0)


@router.get("/{run_id}/parameter_summary")
async def get_run_parameter_summary(
    run_id: str,
    model: str = Query(default=""),
    treatment: str = Query(default=""),
    output_type: str = Query(default=""),
    series_type: str = Query(default=""),
):
    params = {
        "model": model,
        "treatment": treatment,
        "output_type": output_type,
        "series_type": series_type,
    }
    return await _runner_request(
        "GET",
        f"/api/runs/{run_id}/parameter_summary",
        params=params,
        timeout=30.0,
    )


@router.get("/{run_id}/parameters_accepted")
async def get_run_parameters_accepted(
    run_id: str,
    model: str = Query(default=""),
    treatment: str = Query(default=""),
    output_type: str = Query(default=""),
    series_type: str = Query(default=""),
):
    params = {
        "model": model,
        "treatment": treatment,
        "output_type": output_type,
        "series_type": series_type,
    }
    return await _runner_request(
        "GET",
        f"/api/runs/{run_id}/parameters_accepted",
        params=params,
        timeout=30.0,
    )


@router.get("/{run_id}/parameter_best")
async def get_run_parameter_best(
    run_id: str,
    model: str = Query(default=""),
    treatment: str = Query(default=""),
    output_type: str = Query(default=""),
    series_type: str = Query(default=""),
):
    params = {
        "model": model,
        "treatment": treatment,
        "output_type": output_type,
        "series_type": series_type,
    }
    return await _runner_request(
        "GET",
        f"/api/runs/{run_id}/parameter_best",
        params=params,
        timeout=30.0,
    )


@router.get("/{run_id}/download")
async def download_run_file(
    run_id: str,
    rel_path: str = Query(default=""),
    artifact_id: int | None = Query(default=None),
    bundle: bool = Query(default=False),
):
    params = {
        "rel_path": rel_path,
        "artifact_id": artifact_id,
        "bundle": 1 if bundle else 0,
    }
    return await _runner_stream(
        "GET",
        f"/api/runs/{run_id}/download",
        params=params,
        timeout=120.0,
    )