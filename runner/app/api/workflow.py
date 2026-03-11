"""
Runner workflow API.

Responsibilities:
- expose workflow-related site metadata to Portal
- accept workflow submissions
- create run records in Runner DB
- dispatch runs to Site through dispatcher
- expose run manifest / timeseries viewers for Portal Account page

Notes:
- Runner is the control plane only
- Site remains the execution plane
- Permission checks should normally happen in Portal
"""

from __future__ import annotations

from typing import Any

import httpx
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.core.settings import SITE_REQUEST_TIMEOUT
from app.services.site_registry import registry
from app.services.run_manager import create_run, get_run
from app.services.dispatcher import dispatch_run


router = APIRouter(prefix="/api/workflow", tags=["workflow"])


# ---------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------
class SubmitIn(BaseModel):
    """
    Workflow submission payload from Portal.
    """

    site: str = Field(..., description="Site ID")
    models: list[str] = Field(default_factory=list, description="Selected model IDs")
    treatments: list[str] = Field(default_factory=list, description="Selected treatments")
    task: str = Field(..., description="simulate | mcmc | auto_forecast | forecast | custom")
    parameters: dict[str, Any] = Field(default_factory=dict, description="Parameter overrides")
    da: dict[str, Any] = Field(default_factory=dict, description="Data assimilation settings")
    notes: str = Field(default="", description="Optional notes")
    name: str = Field(default="", description="Optional human-readable job name")
    submitted_by: str = Field(default="", description="Username from Portal")
    user_id: int | None = Field(default=None, description="Optional Portal user ID")


# ---------------------------------------------------------------------
# Site proxy helpers
# ---------------------------------------------------------------------
async def _site_get_json(
    *,
    site_id: str,
    path: str,
    params: dict[str, Any] | None = None,
    timeout: float | None = None,
) -> dict[str, Any] | list[Any] | None:
    """
    Fetch JSON from one site endpoint.

    Returns:
    - parsed JSON on success
    - None if site lookup fails or HTTP request fails
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
        async with httpx.AsyncClient(timeout=timeout or SITE_REQUEST_TIMEOUT) as client:
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
    timeout: float | None = None,
) -> dict[str, Any] | list[Any]:
    """
    Fetch JSON from a site endpoint or raise HTTPException.
    """
    site = registry.get_site(site_id)
    if not site:
        raise HTTPException(status_code=404, detail=f"Site not found: {site_id}")

    if not site.get("enabled", True):
        raise HTTPException(status_code=403, detail=f"Site is disabled: {site_id}")

    base_url = (site.get("base_url") or "").rstrip("/")
    if not base_url:
        raise HTTPException(status_code=500, detail=f"Site base_url missing: {site_id}")

    try:
        async with httpx.AsyncClient(timeout=timeout or SITE_REQUEST_TIMEOUT) as client:
            resp = await client.get(f"{base_url}{path}", params=params)
            if resp.status_code != 200:
                raise HTTPException(
                    status_code=502,
                    detail=f"Site request failed: GET {path} returned {resp.status_code}",
                )
            data = resp.json()
            return data if isinstance(data, (dict, list)) else {}
    except HTTPException:
        raise
    except Exception as ex:
        raise HTTPException(status_code=502, detail=f"Site request failed: {str(ex)}")


def _merge_unique_strs(items: list[str], extra: list[str]) -> list[str]:
    """
    Merge two string lists while preserving order and uniqueness.
    """
    out: list[str] = []
    seen: set[str] = set()

    for value in [*(items or []), *(extra or [])]:
        v = str(value or "").strip()
        if not v or v in seen:
            continue
        out.append(v)
        seen.add(v)

    return out


def _pick_model_id(payload: SubmitIn, site_meta: dict[str, Any] | None) -> str:
    """
    Choose one model_id for run creation.

    Current Runner schema stores one model_id per run.
    For now, Workflow is expected to submit one selected model.
    """
    if payload.models:
        return str(payload.models[0])

    models = (site_meta or {}).get("models") or []
    if models:
        return str(models[0])

    return ""


def _derive_models_from_manifest(manifest: dict[str, Any]) -> list[str]:
    """
    Resolve model list from manifest.

    Priority:
    1) request.models / request.model
    2) outputs.index keys
    3) artifacts[].model_id
    4) top-level model_id
    """
    out: list[str] = []
    seen: set[str] = set()

    def add(v: Any) -> None:
        s = str(v or "").strip()
        if s and s not in seen:
            seen.add(s)
            out.append(s)

    req = manifest.get("request") or {}

    if isinstance(req.get("models"), list):
        for x in req["models"]:
            add(x)

    if req.get("model"):
        add(req.get("model"))

    outputs = manifest.get("outputs") or {}
    index_obj = outputs.get("index") or {}
    if isinstance(index_obj, dict):
        for model_id in index_obj.keys():
            add(model_id)

    artifacts = manifest.get("artifacts") or []
    if isinstance(artifacts, list):
        for item in artifacts:
            if isinstance(item, dict):
                add(item.get("model_id"))

    add(manifest.get("model_id"))

    return out


def _derive_treatments_from_manifest(manifest: dict[str, Any]) -> list[str]:
    """
    Resolve treatment list from manifest.

    Priority:
    1) request.treatments / request.treatment
    2) outputs.index[model] keys
    3) artifacts[].treatment
    """
    out: list[str] = []
    seen: set[str] = set()

    def add(v: Any) -> None:
        s = str(v or "").strip()
        if s and s not in seen:
            seen.add(s)
            out.append(s)

    req = manifest.get("request") or {}

    if isinstance(req.get("treatments"), list):
        for x in req["treatments"]:
            add(x)

    if req.get("treatment"):
        add(req.get("treatment"))

    outputs = manifest.get("outputs") or {}
    index_obj = outputs.get("index") or {}
    if isinstance(index_obj, dict):
        for model_block in index_obj.values():
            if isinstance(model_block, dict):
                for treatment in model_block.keys():
                    add(treatment)

    artifacts = manifest.get("artifacts") or []
    if isinstance(artifacts, list):
        for item in artifacts:
            if isinstance(item, dict):
                add(item.get("treatment"))

    return out


# ---------------------------------------------------------------------
# Workflow metadata endpoints
# ---------------------------------------------------------------------
@router.get("/sites")
async def workflow_sites():
    """
    Return enabled site IDs known to Runner.
    """
    return {"sites": registry.list_site_ids(enabled_only=True)}


@router.get("/site_meta")
async def workflow_site_meta(site: str = Query(..., description="Site ID")):
    """
    Proxy site /meta through Runner.

    This keeps Portal talking to Runner only.
    """
    return await _require_site_json(
        site_id=site,
        path="/meta",
        timeout=SITE_REQUEST_TIMEOUT,
    )


@router.get("/params_meta")
async def workflow_params_meta(
    site: str = Query(..., description="Site ID"),
    model: str = Query("", description="Optional model ID"),
):
    """
    Proxy site /params/meta through Runner.
    """
    params = {"model": model} if model else None
    return await _require_site_json(
        site_id=site,
        path="/params/meta",
        params=params,
        timeout=SITE_REQUEST_TIMEOUT,
    )


@router.get("/meta")
async def workflow_meta():
    """
    Return lightweight workflow bootstrap metadata.
    """
    sites = registry.list_site_ids(enabled_only=True)

    models: list[str] = []
    treatments: list[str] = []

    if sites:
        first_site = sites[0]
        site_meta = await _site_get_json(
            site_id=first_site,
            path="/meta",
            timeout=SITE_REQUEST_TIMEOUT,
        )
        if isinstance(site_meta, dict):
            models = _merge_unique_strs([], site_meta.get("models") or [])
            treatments = _merge_unique_strs([], site_meta.get("treatments") or [])

    return {
        "sites": sites,
        "models": models,
        "treatments": treatments,
        "tasks": [
            {"key": "simulate", "label": "Simulation"},
            {"key": "mcmc", "label": "MCMC Assimilation"},
            {"key": "auto_forecast", "label": "Auto Forecast"},
        ],
    }


# ---------------------------------------------------------------------
# Submit workflow job
# ---------------------------------------------------------------------
@router.post("/submit")
async def submit_workflow(payload: SubmitIn):
    """
    Create and dispatch one workflow run.
    """
    site = registry.get_site(payload.site)
    if not site:
        raise HTTPException(status_code=404, detail=f"Site not found: {payload.site}")

    if not site.get("enabled", True):
        raise HTTPException(status_code=403, detail=f"Site is disabled: {payload.site}")

    site_meta = await _site_get_json(
        site_id=payload.site,
        path="/meta",
        timeout=SITE_REQUEST_TIMEOUT,
    ) or {}

    model_id = _pick_model_id(payload, site_meta if isinstance(site_meta, dict) else {})
    if not model_id:
        raise HTTPException(
            status_code=400,
            detail=f"No model selected and site {payload.site} did not provide a default model",
        )

    if not payload.treatments:
        site_treatments = (site_meta if isinstance(site_meta, dict) else {}).get("treatments") or []
        if not site_treatments:
            raise HTTPException(
                status_code=400,
                detail="At least one treatment is required",
            )

    request_payload = {
        "name": payload.name or "",
        "site": payload.site,
        "models": payload.models,
        "treatments": payload.treatments,
        "task": payload.task,
        "parameters": payload.parameters or {},
        "da": payload.da or {},
        "notes": payload.notes or "",
        "submitted_by": payload.submitted_by or "",
    }

    try:
        run = await create_run(
            user_id=payload.user_id,
            username=payload.submitted_by or "",
            site_id=payload.site,
            model_id=model_id,
            task_type=payload.task,
            trigger_type="manual",
            payload=request_payload,
            retention_class="published" if payload.task == "auto_forecast" else "normal",
            output_dir="",
            site_base_url=site.get("base_url", "") or "",
        )
    except ValueError as ex:
        raise HTTPException(status_code=400, detail=str(ex))
    except Exception as ex:
        raise HTTPException(status_code=500, detail=f"Failed to create run: {str(ex)}")

    try:
        final_run = await dispatch_run(run["id"])
    except Exception as ex:
        raise HTTPException(status_code=500, detail=f"Failed to dispatch run: {str(ex)}")

    return {
        "ok": True,
        "run": final_run,
    }


# ---------------------------------------------------------------------
# Run viewer endpoints
# ---------------------------------------------------------------------
@router.get("/runs/{run_id}/manifest")
async def workflow_run_manifest(run_id: str):
    """
    Return one run manifest by proxying the owning Site.
    """
    run = await get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")

    site_id = str(run.get("site_id") or "").strip()
    if not site_id:
        raise HTTPException(status_code=500, detail="Run site_id missing")

    return await _require_site_json(
        site_id=site_id,
        path=f"/runs/{run_id}/manifest",
        timeout=SITE_REQUEST_TIMEOUT,
    )


@router.get("/runs/{run_id}/timeseries")
async def workflow_run_timeseries(
    run_id: str,
    variable: str = Query(..., description="Variable name"),
    model: str = Query("", description="Optional model ID"),
    treatment: str = Query("", description="Optional treatment ID"),
):
    """
    Return one run timeseries by proxying the owning Site.
    """
    run = await get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")

    site_id = str(run.get("site_id") or "").strip()
    if not site_id:
        raise HTTPException(status_code=500, detail="Run site_id missing")

    manifest = await _require_site_json(
        site_id=site_id,
        path=f"/runs/{run_id}/manifest",
        timeout=SITE_REQUEST_TIMEOUT,
    )

    if not isinstance(manifest, dict):
        raise HTTPException(status_code=502, detail="Invalid manifest response from site")

    if not model:
        models = _derive_models_from_manifest(manifest)
        model = models[0] if models else ""

    if not treatment:
        treatments = _derive_treatments_from_manifest(manifest)
        treatment = treatments[0] if treatments else ""

    if not model or not treatment:
        raise HTTPException(
            status_code=400,
            detail="model/treatment missing and cannot be resolved from manifest",
        )

    return await _require_site_json(
        site_id=site_id,
        path=f"/runs/{run_id}/timeseries",
        params={
            "variable": variable,
            "model": model,
            "treatment": treatment,
        },
        timeout=SITE_REQUEST_TIMEOUT,
    )


@router.get("/runs/{run_id}/parameter_summary")
async def workflow_run_parameter_summary(
    run_id: str,
    model: str = Query("", description="Optional model ID"),
    treatment: str = Query("", description="Optional treatment ID"),
):
    """
    Return one run parameter summary by proxying the owning Site.
    """
    run = await get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")

    site_id = str(run.get("site_id") or "").strip()
    if not site_id:
        raise HTTPException(status_code=500, detail="Run site_id missing")

    manifest = await _require_site_json(
        site_id=site_id,
        path=f"/runs/{run_id}/manifest",
        timeout=SITE_REQUEST_TIMEOUT,
    )

    if not isinstance(manifest, dict):
        raise HTTPException(status_code=502, detail="Invalid manifest response from site")

    if not model:
        models = _derive_models_from_manifest(manifest)
        model = models[0] if models else ""

    if not treatment:
        treatments = _derive_treatments_from_manifest(manifest)
        treatment = treatments[0] if treatments else ""

    if not model or not treatment:
        raise HTTPException(
            status_code=400,
            detail="model/treatment missing and cannot be resolved from manifest",
        )

    return await _require_site_json(
        site_id=site_id,
        path=f"/runs/{run_id}/parameter_summary",
        params={
            "model": model,
            "treatment": treatment,
        },
        timeout=SITE_REQUEST_TIMEOUT,
    )


@router.get("/runs/{run_id}/parameters_accepted")
async def workflow_run_parameters_accepted(
    run_id: str,
    model: str = Query("", description="Optional model ID"),
    treatment: str = Query("", description="Optional treatment ID"),
):
    """
    Return one run accepted parameter samples by proxying the owning Site.
    """
    run = await get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")

    site_id = str(run.get("site_id") or "").strip()
    if not site_id:
        raise HTTPException(status_code=500, detail="Run site_id missing")

    manifest = await _require_site_json(
        site_id=site_id,
        path=f"/runs/{run_id}/manifest",
        timeout=SITE_REQUEST_TIMEOUT,
    )

    if not isinstance(manifest, dict):
        raise HTTPException(status_code=502, detail="Invalid manifest response from site")

    if not model:
        models = _derive_models_from_manifest(manifest)
        model = models[0] if models else ""

    if not treatment:
        treatments = _derive_treatments_from_manifest(manifest)
        treatment = treatments[0] if treatments else ""

    if not model or not treatment:
        raise HTTPException(
            status_code=400,
            detail="model/treatment missing and cannot be resolved from manifest",
        )

    return await _require_site_json(
        site_id=site_id,
        path=f"/runs/{run_id}/parameters_accepted",
        params={
            "model": model,
            "treatment": treatment,
        },
        timeout=SITE_REQUEST_TIMEOUT,
    )