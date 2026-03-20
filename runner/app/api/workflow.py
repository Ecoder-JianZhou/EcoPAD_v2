"""
Runner workflow API.

Responsibilities:
- expose workflow-related site metadata to Portal
- accept workflow submissions
- create run records in Runner DB
- dispatch runs to Site through dispatcher
- expose run manifest / timeseries / parameter readers for Portal

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
from app.services.dispatcher import dispatch_run
from app.services.run_manager import create_run, get_run
from app.services.site_registry import registry


router = APIRouter(prefix="/api/workflow", tags=["workflow"])


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
# Schemas
# ---------------------------------------------------------------------
class SubmitIn(BaseModel):
    site: str = Field(..., description="Site ID")
    models: list[str] = Field(default_factory=list, description="Selected model IDs")
    treatments: list[str] = Field(default_factory=list, description="Selected treatments")
    task: str = Field(..., description="Workflow task type")
    parameters: dict[str, Any] = Field(default_factory=dict, description="Parameter overrides")
    da: dict[str, Any] = Field(default_factory=dict, description="Data assimilation settings")
    notes: str = Field(default="", description="Optional notes")
    name: str = Field(default="", description="Optional human-readable job name")
    submitted_by: str = Field(default="", description="Username from Portal")
    user_id: int | None = Field(default=None, description="Optional Portal user ID")


# ---------------------------------------------------------------------
# Normalizers
# ---------------------------------------------------------------------
def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_task(task: Any) -> str:
    text = _normalize_text(task).lower()

    mapping = {
        "simulate": "simulation_without_da",
        "simulation_without_da": "simulation_without_da",
        "simulation without da": "simulation_without_da",
        "simulation_with_da": "simulation_with_da",
        "simulation with da": "simulation_with_da",
        "forecast_with_da": "forecast_with_da",
        "forecast with da": "forecast_with_da",
        "forecast_without_da": "forecast_without_da",
        "forecast without da": "forecast_without_da",
        "auto_forecast": "auto_forecast",
        "auto forecast": "auto_forecast",
        "custom": "custom",
    }

    normalized = mapping.get(text, text)
    if normalized not in (
        "simulation_without_da",
        "simulation_with_da",
        "forecast_with_da",
        "forecast_without_da",
        "auto_forecast",
        "custom",
    ):
        raise HTTPException(status_code=400, detail=f"Unsupported task: {text or '<empty>'}")
    return normalized


def _normalize_output_type(value: Any, default: str = "") -> str:
    text = _normalize_text(value)
    if not text:
        return default

    if text in SUPPORTED_OUTPUT_TYPES:
        return text

    mapped = LEGACY_OUTPUT_ALIASES.get(text)
    if mapped:
        return mapped

    return default


def _resolve_output_types_for_submission(task: str, da: dict[str, Any] | None = None) -> list[str]:
    clean_task = _normalize_task(task)
    da = da or {}

    if clean_task == "simulation_without_da":
        return ["simulation_without_da"]

    if clean_task == "simulation_with_da":
        return ["simulation_with_da"]

    if clean_task == "forecast_with_da":
        return ["forecast_with_da"]

    if clean_task == "forecast_without_da":
        return ["forecast_without_da"]

    if clean_task == "auto_forecast":
        enable_with_da = bool(da.get("auto_forecast_with_da", True))
        enable_without_da = bool(da.get("auto_forecast_without_da", True))

        legacy_requested = _normalize_output_type(da.get("series_type"), default="")
        if legacy_requested == "forecast_with_da":
            return ["auto_forecast_with_da"]
        if legacy_requested == "forecast_without_da":
            return ["auto_forecast_without_da"]

        out: list[str] = []
        if enable_with_da:
            out.append("auto_forecast_with_da")
        if enable_without_da:
            out.append("auto_forecast_without_da")

        if not out:
            out = ["auto_forecast_with_da"]

        return out

    return []


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
    site = registry.get_site(site_id)
    if not site or not site.get("enabled", True):
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
    if payload.models:
        return str(payload.models[0])

    models = (site_meta or {}).get("models") or []
    if models:
        return str(models[0])

    return ""


def _derive_models_from_manifest(manifest: dict[str, Any]) -> list[str]:
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


def _derive_treatments_from_manifest(manifest: dict[str, Any], model: str = "") -> list[str]:
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

    add(req.get("treatment"))

    outputs = manifest.get("outputs") or {}
    index_obj = outputs.get("index") or {}
    if isinstance(index_obj, dict):
        if model and isinstance(index_obj.get(model), dict):
            for treatment in index_obj[model].keys():
                add(treatment)
        else:
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


def _derive_output_types_from_manifest(
    manifest: dict[str, Any],
    *,
    model: str = "",
    treatment: str = "",
) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()

    def add(v: Any) -> None:
        s = _normalize_output_type(v, default="")
        if s and s not in seen:
            seen.add(s)
            out.append(s)

    req = manifest.get("request") or {}
    expected = req.get("expected_output_types")
    if isinstance(expected, list):
        for item in expected:
            add(item)

    outputs = manifest.get("outputs") or {}
    index_obj = outputs.get("index") or {}
    if isinstance(index_obj, dict) and model and treatment:
        treatment_block = index_obj.get(model, {}).get(treatment)
        if isinstance(treatment_block, dict):
            for output_type in treatment_block.keys():
                add(output_type)

    artifacts = manifest.get("artifacts") or []
    if isinstance(artifacts, list):
        for item in artifacts:
            if not isinstance(item, dict):
                continue
            if str(item.get("artifact_type") or "").strip() != "timeseries":
                continue

            am = str(item.get("model_id") or "").strip()
            at = str(item.get("treatment") or "").strip()
            if model and am != model:
                continue
            if treatment and at != treatment:
                continue

            add(item.get("output_type") or item.get("series_type"))

    return out


def _collect_tasks_from_site_meta(site_meta: dict[str, Any]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()

    model_meta = site_meta.get("model_meta") or {}
    if not isinstance(model_meta, dict):
        return out

    for _, block in model_meta.items():
        if not isinstance(block, dict):
            continue
        tasks = block.get("tasks") or []
        if not isinstance(tasks, list):
            continue
        for task in tasks:
            t = _normalize_task(task)
            if t and t not in seen:
                seen.add(t)
                out.append(t)

    return out


def _task_label(task: str) -> str:
    labels = {
        "simulation_without_da": "Simulation without DA",
        "simulation_with_da": "Simulation with DA",
        "forecast_with_da": "Forecast with DA",
        "forecast_without_da": "Forecast without DA",
        "auto_forecast": "Auto Forecast",
        "custom": "Custom",
    }
    return labels.get(task, task)


# ---------------------------------------------------------------------
# Workflow metadata endpoints
# ---------------------------------------------------------------------
@router.get("/sites")
async def workflow_sites():
    return {"sites": registry.list_site_ids(enabled_only=True)}


@router.get("/site_meta")
async def workflow_site_meta(site: str = Query(..., description="Site ID")):
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
    params = {"model": model} if model else None
    return await _require_site_json(
        site_id=site,
        path="/params/meta",
        params=params,
        timeout=SITE_REQUEST_TIMEOUT,
    )


@router.get("/meta")
async def workflow_meta():
    sites = registry.list_site_ids(enabled_only=True)

    models: list[str] = []
    treatments: list[str] = []
    output_types: list[str] = []
    tasks: list[dict[str, str]] = []

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
            output_types = _merge_unique_strs(
                [],
                site_meta.get("output_types") or site_meta.get("series_types") or [],
            )

            enabled_tasks = _collect_tasks_from_site_meta(site_meta)
            tasks = [{"key": t, "label": _task_label(t)} for t in enabled_tasks]

    if not tasks:
        tasks = [
            {"key": "simulation_without_da", "label": "Simulation without DA"},
            {"key": "simulation_with_da", "label": "Simulation with DA"},
            {"key": "forecast_with_da", "label": "Forecast with DA"},
            {"key": "forecast_without_da", "label": "Forecast without DA"},
            {"key": "auto_forecast", "label": "Auto Forecast"},
        ]

    return {
        "sites": sites,
        "models": models,
        "treatments": treatments,
        "output_types": output_types,
        "series_types": output_types,
        "tasks": tasks,
    }


# ---------------------------------------------------------------------
# Submit workflow job
# ---------------------------------------------------------------------
@router.post("/submit")
async def submit_workflow(payload: SubmitIn):
    site = registry.get_site(payload.site)
    if not site:
        raise HTTPException(status_code=404, detail=f"Site not found: {payload.site}")

    if not site.get("enabled", True):
        raise HTTPException(status_code=403, detail=f"Site is disabled: {payload.site}")

    normalized_task = _normalize_task(payload.task)

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

    treatments = payload.treatments or []
    if not treatments:
        site_treatments = (site_meta if isinstance(site_meta, dict) else {}).get("treatments") or []
        if not site_treatments:
            raise HTTPException(status_code=400, detail="At least one treatment is required")
        treatments = list(site_treatments[:1])

    expected_output_types = _resolve_output_types_for_submission(normalized_task, payload.da)

    request_payload = {
        "name": payload.name or "",
        "site": payload.site,
        "models": payload.models or ([model_id] if model_id else []),
        "treatments": treatments,
        "task": normalized_task,
        "parameters": payload.parameters or {},
        "da": payload.da or {},
        "notes": payload.notes or "",
        "submitted_by": payload.submitted_by or "",
        "expected_output_types": expected_output_types,
    }

    if expected_output_types:
        request_payload["output_type"] = expected_output_types[0]
        request_payload["series_type"] = expected_output_types[0]

    try:
        run = await create_run(
            user_id=payload.user_id,
            username=payload.submitted_by or "",
            site_id=payload.site,
            model_id=model_id,
            task_type=normalized_task,
            trigger_type="manual",
            payload=request_payload,
            retention_class="published" if normalized_task == "auto_forecast" else "normal",
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
    output_type: str = Query("", description="Optional output type"),
    series_type: str = Query("", description="Backward-compatible alias"),
):
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
        treatments = _derive_treatments_from_manifest(manifest, model=model)
        treatment = treatments[0] if treatments else ""

    if not model or not treatment:
        raise HTTPException(status_code=400, detail="model/treatment missing and cannot be resolved from manifest")

    resolved_output_type = _normalize_output_type(output_type or series_type, default="")
    if not resolved_output_type:
        available_output_types = _derive_output_types_from_manifest(
            manifest,
            model=model,
            treatment=treatment,
        )
        resolved_output_type = available_output_types[0] if available_output_types else "forecast_with_da"

    return await _require_site_json(
        site_id=site_id,
        path=f"/runs/{run_id}/timeseries",
        params={
            "variable": variable,
            "model": model,
            "treatment": treatment,
            "output_type": resolved_output_type,
            "series_type": resolved_output_type,
        },
        timeout=SITE_REQUEST_TIMEOUT,
    )


@router.get("/runs/{run_id}/parameter_summary")
async def workflow_run_parameter_summary(
    run_id: str,
    model: str = Query("", description="Optional model ID"),
    treatment: str = Query("", description="Optional treatment ID"),
    output_type: str = Query("", description="Optional output type"),
    series_type: str = Query("", description="Backward-compatible alias"),
):
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
        treatments = _derive_treatments_from_manifest(manifest, model=model)
        treatment = treatments[0] if treatments else ""

    if not model or not treatment:
        raise HTTPException(status_code=400, detail="model/treatment missing and cannot be resolved from manifest")

    resolved_output_type = _normalize_output_type(output_type or series_type, default="")
    if not resolved_output_type:
        available_output_types = _derive_output_types_from_manifest(
            manifest,
            model=model,
            treatment=treatment,
        )
        resolved_output_type = available_output_types[0] if available_output_types else ""

    return await _require_site_json(
        site_id=site_id,
        path=f"/runs/{run_id}/parameter_summary",
        params={
            "model": model,
            "treatment": treatment,
            "output_type": resolved_output_type,
            "series_type": resolved_output_type,
        },
        timeout=SITE_REQUEST_TIMEOUT,
    )


@router.get("/runs/{run_id}/parameters_accepted")
async def workflow_run_parameters_accepted(
    run_id: str,
    model: str = Query("", description="Optional model ID"),
    treatment: str = Query("", description="Optional treatment ID"),
    output_type: str = Query("", description="Optional output type"),
    series_type: str = Query("", description="Backward-compatible alias"),
):
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
        treatments = _derive_treatments_from_manifest(manifest, model=model)
        treatment = treatments[0] if treatments else ""

    if not model or not treatment:
        raise HTTPException(status_code=400, detail="model/treatment missing and cannot be resolved from manifest")

    resolved_output_type = _normalize_output_type(output_type or series_type, default="")
    if not resolved_output_type:
        available_output_types = _derive_output_types_from_manifest(
            manifest,
            model=model,
            treatment=treatment,
        )
        resolved_output_type = available_output_types[0] if available_output_types else ""

    return await _require_site_json(
        site_id=site_id,
        path=f"/runs/{run_id}/parameters_accepted",
        params={
            "model": model,
            "treatment": treatment,
            "output_type": resolved_output_type,
            "series_type": resolved_output_type,
        },
        timeout=SITE_REQUEST_TIMEOUT,
    )


@router.get("/runs/{run_id}/parameter_best")
async def workflow_run_parameter_best(
    run_id: str,
    model: str = Query("", description="Optional model ID"),
    treatment: str = Query("", description="Optional treatment ID"),
    output_type: str = Query("", description="Optional output type"),
    series_type: str = Query("", description="Backward-compatible alias"),
):
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
        treatments = _derive_treatments_from_manifest(manifest, model=model)
        treatment = treatments[0] if treatments else ""

    if not model or not treatment:
        raise HTTPException(status_code=400, detail="model/treatment missing and cannot be resolved from manifest")

    resolved_output_type = _normalize_output_type(output_type or series_type, default="")
    if not resolved_output_type:
        available_output_types = _derive_output_types_from_manifest(
            manifest,
            model=model,
            treatment=treatment,
        )
        resolved_output_type = available_output_types[0] if available_output_types else ""

    return await _require_site_json(
        site_id=site_id,
        path=f"/runs/{run_id}/parameter_best",
        params={
            "model": model,
            "treatment": treatment,
            "output_type": resolved_output_type,
            "series_type": resolved_output_type,
        },
        timeout=SITE_REQUEST_TIMEOUT,
    )