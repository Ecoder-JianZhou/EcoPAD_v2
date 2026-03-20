from __future__ import annotations

from typing import Any

import httpx
from fastapi import APIRouter, HTTPException, Query

from app.core.settings import SITE_REQUEST_TIMEOUT
from app.services.forecast_registry import (
    get_forecast_summary,
    get_latest_auto_forecast_run_for_series,
    get_latest_forecast,
    get_parameter_hist_artifact_for_run,
    list_latest_models,
    list_latest_treatments,
    list_latest_variables,
    list_parameter_history_for_schedule,
)
from app.services.run_manager import (
    get_first_run_output,
    get_run,
    list_runs_catalog,
)
from app.services.site_registry import registry

router = APIRouter(prefix="/api/forecast", tags=["forecast"])


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
# Generic helpers
# ---------------------------------------------------------------------
def _safe_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _parse_csv_arg(value: str) -> list[str]:
    if not value:
        return []

    out: list[str] = []
    for item in value.split(","):
        v = str(item or "").strip()
        if v:
            out.append(v)
    return out


def _coerce_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


def _normalize_task_type(value: Any) -> str:
    text = str(value or "").strip().lower()

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
    }
    return mapping.get(text, text)


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


def _normalize_catalog_output_type(value: Any, task_type: Any = "") -> str:
    raw = str(value or "").strip()
    if raw:
        return _normalize_output_type(raw)

    task = _normalize_task_type(task_type)
    if task == "auto_forecast":
        return "auto_forecast_with_da"
    if task == "simulation_without_da":
        return "simulation_without_da"
    if task == "simulation_with_da":
        return "simulation_with_da"
    if task == "forecast_with_da":
        return "forecast_with_da"
    if task == "forecast_without_da":
        return "forecast_without_da"

    return ""


def _output_type_to_registry_series_type(output_type: str) -> str:
    output_type = _normalize_output_type(output_type, default="")
    if output_type == "auto_forecast_with_da":
        return "forecast_with_da"
    if output_type == "auto_forecast_without_da":
        return "forecast_without_da"
    return output_type


def _require_site_enabled(site_id: str) -> dict[str, Any]:
    site = registry.get_site(site_id)
    if not site:
        raise HTTPException(status_code=404, detail=f"Site not found: {site_id}")
    if not site.get("enabled", True):
        raise HTTPException(status_code=403, detail=f"Site is disabled: {site_id}")
    return site


def _get_site_base_url(site_id: str) -> str:
    site = _require_site_enabled(site_id)
    base_url = str(site.get("base_url") or "").rstrip("/")
    if not base_url:
        raise HTTPException(status_code=500, detail=f"Site base_url missing: {site_id}")
    return base_url


def _format_run_label(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if "T" in text:
        text = text.replace("T", " ")
    if "." in text:
        text = text.split(".", 1)[0]
    if len(text) >= 16:
        return text[:16]
    return text


def _extract_parameter_time(parameter: dict[str, Any] | None, row: dict[str, Any] | None = None) -> str:
    """
    Prefer parameter-specific analysis/data time over run submission time.
    """
    parameter = parameter if isinstance(parameter, dict) else {}
    row = row if isinstance(row, dict) else {}

    for key in (
        "data_time",
        "data_end_time",
        "analysis_time",
        "obs_end_time",
        "time",
    ):
        value = str(parameter.get(key) or row.get(key) or "").strip()
        if value:
            return value

    return ""


# ---------------------------------------------------------------------
# Response shapers
# ---------------------------------------------------------------------
def _shape_multi_series_response(*, units: str = "", items: list[dict[str, Any]]) -> dict[str, Any]:
    return {"units": units or "", "series": items}


def _shape_parameter_history_response(
    *,
    site_id: str,
    param_id: str,
    output_type: str,
    series_items: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "site_id": site_id,
        "param": param_id,
        "output_type": output_type,
        "series_type": output_type,
        "series": series_items,
    }


def _shape_latest_params_response(
    *,
    site_id: str,
    model_id: str,
    treatment: str,
    variable: str,
    output_type: str,
    forecast_row: dict[str, Any] | None,
    run_row: dict[str, Any] | None,
    artifact_row: dict[str, Any] | None,
    summary: dict[str, Any] | None,
) -> dict[str, Any]:
    source_run_id = ""
    scheduled_task_id = None

    if isinstance(forecast_row, dict):
        source_run_id = str(forecast_row.get("source_run_id") or "").strip()

    if isinstance(run_row, dict):
        scheduled_task_id = run_row.get("scheduled_task_id")

    return {
        "site_id": site_id,
        "model_id": model_id,
        "treatment": treatment,
        "variable": variable,
        "output_type": output_type,
        "series_type": output_type,
        "source_run_id": source_run_id,
        "scheduled_task_id": scheduled_task_id,
        "forecast": forecast_row or None,
        "artifact": artifact_row or None,
        "summary": summary or None,
    }


def _shape_runs_response(*, items: list[dict[str, Any]]) -> dict[str, Any]:
    return {"runs": items}


# ---------------------------------------------------------------------
# Parameter extractors
# ---------------------------------------------------------------------
def _extract_best_parameter_value(best_obj: dict[str, Any] | None, param_id: str) -> float | None:
    if not isinstance(best_obj, dict):
        return None

    params = best_obj.get("parameters") or []
    if not isinstance(params, list):
        return None

    for item in params:
        if not isinstance(item, dict):
            continue
        if str(item.get("id") or "").strip() == str(param_id).strip():
            return _coerce_float(item.get("value"))

    return None


def _extract_parameter_from_summary(
    summary_obj: dict[str, Any] | None,
    param_id: str,
) -> dict[str, Any] | None:
    if not isinstance(summary_obj, dict):
        return None

    summary_block = summary_obj.get("summary")
    params = summary_block.get("parameters") if isinstance(summary_block, dict) else None
    if not isinstance(params, list):
        params = summary_obj.get("parameters")
    if not isinstance(params, list):
        return None

    for item in params:
        if not isinstance(item, dict):
            continue
        if str(item.get("id") or "").strip() == str(param_id).strip():
            return item

    return None


# ---------------------------------------------------------------------
# Site HTTP helpers
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

    base_url = str(site.get("base_url") or "").rstrip("/")
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
    base_url = _get_site_base_url(site_id)

    try:
        async with httpx.AsyncClient(timeout=timeout or SITE_REQUEST_TIMEOUT) as client:
            resp = await client.get(f"{base_url}{path}", params=params)
            if resp.status_code != 200:
                raise HTTPException(
                    status_code=502,
                    detail=f"Site request failed: GET {path} returned {resp.status_code}",
                )
            return resp.json()
    except HTTPException:
        raise
    except Exception as ex:
        raise HTTPException(status_code=502, detail=f"Site request failed: {str(ex)}")


async def _get_site_meta(site_id: str) -> dict[str, Any]:
    raw = await _site_get_json(site_id=site_id, path="/meta", timeout=SITE_REQUEST_TIMEOUT)
    return raw if isinstance(raw, dict) else {}


# ---------------------------------------------------------------------
# Forecast/query helpers
# ---------------------------------------------------------------------
async def _get_site_summary(site_id: str) -> dict[str, Any]:
    _require_site_enabled(site_id)
    return await get_forecast_summary(site_id)


async def _resolve_requested_models_and_treatments(
    *,
    site_id: str,
    models: str,
    treatments: str,
) -> tuple[list[str], list[str], dict[str, Any]]:
    summary = await _get_site_summary(site_id)
    requested_models = _parse_csv_arg(models)
    requested_treatments = _parse_csv_arg(treatments)

    if not requested_models:
        requested_models = _safe_list(summary.get("models"))[:1]
    if not requested_treatments:
        requested_treatments = _safe_list(summary.get("treatments"))[:1]

    return requested_models, requested_treatments, summary


async def _find_latest_forecast_for_output_type(
    *,
    site_id: str,
    model_id: str,
    treatment: str,
    variable: str,
    output_type: str,
    site_summary: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    registry_series_type = _output_type_to_registry_series_type(output_type)

    if variable:
        latest = await get_latest_forecast(
            site_id=site_id,
            model_id=model_id,
            variable=variable,
            treatment=treatment,
            series_type=registry_series_type,
            published_only=True,
        )
        if latest:
            return latest

    variables = _safe_list((site_summary or {}).get("variables"))
    for var_name in variables:
        latest = await get_latest_forecast(
            site_id=site_id,
            model_id=model_id,
            variable=str(var_name),
            treatment=treatment,
            series_type=registry_series_type,
            published_only=True,
        )
        if latest:
            return latest

    return None


async def _get_run_parameter_summary(
    *,
    site_id: str,
    run_id: str,
    model_id: str,
    treatment: str,
    output_type: str = "",
) -> dict[str, Any] | None:
    raw = await _site_get_json(
        site_id=site_id,
        path=f"/runs/{run_id}/parameter_summary",
        params={
            "model": model_id,
            "treatment": treatment,
            "output_type": output_type,
            "series_type": output_type,
        },
        timeout=SITE_REQUEST_TIMEOUT,
    )
    return raw if isinstance(raw, dict) else None


async def _get_run_parameter_best(
    *,
    site_id: str,
    run_id: str,
    model_id: str,
    treatment: str,
    output_type: str = "",
) -> dict[str, Any] | None:
    raw = await _site_get_json(
        site_id=site_id,
        path=f"/runs/{run_id}/parameter_best",
        params={
            "model": model_id,
            "treatment": treatment,
            "output_type": output_type,
            "series_type": output_type,
        },
        timeout=SITE_REQUEST_TIMEOUT,
    )
    return raw if isinstance(raw, dict) else None


# ---------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------
@router.get("/sites")
async def forecast_sites() -> dict[str, Any]:
    return {"sites": registry.list_site_ids(enabled_only=True)}


@router.get("/{site_id}/meta")
async def forecast_meta(site_id: str) -> dict[str, Any]:
    _require_site_enabled(site_id)

    site_meta = await _get_site_meta(site_id)
    summary = await _get_site_summary(site_id)

    registry_models = await list_latest_models(site_id)
    registry_variables = await list_latest_variables(site_id)
    registry_treatments = await list_latest_treatments(site_id)

    models = site_meta.get("models")
    if not isinstance(models, list) or not models:
        models = registry_models

    variables = site_meta.get("variables")
    if not isinstance(variables, list) or not variables:
        variables = registry_variables

    treatments = site_meta.get("treatments")
    if not isinstance(treatments, list) or not treatments:
        treatments = registry_treatments

    output_types = site_meta.get("output_types")
    if not isinstance(output_types, list) or not output_types:
        output_types = _safe_list(summary.get("series_types"))

    site_meta["models"] = models or []
    site_meta["variables"] = variables or []
    site_meta["treatments"] = treatments or []
    site_meta["output_types"] = output_types or []
    site_meta["series_types"] = output_types or []
    return site_meta


@router.get("/{site_id}/summary")
async def forecast_summary(site_id: str) -> dict[str, Any]:
    return await _get_site_summary(site_id)


@router.get("/{site_id}/runs")
async def forecast_runs(
    site_id: str,
    models: str = Query(""),
    treatments: str = Query(""),
    variable: str = Query(""),
    task_type: str = Query(""),
    scheduled_task_id: int | None = Query(default=None),
    output_type: str = Query(default="forecast_with_da"),
    series_type: str = Query(default=""),
    limit: int = Query(default=200, ge=1, le=1000),
) -> dict[str, Any]:
    _require_site_enabled(site_id)

    resolved_output_type = _normalize_output_type(output_type or series_type)
    resolved_task_type = _normalize_task_type(task_type)

    requested_models = _parse_csv_arg(models)
    requested_treatments = _parse_csv_arg(treatments)
    clean_variable = str(variable or "").strip()

    rows = await list_runs_catalog(
        site_id=site_id,
        models=requested_models,
        treatments=requested_treatments,
        variable=clean_variable,
        task_type=resolved_task_type,
        scheduled_task_id=scheduled_task_id,
        limit=limit,
    )

    filtered_rows: list[dict[str, Any]] = []
    for row in rows:
        row_output_type = _normalize_catalog_output_type(
            row.get("catalog_output_type") or row.get("output_type") or row.get("series_type"),
            row.get("task_type"),
        )

        if row_output_type and row_output_type != resolved_output_type:
            if _normalize_task_type(row.get("task_type")) == "auto_forecast":
                continue

        filtered_rows.append(row)

    latest_run_ids: set[str] = set()
    if clean_variable:
        seen_pairs: set[tuple[str, str]] = set()
        for row in filtered_rows:
            model_id = str(row.get("catalog_model_id") or row.get("model_id") or "").strip()
            treatment = str(row.get("catalog_treatment") or "").strip()
            if not model_id or not treatment:
                continue

            key = (model_id, treatment)
            if key in seen_pairs:
                continue
            seen_pairs.add(key)

            latest = await _find_latest_forecast_for_output_type(
                site_id=site_id,
                model_id=model_id,
                treatment=treatment,
                variable=clean_variable,
                output_type=resolved_output_type,
            )
            if latest:
                source_run_id = str(latest.get("source_run_id") or "").strip()
                if source_run_id:
                    latest_run_ids.add(source_run_id)

    items: list[dict[str, Any]] = []
    for row in filtered_rows:
        run_id = str(row.get("id") or "").strip()
        run_time = str(
            row.get("finished_at")
            or row.get("started_at")
            or row.get("created_at")
            or ""
        ).strip()

        row_output_type = _normalize_catalog_output_type(
            row.get("catalog_output_type") or row.get("output_type") or row.get("series_type"),
            row.get("task_type"),
        ) or resolved_output_type

        items.append(
            {
                "run_id": run_id,
                "label": _format_run_label(run_time) or run_id,
                "time": run_time,
                "created_at": row.get("created_at"),
                "started_at": row.get("started_at"),
                "finished_at": row.get("finished_at"),
                "updated_at": row.get("updated_at"),
                "status": row.get("status"),
                "scheduled_task_id": row.get("scheduled_task_id"),
                "triggered_by": row.get("trigger_type") or "",
                "site_id": row.get("site_id") or site_id,
                "model_id": row.get("catalog_model_id") or row.get("model_id") or "",
                "treatment": row.get("catalog_treatment") or "",
                "variable": clean_variable or row.get("catalog_variable") or "",
                "task_type": _normalize_task_type(row.get("task_type") or ""),
                "output_type": row_output_type,
                "series_type": row_output_type,
                "is_latest_published": run_id in latest_run_ids,
            }
        )

    return _shape_runs_response(items=items)


@router.get("/{site_id}/runs/{run_id}/timeseries")
async def forecast_run_timeseries(
    site_id: str,
    run_id: str,
    variable: str = Query(...),
    model: str = Query(...),
    treatment: str = Query(...),
    output_type: str = Query(default=""),
    series_type: str = Query(default="forecast_with_da"),
) -> dict[str, Any] | list[Any]:
    _require_site_enabled(site_id)
    resolved_output_type = _normalize_output_type(output_type or series_type)

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


@router.get("/{site_id}/data")
async def forecast_data(
    site_id: str,
    variable: str = Query(...),
    models: str = Query(""),
    treatments: str = Query(""),
    output_type: str = Query(default=""),
    series_type: str = Query(default="forecast_with_da"),
    show_obs: bool = Query(default=False),
) -> dict[str, Any]:
    _require_site_enabled(site_id)

    resolved_output_type = _normalize_output_type(output_type or series_type)

    requested_models, requested_treatments, summary = await _resolve_requested_models_and_treatments(
        site_id=site_id,
        models=models,
        treatments=treatments,
    )

    if not requested_models or not requested_treatments:
        return {"units": "", "series": []}

    out_series: list[dict[str, Any]] = []
    units = ""

    for model_id in requested_models:
        for treatment in requested_treatments:
            latest = await _find_latest_forecast_for_output_type(
                site_id=site_id,
                model_id=model_id,
                treatment=treatment,
                variable=variable,
                output_type=resolved_output_type,
                site_summary=summary,
            )
            if not latest:
                continue

            source_run_id = str(latest.get("source_run_id") or "").strip()
            if not source_run_id:
                continue

            site_data = await _site_get_json(
                site_id=site_id,
                path=f"/runs/{source_run_id}/timeseries",
                params={
                    "variable": variable,
                    "model": model_id,
                    "treatment": treatment,
                    "output_type": resolved_output_type,
                    "series_type": resolved_output_type,
                },
                timeout=SITE_REQUEST_TIMEOUT,
            )
            if not isinstance(site_data, dict):
                continue

            site_units = str(site_data.get("units") or "")
            if site_units and not units:
                units = site_units

            raw_series = _safe_list(site_data.get("series"))
            if not raw_series:
                continue

            first = raw_series[0] if isinstance(raw_series[0], dict) else {}
            out_series.append(
                {
                    "key": f"{model_id}||{treatment}||{resolved_output_type}",
                    "kind": "forecast",
                    "output_type": resolved_output_type,
                    "series_type": resolved_output_type,
                    "model": model_id,
                    "treatment": treatment,
                    "time": first.get("time") or [],
                    "mean": first.get("mean") or [],
                    "lo": first.get("lo") or first.get("q05") or [],
                    "hi": first.get("hi") or first.get("q95") or [],
                    "source_run_id": source_run_id,
                }
            )

            if show_obs:
                obs_data = await _site_get_json(
                    site_id=site_id,
                    path="/obs",
                    params={
                        "variable": variable,
                        "treatment": treatment,
                        "model": model_id,
                    },
                    timeout=SITE_REQUEST_TIMEOUT,
                )
                if isinstance(obs_data, dict):
                    if isinstance(obs_data.get("points"), list):
                        for idx, item in enumerate(obs_data["points"]):
                            if not isinstance(item, dict):
                                continue
                            out_series.append(
                                {
                                    "key": f"{model_id}||{treatment}||obs||{idx}",
                                    "kind": "observation",
                                    "output_type": resolved_output_type,
                                    "series_type": resolved_output_type,
                                    "model": model_id,
                                    "treatment": item.get("treatment") or treatment,
                                    "time": item.get("time") or [],
                                    "value": item.get("value") or [],
                                    "std": item.get("std") or [],
                                }
                            )
                    else:
                        out_series.append(
                            {
                                "key": f"{model_id}||{treatment}||obs",
                                "kind": "observation",
                                "output_type": resolved_output_type,
                                "series_type": resolved_output_type,
                                "model": model_id,
                                "treatment": obs_data.get("treatment") or treatment,
                                "time": obs_data.get("time") or [],
                                "value": obs_data.get("value") or [],
                                "std": obs_data.get("std") or [],
                            }
                        )

    return _shape_multi_series_response(units=units, items=out_series)


@router.get("/{site_id}/obs")
async def forecast_obs(
    site_id: str,
    variable: str = Query(...),
    models: str = Query(""),
    treatments: str = Query(""),
) -> dict[str, Any]:
    _require_site_enabled(site_id)

    requested_models = _parse_csv_arg(models)
    requested_treatments = _parse_csv_arg(treatments)

    summary = await _get_site_summary(site_id)

    if not requested_models:
        requested_models = _safe_list(summary.get("models"))[:1]

    if not requested_treatments:
        requested_treatments = _safe_list(summary.get("treatments"))[:1]

    if not requested_models or not requested_treatments:
        return {"points": []}

    points: list[dict[str, Any]] = []

    for model_id in requested_models:
        for treatment in requested_treatments:
            obs_data = await _site_get_json(
                site_id=site_id,
                path="/obs",
                params={
                    "variable": variable,
                    "treatment": treatment,
                    "model": model_id,
                },
                timeout=SITE_REQUEST_TIMEOUT,
            )
            if not isinstance(obs_data, dict):
                continue

            if isinstance(obs_data.get("points"), list):
                for item in obs_data["points"]:
                    if not isinstance(item, dict):
                        continue
                    points.append(
                        {
                            "model": model_id,
                            "treatment": item.get("treatment") or treatment,
                            "time": item.get("time") or [],
                            "value": item.get("value") or [],
                            "std": item.get("std") or [],
                        }
                    )
            else:
                points.append(
                    {
                        "model": model_id,
                        "treatment": obs_data.get("treatment") or treatment,
                        "time": obs_data.get("time") or [],
                        "value": obs_data.get("value") or [],
                        "std": obs_data.get("std") or [],
                    }
                )

    return {"points": points}


@router.get("/{site_id}/params/meta")
async def forecast_params_meta(
    site_id: str,
    model: str = Query(""),
) -> dict[str, Any] | list[Any]:
    params = {"model": model} if model else None
    return await _require_site_json(
        site_id=site_id,
        path="/params/meta",
        params=params,
        timeout=SITE_REQUEST_TIMEOUT,
    )


@router.get("/{site_id}/params/latest")
async def forecast_params_latest(
    site_id: str,
    model: str = Query(...),
    treatment: str = Query(...),
    variable: str = Query(...),
    output_type: str = Query(default=""),
    series_type: str = Query(default="forecast_with_da"),
) -> dict[str, Any]:
    _require_site_enabled(site_id)
    resolved_output_type = _normalize_output_type(output_type or series_type)

    latest = await get_latest_forecast(
        site_id=site_id,
        model_id=model,
        variable=variable,
        treatment=treatment,
        series_type=_output_type_to_registry_series_type(resolved_output_type),
        published_only=True,
    )
    if not latest:
        return _shape_latest_params_response(
            site_id=site_id,
            model_id=model,
            treatment=treatment,
            variable=variable,
            output_type=resolved_output_type,
            forecast_row=None,
            run_row=None,
            artifact_row=None,
            summary=None,
        )

    source_run_id = str(latest.get("source_run_id") or "").strip()
    if not source_run_id:
        return _shape_latest_params_response(
            site_id=site_id,
            model_id=model,
            treatment=treatment,
            variable=variable,
            output_type=resolved_output_type,
            forecast_row=latest,
            run_row=None,
            artifact_row=None,
            summary=None,
        )

    latest_run = await get_latest_auto_forecast_run_for_series(
        site_id=site_id,
        model_id=model,
        treatment=treatment,
        variable=variable,
        series_type=_output_type_to_registry_series_type(resolved_output_type),
    )
    artifact = await get_first_run_output(source_run_id, "parameter_summary", model_id=model)
    summary = await _get_run_parameter_summary(
        site_id=site_id,
        run_id=source_run_id,
        model_id=model,
        treatment=treatment,
        output_type=resolved_output_type,
    )

    return _shape_latest_params_response(
        site_id=site_id,
        model_id=model,
        treatment=treatment,
        variable=variable,
        output_type=resolved_output_type,
        forecast_row=latest,
        run_row=latest_run,
        artifact_row=artifact,
        summary=summary,
    )


@router.get("/{site_id}/params/history")
async def forecast_params_history(
    site_id: str,
    param: str = Query(...),
    models: str = Query(""),
    treatments: str = Query(""),
    variable: str = Query(""),
    output_type: str = Query(default=""),
    series_type: str = Query(default="forecast_with_da"),
) -> dict[str, Any]:
    _require_site_enabled(site_id)
    resolved_output_type = _normalize_output_type(output_type or series_type)

    requested_models, requested_treatments, _site_summary = await _resolve_requested_models_and_treatments(
        site_id=site_id,
        models=models,
        treatments=treatments,
    )

    if not requested_models or not requested_treatments:
        return _shape_parameter_history_response(
            site_id=site_id,
            param_id=param,
            output_type=resolved_output_type,
            series_items=[],
        )

    out_series: list[dict[str, Any]] = []
    seen_keys: set[tuple[str, str, str, str]] = set()

    for model_id in requested_models:
        for treatment in requested_treatments:
            # 直接查这一组条件下的所有历史 runs
            rows = await list_runs_catalog(
                site_id=site_id,
                models=[model_id],
                treatments=[treatment],
                variable=variable or "",
                task_type="",   # 不强制限定，让 forecast / auto_forecast / simulation_with_da 都能进来
                limit=1000,
            )

            for row in rows:
                run_id = str(row.get("id") or "").strip()
                if not run_id:
                    continue

                row_output_type = _normalize_catalog_output_type(
                    row.get("catalog_output_type") or row.get("output_type") or row.get("series_type"),
                    row.get("task_type"),
                )

                # 只保留当前页面请求的 output_type
                if row_output_type != resolved_output_type:
                    continue

                dedup_key = (run_id, model_id, treatment, resolved_output_type)
                if dedup_key in seen_keys:
                    continue
                seen_keys.add(dedup_key)

                summary_obj = await _get_run_parameter_summary(
                    site_id=site_id,
                    run_id=run_id,
                    model_id=model_id,
                    treatment=treatment,
                    output_type=resolved_output_type,
                )

                parameter = _extract_parameter_from_summary(summary_obj, param)

                best_value = None
                q05 = None
                q95 = None

                if isinstance(parameter, dict):
                    best_value = _coerce_float(
                        parameter.get("value")
                        or parameter.get("optimized")
                        or parameter.get("mean")
                        or parameter.get("map")
                    )
                    q05 = _coerce_float(
                        parameter.get("p05")
                        or parameter.get("q05")
                        or parameter.get("accepted_min")
                        or parameter.get("minimum")
                    )
                    q95 = _coerce_float(
                        parameter.get("p95")
                        or parameter.get("q95")
                        or parameter.get("accepted_max")
                        or parameter.get("maximum")
                    )

                if best_value is None:
                    best_obj = await _get_run_parameter_best(
                        site_id=site_id,
                        run_id=run_id,
                        model_id=model_id,
                        treatment=treatment,
                        output_type=resolved_output_type,
                    )
                    best_value = _extract_best_parameter_value(best_obj, param)

                if best_value is None:
                    continue

                run_row = await get_run(run_id)
                if not run_row:
                    continue

                # 时间优先级：
                # 1) parameter 本身携带的时间
                # 2) summary 顶层时间
                # 3) run finished/updated/created
                time_value = str(
                    (
                        parameter.get("data_time")
                        if isinstance(parameter, dict) else ""
                    )
                    or (
                        parameter.get("analysis_time")
                        if isinstance(parameter, dict) else ""
                    )
                    or (
                        parameter.get("time")
                        if isinstance(parameter, dict) else ""
                    )
                    or (
                        summary_obj.get("data_time")
                        if isinstance(summary_obj, dict) else ""
                    )
                    or (
                        summary_obj.get("analysis_time")
                        if isinstance(summary_obj, dict) else ""
                    )
                    or run_row.get("finished_at")
                    or run_row.get("updated_at")
                    or run_row.get("created_at")
                    or ""
                ).strip()

                if not time_value:
                    continue

                out_series.append(
                    {
                        "model": model_id,
                        "treatment": treatment,
                        "output_type": resolved_output_type,
                        "series_type": resolved_output_type,
                        "run_id": run_id,
                        "time": time_value,
                        "value": best_value,
                        "q05": q05,
                        "q95": q95,
                    }
                )

    out_series.sort(
        key=lambda x: str(x.get("time") or "")
    )

    return _shape_parameter_history_response(
        site_id=site_id,
        param_id=param,
        output_type=resolved_output_type,
        series_items=out_series,
    )


@router.get("/{site_id}/params/hist")
async def forecast_params_hist(
    site_id: str,
    run_id: str = Query(...),
    models: str = Query(""),
    treatments: str = Query(""),
    params: str = Query(""),
) -> dict[str, Any] | list[Any]:
    _require_site_enabled(site_id)

    requested_models = _parse_csv_arg(models)
    model_id = requested_models[0] if requested_models else ""

    if model_id:
        artifact = await get_parameter_hist_artifact_for_run(
            run_id=run_id,
            model_id=model_id,
            artifact_type="parameter_posterior",
        )
        if not artifact:
            artifact = await get_parameter_hist_artifact_for_run(
                run_id=run_id,
                model_id=model_id,
                artifact_type="parameter_hist",
            )
        if artifact:
            return {
                "run_id": run_id,
                "model_id": model_id,
                "params": _parse_csv_arg(params),
                "treatments": _parse_csv_arg(treatments),
                "artifact": artifact,
            }

    return await _require_site_json(
        site_id=site_id,
        path="/params/hist",
        params={
            "run_id": run_id,
            "models": models,
            "treatments": treatments,
            "params": params,
        },
        timeout=SITE_REQUEST_TIMEOUT,
    )