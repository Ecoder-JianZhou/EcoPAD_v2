"""
Runner dispatcher service.

Responsibilities:
- send one run request from Runner to the target Site service
- transition run status in Runner DB
- retrieve the final run manifest from Site
- register all site-declared artifacts into Runner run_outputs
- publish eligible forecast rows into forecast_registry

Design notes:
- Dispatcher is the bridge between Runner and Site
- Runner does not implement model logic
- Site is responsible for actual execution and for producing manifest.json
- Runner trusts manifest["artifacts"] as the canonical artifact declaration
- Runner trusts manifest["forecast_registry"] as the canonical publish declaration
"""

from __future__ import annotations

import json
from typing import Any

import httpx

from app.core.settings import SITE_REQUEST_TIMEOUT
from app.services.forecast_registry import publish_from_manifest
from app.services.run_manager import (
    get_run,
    mark_run_done,
    mark_run_failed,
    mark_run_running,
    replace_run_outputs_from_manifest,
)
from app.services.site_registry import registry


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
def _safe_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _safe_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_output_type(value: Any, default: str = "forecast_with_da") -> str:
    text = _normalize_text(value)
    if not text:
        return default
    if text in SUPPORTED_OUTPUT_TYPES:
        return text
    mapped = LEGACY_OUTPUT_ALIASES.get(text)
    if mapped:
        return mapped
    return default


def _should_publish_to_forecast(task_type: str) -> bool:
    """
    Current business rule:
    - only auto_forecast results are published into forecast_registry
    """
    return _normalize_text(task_type) == "auto_forecast"


def _build_site_run_payload(run: dict[str, Any]) -> dict[str, Any]:
    """
    Build the payload sent from Runner to Site /run.
    """
    payload: dict[str, Any]
    try:
        payload = json.loads(run.get("payload_json") or "{}")
        if not isinstance(payload, dict):
            payload = {}
    except Exception:
        payload = {}

    return {
        "run_id": run["id"],
        "site_id": run["site_id"],
        "model_id": run["model_id"],
        "task_type": run["task_type"],
        "trigger_type": run.get("trigger_type", "manual"),
        "payload": payload,
    }


async def _post_run_to_site(
    *,
    base_url: str,
    site_payload: dict[str, Any],
) -> dict[str, Any]:
    """
    Trigger site execution via POST /run.
    """
    async with httpx.AsyncClient(timeout=SITE_REQUEST_TIMEOUT) as client:
        resp = await client.post(f"{base_url.rstrip('/')}/run", json=site_payload)
        resp.raise_for_status()

        try:
            data = resp.json()
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}


async def _fetch_manifest_from_site(
    *,
    base_url: str,
    run_id: str,
) -> dict[str, Any]:
    """
    Fetch canonical run manifest from Site.
    """
    async with httpx.AsyncClient(timeout=SITE_REQUEST_TIMEOUT) as client:
        resp = await client.get(f"{base_url.rstrip('/')}/runs/{run_id}/manifest")
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, dict) else {}


async def _resolve_site_base_url(site_id: str) -> str:
    site = registry.get_site(site_id)
    if not site:
        raise ValueError(f"Site not found in registry: {site_id}")

    base_url = str(site.get("base_url") or "").rstrip("/")
    if not base_url:
        raise ValueError(f"Site base_url missing for site: {site_id}")

    return base_url


def _extract_inline_manifest(site_response: dict[str, Any] | None) -> dict[str, Any]:
    """
    Extract inline manifest from Site /run response when present.

    Supported:
    - {"manifest": {...}}
    - {"data": {"manifest": {...}}}
    """
    site_response = _safe_dict(site_response)

    manifest = _safe_dict(site_response.get("manifest"))
    if manifest:
        return manifest

    data = _safe_dict(site_response.get("data"))
    manifest = _safe_dict(data.get("manifest"))
    if manifest:
        return manifest

    return {}


def _site_response_indicates_failure(site_response: dict[str, Any] | None) -> str:
    """
    Inspect site /run response for explicit failure indication.
    """
    site_response = _safe_dict(site_response)

    status = _normalize_text(site_response.get("status") or site_response.get("execution_status")).lower()
    if status in {"failed", "error", "cancelled"}:
        return _normalize_text(site_response.get("detail") or site_response.get("message") or "Site returned failure status")

    ok_val = site_response.get("ok")
    if ok_val is False:
        return _normalize_text(site_response.get("detail") or site_response.get("message") or "Site returned ok=false")

    return ""


def _manifest_indicates_failure(manifest: dict[str, Any] | None) -> str:
    """
    Inspect canonical manifest for explicit execution failure.
    """
    manifest = _safe_dict(manifest)
    execution = _safe_dict(manifest.get("execution"))

    status = _normalize_text(execution.get("status")).lower()
    if status and status not in {"done", "completed", "succeeded", "success"}:
        return _normalize_text(execution.get("error") or execution.get("message") or f"Manifest execution status is {status}")

    return ""


def _normalize_manifest_for_runner(manifest: dict[str, Any]) -> dict[str, Any]:
    """
    Normalize site manifest into Runner-expected shape.

    Compatibility rules:
    - prefer manifest["forecast_registry"] when already present
    - if missing, upgrade legacy manifest["publish_forecast"] into forecast_registry
    - normalize output_type / series_type
    - normalize old forecast_mode -> output_type/series_type
    """
    manifest = _safe_dict(manifest)

    # ---------------------------------------------------------
    # Case 1: canonical forecast_registry already exists
    # ---------------------------------------------------------
    raw_registry = manifest.get("forecast_registry")
    if isinstance(raw_registry, list):
        normalized_items: list[dict[str, Any]] = []

        for item in raw_registry:
            if not isinstance(item, dict):
                continue

            obj = dict(item)

            normalized_output_type = _normalize_output_type(
                obj.get("output_type") or obj.get("series_type") or obj.get("forecast_mode"),
                default="forecast_with_da",
            )
            obj["output_type"] = normalized_output_type
            obj["series_type"] = normalized_output_type

            source_ref = _safe_dict(obj.get("source_ref"))
            if source_ref:
                source_ref["output_type"] = normalized_output_type
                source_ref["series_type"] = normalized_output_type
                obj["source_ref"] = source_ref

            normalized_items.append(obj)

        manifest["forecast_registry"] = normalized_items
        return manifest

    # ---------------------------------------------------------
    # Case 2: upgrade legacy publish_forecast -> forecast_registry
    # ---------------------------------------------------------
    legacy_items = manifest.get("publish_forecast")
    if not isinstance(legacy_items, list):
        return manifest

    outputs = _safe_dict(manifest.get("outputs"))
    index_obj = _safe_dict(outputs.get("index"))

    normalized_items: list[dict[str, Any]] = []

    for item in legacy_items:
        if not isinstance(item, dict):
            continue

        model_id = _normalize_text(item.get("model_id"))
        treatment = _normalize_text(item.get("treatment"))
        variable = _normalize_text(item.get("variable"))

        if not model_id or not treatment or not variable:
            continue

        output_type = _normalize_output_type(
            item.get("output_type") or item.get("series_type") or item.get("forecast_mode"),
            default="forecast_with_da",
        )

        model_block = _safe_dict(index_obj.get(model_id))
        treatment_block = _safe_dict(model_block.get(treatment))
        series_block = _safe_dict(treatment_block.get(output_type))
        rel_path = _normalize_text(series_block.get(variable))

        normalized_items.append(
            {
                "model_id": model_id,
                "treatment": treatment,
                "variable": variable,
                "output_type": output_type,
                "series_type": output_type,
                "data_path": rel_path,
                "obs_path": _normalize_text(item.get("obs_path")),
                "source_ref": {
                    "rel_path": rel_path,
                    "media_type": "application/json",
                    "output_type": output_type,
                    "series_type": output_type,
                },
                "is_published": 1,
            }
        )

    manifest["forecast_registry"] = normalized_items
    return manifest


# ---------------------------------------------------------------------
# Internal dispatcher
# ---------------------------------------------------------------------
async def _dispatch_once(
    run_id: str,
    *,
    collect_details: bool = False,
) -> dict[str, Any]:
    """
    Internal dispatch implementation shared by public APIs.
    """
    run = await get_run(run_id)
    if run is None:
        raise ValueError(f"Run not found: {run_id}")

    manifest: dict[str, Any] | None = None
    published_rows: list[dict[str, Any]] = []
    site_response: dict[str, Any] | None = None

    try:
        base_url = await _resolve_site_base_url(run["site_id"])

        # -------------------------------------------------------------
        # Step 1: mark run as running
        # -------------------------------------------------------------
        await mark_run_running(run_id)

        run = await get_run(run_id)
        if run is None:
            raise RuntimeError(f"Run disappeared after mark_run_running: {run_id}")

        site_payload = _build_site_run_payload(run)

        # -------------------------------------------------------------
        # Step 2: trigger site execution
        # -------------------------------------------------------------
        site_response = await _post_run_to_site(
            base_url=base_url,
            site_payload=site_payload,
        )

        fail_msg = _site_response_indicates_failure(site_response)
        if fail_msg:
            raise RuntimeError(fail_msg)

        # -------------------------------------------------------------
        # Step 3: obtain final manifest
        # -------------------------------------------------------------
        inline_manifest = _extract_inline_manifest(site_response)

        fetched_manifest: dict[str, Any] = {}
        try:
            fetched_manifest = await _fetch_manifest_from_site(
                base_url=base_url,
                run_id=run_id,
            )
        except Exception:
            fetched_manifest = {}

        if fetched_manifest:
            manifest = _safe_dict(fetched_manifest)
        else:
            manifest = _safe_dict(inline_manifest)

        manifest = _normalize_manifest_for_runner(manifest)

        fail_msg = _manifest_indicates_failure(manifest)
        if fail_msg:
            raise RuntimeError(fail_msg)

        # -------------------------------------------------------------
        # Step 4: register run outputs
        # -------------------------------------------------------------
        await replace_run_outputs_from_manifest(run_id, manifest)

        # -------------------------------------------------------------
        # Step 5: publish to forecast_registry when eligible
        # -------------------------------------------------------------
        if _should_publish_to_forecast(run.get("task_type", "")):
            published_rows = await publish_from_manifest(
                site_id=run["site_id"],
                source_run_id=run_id,
                manifest=manifest,
            )

        # -------------------------------------------------------------
        # Step 6: mark done
        # -------------------------------------------------------------
        final_run = await mark_run_done(run_id)

        return {
            "run": final_run,
            "manifest": manifest if collect_details else None,
            "published": published_rows if collect_details else [],
            "site_response": site_response if collect_details else None,
        }

    except httpx.HTTPStatusError as ex:
        detail = f"Site HTTP error: {ex.response.status_code} {ex.response.text}"
        final_run = await mark_run_failed(run_id, detail)
        return {
            "run": final_run,
            "manifest": manifest if collect_details else None,
            "published": published_rows if collect_details else [],
            "site_response": site_response if collect_details else None,
        }

    except httpx.RequestError as ex:
        detail = f"Site request failed: {str(ex)}"
        final_run = await mark_run_failed(run_id, detail)
        return {
            "run": final_run,
            "manifest": manifest if collect_details else None,
            "published": published_rows if collect_details else [],
            "site_response": site_response if collect_details else None,
        }

    except Exception as ex:
        detail = f"Dispatch failed: {str(ex)}"
        final_run = await mark_run_failed(run_id, detail)
        return {
            "run": final_run,
            "manifest": manifest if collect_details else None,
            "published": published_rows if collect_details else [],
            "site_response": site_response if collect_details else None,
        }


# ---------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------
async def dispatch_run(run_id: str) -> dict[str, Any]:
    """
    Dispatch one run to the target Site and return the final Runner run row.
    """
    result = await _dispatch_once(run_id, collect_details=False)
    return result["run"]


async def dispatch_run_and_collect(run_id: str) -> dict[str, Any]:
    """
    Dispatch one run and return a richer debug payload.
    """
    return await _dispatch_once(run_id, collect_details=True)