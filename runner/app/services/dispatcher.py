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


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def _safe_list(value: Any) -> list[Any]:
    """
    Convert a value to list safely.

    Returns an empty list for non-list inputs.
    """
    return value if isinstance(value, list) else []


def _safe_dict(value: Any) -> dict[str, Any]:
    """
    Convert a value to dict safely.

    Returns an empty dict for non-dict inputs.
    """
    return value if isinstance(value, dict) else {}


def _should_publish_to_forecast(task_type: str) -> bool:
    """
    Decide whether this run type should be published to Forecast page.

    Current business rule:
    - only auto_forecast results are published into forecast_registry

    Future extension:
    - this can be expanded to include additional task types if needed
    """
    return str(task_type or "").strip() == "auto_forecast"


def _build_site_run_payload(run: dict[str, Any]) -> dict[str, Any]:
    """
    Build the payload sent from Runner to Site /run.

    Notes:
    - payload_json is the original Runner-side submission payload
    - Dispatcher injects run-level metadata expected by Site
    - Site can use run_id as the stable root directory / manifest key
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

    Expected behavior:
    - Site executes the requested task
    - Site may return a lightweight execution summary
    - Site may optionally return the final manifest inline

    Return:
    - parsed JSON dict on success
    - {} if the response body is empty / not a dict
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
    Fetch run manifest from Site via GET /runs/{run_id}/manifest.

    This is the canonical way Runner retrieves the final artifact declaration
    after the Site finishes execution.
    """
    async with httpx.AsyncClient(timeout=SITE_REQUEST_TIMEOUT) as client:
        resp = await client.get(f"{base_url.rstrip('/')}/runs/{run_id}/manifest")
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, dict) else {}


async def _resolve_site_base_url(site_id: str) -> str:
    """
    Resolve one site's base_url from the site registry.
    """
    site = registry.get_site(site_id)
    if not site:
        raise ValueError(f"Site not found in registry: {site_id}")

    base_url = str(site.get("base_url") or "").rstrip("/")
    if not base_url:
        raise ValueError(f"Site base_url missing for site: {site_id}")

    return base_url


def _extract_inline_manifest(site_response: dict[str, Any] | None) -> dict[str, Any]:
    """
    Extract an inline manifest from Site /run response when present.

    Supported shapes:
    - {"manifest": {...}}
    - {"data": {"manifest": {...}}}

    If no inline manifest is present, return {}.
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


def _normalize_manifest_for_runner(manifest: dict[str, Any]) -> dict[str, Any]:
    """
    Normalize site manifest into Runner-expected shape.

    Compatibility rules:
    - prefer manifest["forecast_registry"] when already present
    - if missing, upgrade legacy manifest["publish_forecast"] into forecast_registry
      using outputs.index to fill data_path / source_ref
    """
    manifest = _safe_dict(manifest)

    if isinstance(manifest.get("forecast_registry"), list):
        return manifest

    legacy_items = manifest.get("publish_forecast")
    if not isinstance(legacy_items, list):
        return manifest

    outputs = _safe_dict(manifest.get("outputs"))
    index_obj = _safe_dict(outputs.get("index"))

    normalized_items: list[dict[str, Any]] = []

    for item in legacy_items:
        if not isinstance(item, dict):
            continue

        model_id = str(item.get("model_id") or "").strip()
        treatment = str(item.get("treatment") or "").strip()
        variable = str(item.get("variable") or "").strip()

        if not model_id or not treatment or not variable:
            continue

        model_block = _safe_dict(index_obj.get(model_id))
        treatment_block = _safe_dict(model_block.get(treatment))
        rel_path = str(treatment_block.get(variable) or "").strip()

        normalized_items.append(
            {
                "model_id": model_id,
                "treatment": treatment,
                "variable": variable,
                "data_path": rel_path,
                "source_ref": {
                    "rel_path": rel_path,
                    "media_type": "application/json",
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

    High-level flow:
    1) load run from Runner DB
    2) resolve Site base URL
    3) mark run as running
    4) POST /run to Site
    5) obtain final manifest
       - fetch canonical /runs/{run_id}/manifest when available
       - otherwise fall back to inline manifest from Site /run response
    6) normalize manifest for Runner compatibility
    7) register manifest artifacts into run_outputs
    8) publish manifest forecast rows when task_type is eligible
    9) mark run as done

    Returns:
    {
        "run": {...},
        "manifest": {...} | None,
        "published": [...],
        "site_response": {...} | None
    }
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

        # -------------------------------------------------------------
        # Step 3: obtain final manifest
        #
        # Canonical preference:
        # - always try GET /runs/{run_id}/manifest first
        # - if that is unavailable, fall back to inline manifest
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

        # -------------------------------------------------------------
        # Step 4: register run outputs
        #
        # Important design:
        # - Runner does not special-case parameter artifacts
        # - Site should declare *all* artifacts in manifest["artifacts"]
        # - That includes:
        #   * timeseries
        #   * plots
        #   * CSV/JSON tables
        #   * parameter_summary
        #   * parameter_posterior
        #   * parameter_hist
        # - Runner stores them uniformly in run_outputs
        # -------------------------------------------------------------
        await replace_run_outputs_from_manifest(run_id, manifest)

        # -------------------------------------------------------------
        # Step 5: publish to forecast_registry when eligible
        #
        # Current rule:
        # - only auto_forecast publishes Forecast-page visible rows
        #
        # Site is expected to declare those rows in:
        # manifest["forecast_registry"]
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

    Success flow:
    1) load run from Runner DB
    2) mark run as running
    3) POST /run to Site
    4) fetch / infer manifest
    5) register artifacts into run_outputs
    6) publish forecast rows if task type is eligible
    7) mark run as done

    Failure behavior:
    - mark run as failed with a readable error message
    """
    result = await _dispatch_once(run_id, collect_details=False)
    return result["run"]


async def dispatch_run_and_collect(run_id: str) -> dict[str, Any]:
    """
    Dispatch one run and return a richer debug payload.

    Output shape:
    {
        "run": {...},
        "manifest": {...} | None,
        "published": [...],
        "site_response": {...} | None
    }

    This helper is useful for admin/debug endpoints and integration tests.
    """
    return await _dispatch_once(run_id, collect_details=True)